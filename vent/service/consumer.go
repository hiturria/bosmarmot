package service

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/hyperledger/burrow/execution/evm/abi"
	"github.com/hyperledger/burrow/execution/exec"
	"github.com/hyperledger/burrow/rpc/rpcevents"
	"github.com/monax/bosmarmot/vent/config"
	"github.com/monax/bosmarmot/vent/logger"
	"github.com/monax/bosmarmot/vent/sqldb"
	"github.com/monax/bosmarmot/vent/sqlsol"
	"github.com/monax/bosmarmot/vent/types"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

const (
	eventNameLabel   = "eventName"
	eventHeightLabel = "height"
	eventTxHashLabel = "txHash"
	eventIndexLabel  = "index"
	eventTypeLabel   = "eventType"
)

// Consumer contains basic configuration for consumer to run
type Consumer struct {
	Config  *config.Flags
	Log     *logger.Logger
	Closing bool
}

// NewConsumer constructs a new consumer configuration
func NewConsumer(cfg *config.Flags, log *logger.Logger) *Consumer {
	return &Consumer{
		Config:  cfg,
		Log:     log,
		Closing: false,
	}
}

// Run connects to a grpc service and subscribes to log events,
// then gets tables structures, maps them & parse event data.
// Store data in SQL event tables, it runs forever
func (c *Consumer) Run() error {
	c.Log.Info("msg", "Reading events config file")

	byteValue, err := readFile(c.Config.SpecFile)
	if err != nil {
		return errors.Wrap(err, "Error reading events config file")
	}

	c.Log.Info("msg", "Parsing and mapping events config stream")

	parser, err := sqlsol.NewParser(byteValue)
	if err != nil {
		return errors.Wrap(err, "Error mapping events config stream")
	}

	// obtain tables structures, event & abi specifications
	tables := parser.GetTables()
	eventSpec := parser.GetEventSpec()
	abiSpec := parser.GetAbiSpec()

	if len(eventSpec) == 0 {
		c.Log.Info("msg", "No events specifications found")
		return nil
	}

	c.Log.Info("msg", "Connecting to SQL database")

	db, err := sqldb.NewSQLDB(c.Config.DBAdapter, c.Config.DBURL, c.Config.DBSchema, c.Log)
	if err != nil {
		return errors.Wrap(err, "Error connecting to SQL")
	}
	defer db.Close()

	c.Log.Info("msg", "Synchronizing config and database parser structures")

	err = db.SynchronizeDB(tables)
	if err != nil {
		return errors.Wrap(err, "Error trying to synchronize database")
	}

	c.Log.Info("msg", "Connecting to Burrow gRPC server")

	conn, err := grpc.Dial(c.Config.GRPCAddr, grpc.WithInsecure())
	if err != nil {
		return errors.Wrapf(err, "Error connecting to Burrow gRPC server at %s", c.Config.GRPCAddr)
	}
	defer conn.Close()

	// start a goroutine to listen to events for each event definition in the spec
	// doneCh is used for sending a "done" signal from each goroutine to the main thread
	// eventCh is used for sending received events to the main thread to be stored in the db
	doneCh := make(chan error)
	eventCh := make(chan types.EventData)

	var wg sync.WaitGroup

	for i := range eventSpec {
		spec := eventSpec[i]
		wg.Add(1)

		go func() {
			defer wg.Done()

			c.Log.Info("msg", "Getting last processed block number from SQL log table", "filter", spec.Filter)

			// right now there is no way to know if the last block of events was completely read
			// so we have to begin processing from the last block number stored in database
			// for the given event filter and update event data if already present
			fromBlock, err := db.GetLastBlockID(spec.Filter)
			if err != nil {
				doneCh <- errors.Wrapf(err, "Error trying to get last processed block number from SQL log table (filter: %s)", spec.Filter)
				return
			}

			// string to uint64 from event filtering
			startingBlock, err := strconv.ParseUint(fromBlock, 10, 64)
			if err != nil {
				doneCh <- errors.Wrapf(err, "Error trying to convert fromBlock from string to uint64 (filter: %s)", spec.Filter)
				return
			}

			// setup the execution events client for this spec
			cli := rpcevents.NewExecutionEventsClient(conn)

			request := &rpcevents.BlocksRequest{
				Query:      spec.Filter,
				BlockRange: rpcevents.NewBlockRange(rpcevents.AbsoluteBound(startingBlock), rpcevents.LatestBound()),
			}

			// gets events with given filter & block range based on last processed block taken from database
			evs, err := cli.GetEvents(context.Background(), request)
			if err != nil {
				doneCh <- errors.Wrapf(err, "Error connecting to events stream (filter: %s)", spec.Filter)
				return
			}

			// create a fresh new structure to store block data
			blockData := sqlsol.NewBlockData()

			// getting events
			for {
				if c.Closing {
					break
				} else {
					time.Sleep(100 * time.Millisecond)
				}

				c.Log.Info("msg", "Waiting for events", "filter", spec.Filter)

				resp, err := evs.Recv()
				if err != nil {
					if err == io.EOF {
						c.Log.Info("msg", "EOF received", "filter", spec.Filter)
						break
					} else {
						doneCh <- errors.Wrapf(err, "Error receiving events (filter: %s)", spec.Filter)
						return
					}
				}

				c.Log.Info("msg", "Events received", "length", len(resp.Events), "filter", spec.Filter)

				// get event data
				for _, event := range resp.Events {

					// a fresh new row to store column/value data
					row := make(types.EventDataRow)

					// get header & log data for the given event
					eventHeader := event.GetHeader()
					eventLog := event.GetLog()

					c.Log.Info("msg", fmt.Sprintf("Event Header: %v", eventHeader), "filter", spec.Filter)

					// decode event data using the provided abi specification
					eventData, err := decodeEvent(spec.Event.Name, eventHeader, eventLog, abiSpec, c.Log)
					if err != nil {
						doneCh <- errors.Wrapf(err, "Error decoding event (filter: %s)", spec.Filter)
						return
					}

					// if source block number is different than current
					// upsert rows in specific SQL event tables and update block number
					eventBlockID := fmt.Sprintf("%v", eventHeader.GetHeight())

					if strings.TrimSpace(fromBlock) != strings.TrimSpace(eventBlockID) {

						// store block data in SQL tables (if any)
						if blockData.PendingRows(fromBlock) {

							// gets block data to upsert
							blk := blockData.GetBlockData()

							c.Log.Info("msg", fmt.Sprintf("Upserting rows in SQL event tables %v", blk), "filter", spec.Filter)

							eventCh <- blk
						}

						// end of block setter, clear blockData structure
						blockData = sqlsol.NewBlockData()

						// set new block number
						fromBlock = eventBlockID
					}

					// get eventName to map to SQL tableName
					eventName := eventData[eventNameLabel]
					tableName, err := parser.GetTableName(eventName)
					if err != nil {
						doneCh <- errors.Wrapf(err, "Error getting table name for event (filter: %s)", spec.Filter)
						return
					}

					// for each data element, maps to SQL columnName and gets its value
					// if there is no matching column for the item, it doesn't need to be store in db
					for k, v := range eventData {
						if columnName, err := parser.GetColumnName(eventName, k); err == nil {
							row[columnName] = v
						}
					}

					// so, the row is filled with data, update block number in structure
					blockData.SetBlockID(fromBlock)

					// set row in structure
					blockData.AddRow(tableName, row)
				}
			}

			// store pending block data in SQL tables (if any)
			if blockData.PendingRows(fromBlock) {

				// gets block data to upsert
				blk := blockData.GetBlockData()

				c.Log.Info("msg", fmt.Sprintf("Upserting rows in SQL event tables %v", blk), "filter", spec.Filter)

				eventCh <- blk
			}
		}()
	}

	go func() {
		// wait for all threads to end
		wg.Wait()
		doneCh <- nil
	}()

loop:
	for {
		select {
		case err := <-doneCh:
			if err != nil {
				return err
			}
			break loop
		case blk := <-eventCh:
			// upsert rows in specific SQL event tables and update block number
			if err := db.SetBlock(tables, blk); err != nil {
				return errors.Wrap(err, "Error upserting rows in SQL event tables")
			}
		}
	}

	c.Log.Info("msg", "Done!")
	return nil
}

// Shutdown gracefully shuts down the events consumer
func (c *Consumer) Shutdown() {
	c.Log.Info("msg", "Shutting down...")
	c.Closing = true
}

// decodeEvent unpacks & decodes event data
func decodeEvent(eventName string, header *exec.Header, log *exec.LogEvent, abiSpec *abi.AbiSpec, l *logger.Logger) (map[string]string, error) {

	// to prepare decoded data and map to event item name
	data := make(map[string]string)

	data[eventNameLabel] = eventName

	eventAbiSpec, ok := abiSpec.Events[eventName]
	if !ok {
		return nil, fmt.Errorf("Abi spec not found for event %s", eventName)
	}

	// decode header to get context data for each event
	data[eventIndexLabel] = fmt.Sprintf("%v", header.GetIndex())
	data[eventHeightLabel] = fmt.Sprintf("%v", header.GetHeight())
	data[eventTypeLabel] = header.GetEventType().String()
	data[eventTxHashLabel] = fmt.Sprintf("%v", header.TxHash)

	// build expected interface type array to get log event values & make it string
	unpackedData := make([]interface{}, len(eventAbiSpec.Inputs))
	for i, _ := range unpackedData {
		unpackedData[i] = new(string)
	}

	// unpack event data (topics & data part)
	if err := abi.UnpackEvent(eventAbiSpec, log.Topics, log.Data, unpackedData...); err != nil {
		return nil, errors.Wrap(err, "Could not unpack event data")
	}

	l.Debug("msg", fmt.Sprintf("Unpacked event data %v", unpackedData), "eventName", eventName)

	// for each decoded item value, stores it in given item name
	for i, input := range eventAbiSpec.Inputs {
		data[input.Name] = *unpackedData[i].(*string)

		dataItem := data[input.Name]

		// in the rare case that a not valid UTF8 string is found, convert characters to hexa
		if !utf8.ValidString(dataItem) {

			l.Warn("msg", fmt.Sprintf("Illegal UTF8 string: i = %v, data[input.Name] = %v, input.Name = %v", i, data[input.Name], input.Name), "eventName", eventName)

			s := make([]string, 0, len(dataItem))

			for i := 0; i < len(dataItem); i++ {
				s = append(s, fmt.Sprintf("%x", dataItem[i]))
			}

			data[input.Name] = strings.ToUpper(strings.Join(s, ""))
		}

		l.Debug("msg", fmt.Sprintf("Unpacked data items: unpackedData[%v] = %v, data[input.Name] = %v, input.Name = %v", i, unpackedData[i], data[input.Name], input.Name), "eventName", eventName)

	}

	return data, nil
}

// readFile opens a given file and reads it contents into a stream of bytes
func readFile(file string) ([]byte, error) {
	theFile, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer theFile.Close()

	byteValue, err := ioutil.ReadAll(theFile)
	if err != nil {
		return nil, err
	}

	return byteValue, nil
}
