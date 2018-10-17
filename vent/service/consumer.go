package service

import (
	"context"
	"fmt"
	"io"
	"math/big"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hyperledger/burrow/crypto"
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
	"google.golang.org/grpc/connectivity"
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
	Config         *config.Flags
	Log            *logger.Logger
	Closing        bool
	DB             *sqldb.SQLDB
	GRPCConnection *grpc.ClientConn
	// external events channel used for when vent is leveraged as a library
	EventsChannel chan types.EventData
}

// NewConsumer constructs a new consumer configuration
func NewConsumer(cfg *config.Flags, log *logger.Logger, eChannel chan types.EventData) *Consumer {
	return &Consumer{
		Config:        cfg,
		Log:           log,
		Closing:       false,
		EventsChannel: eChannel,
	}
}

// Run connects to a grpc service and subscribes to log events,
// then gets tables structures, maps them & parse event data.
// Store data in SQL event tables, it runs forever
func (c *Consumer) Run(parser *sqlsol.Parser, abiSpec *abi.AbiSpec, stream bool) error {

	var err error

	// obtain tables structures, event & abi specifications
	tables := parser.GetTables()
	eventSpec := parser.GetEventSpec()

	if len(eventSpec) == 0 {
		c.Log.Info("msg", "No events specifications found")
		return nil
	}

	c.Log.Info("msg", "Connecting to SQL database")

	c.DB, err = sqldb.NewSQLDB(c.Config.DBAdapter, c.Config.DBURL, c.Config.DBSchema, c.Log)
	if err != nil {
		return errors.Wrap(err, "Error connecting to SQL")
	}
	defer c.DB.Close()

	c.Log.Info("msg", "Synchronizing config and database parser structures")

	err = c.DB.SynchronizeDB(tables)
	if err != nil {
		return errors.Wrap(err, "Error trying to synchronize database")
	}

	c.Log.Info("msg", "Connecting to Burrow gRPC server")

	c.GRPCConnection, err = grpc.Dial(c.Config.GRPCAddr, grpc.WithInsecure())
	if err != nil {
		return errors.Wrapf(err, "Error connecting to Burrow gRPC server at %s", c.Config.GRPCAddr)
	}
	defer c.GRPCConnection.Close()

	// doneCh is used for sending a "done" signal from each goroutine to the main thread
	// eventCh is used for sending received events to the main thread to be stored in the db
	doneCh := make(chan error)
	eventCh := make(chan types.EventData)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()

		c.Log.Info("msg", "Getting last processed block number from SQL log table")

		// right now there is no way to know if the last block of events was completely read
		// so we have to begin processing from the last block number stored in database
		// and update event data if already present
		fromBlock, err := c.DB.GetLastBlockID()
		if err != nil {
			doneCh <- errors.Wrapf(err, "Error trying to get last processed block number from SQL log table")
			return
		}

		// string to uint64 from event filtering
		startingBlock, err := strconv.ParseUint(fromBlock, 10, 64)
		if err != nil {
			doneCh <- errors.Wrapf(err, "Error trying to convert fromBlock from string to uint64")
			return
		}

		// setup block range to get needed blocks server side
		cli := rpcevents.NewExecutionEventsClient(c.GRPCConnection)
		var end *rpcevents.Bound
		if stream {
			end = rpcevents.StreamBound()
		} else {
			end = rpcevents.LatestBound()
		}

		request := &rpcevents.BlocksRequest{
			BlockRange: rpcevents.NewBlockRange(rpcevents.AbsoluteBound(startingBlock), end),
		}

		// gets blocks in given range based on last processed block taken from database
		blocks, err := cli.GetBlocks(context.Background(), request)
		if err != nil {
			doneCh <- errors.Wrapf(err, "Error connecting to block stream")
			return
		}

		// create a fresh new structure to store block data
		blockData := sqlsol.NewBlockData()
		// getting blocks
		for {
			if c.Closing {
				break
			} else {
				time.Sleep(100 * time.Millisecond)
			}

			c.Log.Info("msg", "Waiting for blocks...")

			resp, err := blocks.Recv()
			if err != nil {
				if err == io.EOF {
					c.Log.Info("msg", "EOF stream received...")
					continue
				} else {
					if c.Closing {
						c.Log.Info("msg", "GRPC connection closed")
						break
					} else {
						doneCh <- errors.Wrapf(err, "Error receiving blocks")
						return
					}
				}
			}

			c.Log.Info("msg", "Block received", "num_txs", len(resp.TxExecutions))

			// get event data
			for _, txe := range resp.TxExecutions {
				for _, event := range txe.Events {
					taggedEvent := event.Tagged()

					// get header & log data for the given event
					eventHeader := event.GetHeader()
					eventLog := event.GetLog()

					// see which spec filter matches with the one in event data
					for _, spec := range eventSpec {
						qry, err := spec.Query()

						if err != nil {
							doneCh <- errors.Wrapf(err, "Error parsing query from filter string")
							return
						}

						// there's a matching filter, add data to the rows
						if qry.Matches(taggedEvent) {

							// a fresh new row to store column/value data
							row := make(types.EventDataRow)

							c.Log.Info("msg", fmt.Sprintf("Event Header: %v", eventHeader), "filter", spec.Filter)

							// decode event data using the provided abi specification
							eventData, err := decodeEvent(eventHeader, eventLog, abiSpec, c.Log)
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

							// for each data element, maps to SQL columnName and gets its value
							// if there is no matching column for the item, it doesn't need to be store in db
							for k, v := range eventData {
								if column, err := parser.GetColumn(spec.TableName, k); err == nil {
									if column.BytesToString {
										if bytes, ok := v.(*[]byte); ok {
											str := strings.Trim(string(*bytes), "\x00")
											row[column.Name] = interface{}(&str)
											continue
										}
									}
									row[column.Name] = v
								}
							}

							// so, the row is filled with data, update block number in structure
							blockData.SetBlockID(fromBlock)

							// set row in structure
							blockData.AddRow(strings.ToLower(spec.TableName), row)
						}

						// store pending block data in SQL tables (if any)
						if blockData.PendingRows(fromBlock) {

							// gets block data to upsert
							blk := blockData.GetBlockData()

							c.Log.Info("msg", fmt.Sprintf("Upserting rows in SQL event tables %v", blk), "filter", spec.Filter)

							eventCh <- blk
						}
					}
				}
			}
		}
	}()

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
			if err := c.DB.SetBlock(tables, blk); err != nil {
				return errors.Wrap(err, "Error upserting rows in SQL event tables")
			}

			// send to the external events channel in a non-blocking manner
			select {
			case c.EventsChannel <- blk:
			default:
			}
		}
	}

	c.Log.Info("msg", "Done!")
	return nil
}

// Health returns the health status for the consumer
func (c *Consumer) Health() error {
	if c.Closing {
		return errors.New("closing service")
	}

	// check db status
	if c.DB == nil {
		return errors.New("database disconnected")
	}

	if err := c.DB.Ping(); err != nil {
		return errors.New("database unavailable")
	}

	// check grpc connection status
	if c.GRPCConnection == nil {
		return errors.New("grpc disconnected")
	}

	if grpcState := c.GRPCConnection.GetState(); grpcState != connectivity.Ready {
		return errors.New("grpc connection not ready")
	}

	return nil
}

// Shutdown gracefully shuts down the events consumer
func (c *Consumer) Shutdown() {
	c.Log.Info("msg", "Shutting down vent consumer...")
	c.Closing = true
	c.GRPCConnection.Close()
}

// decodeEvent unpacks & decodes event data
func decodeEvent(header *exec.Header, log *exec.LogEvent, abiSpec *abi.AbiSpec, l *logger.Logger) (map[string]interface{}, error) {
	// to prepare decoded data and map to event item name
	data := make(map[string]interface{})

	var eventID abi.EventID
	var evAbi abi.EventSpec
	copy(eventID[:], log.Topics[0].Bytes())

	evAbi, ok := abiSpec.EventsById[eventID]
	if !ok {
		return nil, fmt.Errorf("Abi spec not found for event %x", eventID)
	}

	// decode header to get context data for each event
	data[eventNameLabel] = evAbi.Name
	data[eventIndexLabel] = fmt.Sprintf("%v", header.GetIndex())
	data[eventHeightLabel] = fmt.Sprintf("%v", header.GetHeight())
	data[eventTypeLabel] = header.GetEventType().String()
	data[eventTxHashLabel] = header.TxHash.String()

	// build expected interface type array to get log event values
	unpackedData := abi.GetPackingTypes(evAbi.Inputs)

	// unpack event data (topics & data part)
	if err := abi.UnpackEvent(evAbi, log.Topics, log.Data, unpackedData...); err != nil {
		return nil, errors.Wrap(err, "Could not unpack event data")
	}

	l.Debug("msg", fmt.Sprintf("Unpacked event data %v", unpackedData), "eventName", evAbi.Name)

	// for each decoded item value, stores it in given item name
	for i, input := range evAbi.Inputs {
		switch v := unpackedData[i].(type) {
		case *crypto.Address:
			data[input.Name] = v.String()
		case *big.Int:
			data[input.Name] = v.String()
		default:
			data[input.Name] = v
		}

		l.Debug("msg", fmt.Sprintf("Unpacked data items: data[%v] = %v", input.Name, data[input.Name]), "eventName", evAbi.Name)
	}

	return data, nil
}
