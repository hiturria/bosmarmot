package service

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"strings"

	"github.com/hyperledger/burrow/event"
	"github.com/hyperledger/burrow/event/query"
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

// Consumer contains basic configuration for consumer to run
type Consumer struct {
	Config           *config.Flags
	Log              *logger.Logger
	EventLogDecoders map[string]EventLogDecoder
	Closing          bool
}

// NewConsumer constructs a new consumer configuration
func NewConsumer(cfg *config.Flags, log *logger.Logger) *Consumer {
	return &Consumer{
		Config:           cfg,
		Log:              log,
		EventLogDecoders: make(map[string]EventLogDecoder),
		Closing:          false,
	}
}

// AddEventLogDecoder adds an event log decoder for a given event name
func (c *Consumer) AddEventLogDecoder(eventName string, eventLogDecoder EventLogDecoder) {
	c.EventLogDecoders[eventName] = eventLogDecoder
}

// Run connects to a grpc service and subscribes to log events,
// then gets tables structures, maps them & parse event data.
// Store data in SQL event tables, it runs forever
func (c *Consumer) Run() error {
	c.Log.Info("msg", "Reading events config file")

	byteValue, err := readFile(c.Config.CfgFile)
	if err != nil {
		return errors.Wrap(err, "Error reading events config file")
	}

	c.Log.Info("msg", "Parsing and mapping events config stream")

	parser, err := sqlsol.NewParser(byteValue)
	if err != nil {
		return errors.Wrap(err, "Error mapping events config stream")
	}

	tables := parser.GetTables()

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

	c.Log.Info("msg", "Getting last processed block number from SQL log table")

	fromBlock, err := db.GetLastBlockID()
	if err != nil {
		return errors.Wrap(err, "Error trying to get last processed block number from SQL log table")
	}

	// right now there is no way to know if the last block of events was completely read
	// so we have to begin processing from the last block number stored in database
	// and update event data if already present

	// string to uint64 from event filtering
	startingBlock, err := strconv.ParseUint(fromBlock, 10, 64)
	if err != nil {
		return errors.Wrap(err, "Error trying to convert fromBlock from string to uint64")
	}

	c.Log.Info("msg", "Connecting to Burrow gRPC server")

	conn, err := grpc.Dial(c.Config.GRPCAddr, grpc.WithInsecure())
	if err != nil {
		return errors.Wrapf(err, "Error connecting to Burrow gRPC server at %s", c.Config.GRPCAddr)
	}
	defer conn.Close()

	cli := rpcevents.NewExecutionEventsClient(conn)

	request := &rpcevents.BlocksRequest{
		Query:      query.NewBuilder().AndEquals(event.EventTypeKey, exec.TypeLog.String()).String(),
		BlockRange: rpcevents.NewBlockRange(rpcevents.AbsoluteBound(startingBlock), rpcevents.LatestBound()),
	}
	evs, err := cli.GetEvents(context.Background(), request)
	if err != nil {
		return errors.Wrap(err, "Error connecting to events stream")
	}

	// a fresh new structure to store block data
	blockData := sqlsol.NewBlockData()

	// Grab the events
	for {
		if c.Closing {
			break
		}

		c.Log.Info("msg", "Waiting for events")

		resp, err := evs.Recv()
		if err != nil {
			if err == io.EOF {
				c.Log.Info("msg", "EOF received")
				break
			} else {
				return errors.Wrap(err, "Error receiving events")
			}
		}

		c.Log.Info("msg", fmt.Sprintf("Events received: %v", len(resp.Events)))

		// get event data
		for _, event := range resp.Events {
			// a fresh new row to store column/value data
			row := make(types.EventDataRow)

			// GetHeader gets Header data for the given event
			// GetLog gets log event data for the given event
			eventHeader := event.GetHeader()
			eventLog := event.GetLog()

			// decode event data using the provided event log decoders
			eventData := DecodeEvent(eventHeader, eventLog, c.EventLogDecoders)

			// ------------------------------------------------
			// if source block number is different than current...
			// upsert rows in specific SQL event tables and update block number
			eventBlockID := fmt.Sprintf("%v", eventHeader.GetHeight())

			if strings.TrimSpace(fromBlock) != strings.TrimSpace(eventBlockID) {
				// store block data in SQL tables (if any)
				if blockData.PendingRows(fromBlock) {

					// gets block data to upsert
					blk := blockData.GetBlockData()

					c.Log.Info("msg", fmt.Sprintf("Upserting rows in SQL event tables %v", blk))

					// upsert rows in specific SQL event tables and update block number
					err = db.SetBlock(tables, blk)
					if err != nil {
						return errors.Wrap(err, "Error upserting rows in SQL event tables")
					}
				}

				// end of block setter, clear blockData structure
				blockData = sqlsol.NewBlockData()

				// set new block number
				fromBlock = eventBlockID
			}

			// get eventName to map to SQL tableName
			eventName := eventData["eventName"]
			tableName, err := parser.GetTableName(eventName)
			if err != nil {
				return err
			}

			// for each data element, maps to SQL columnName and gets its value
			for k, v := range eventData {
				columnName, err := parser.GetColumnName(eventName, k)
				if err != nil {
					return err
				}

				row[columnName] = v
			}

			// so, the row is filled with data, update structure
			// store block number
			blockData.SetBlockID(fromBlock)

			// set row in structure
			blockData.AddRow(tableName, row)
		}
	}

	// store pending block data in SQL tables (if any)
	if blockData.PendingRows(fromBlock) {
		// gets block data to upsert
		blk := blockData.GetBlockData()

		c.Log.Info("msg", fmt.Sprintf("Upserting rows in SQL event tables %v", blk))

		// upsert rows in specific SQL event tables and update block number
		err = db.SetBlock(tables, blk)
		if err != nil {
			return errors.Wrap(err, "Error upserting rows in SQL event tables")
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
