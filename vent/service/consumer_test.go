// +build integration

package service_test

import (
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/monax/bosmarmot/vent/config"
	"github.com/monax/bosmarmot/vent/logger"
	"github.com/monax/bosmarmot/vent/service"
	"github.com/monax/bosmarmot/vent/sqlsol"
	"github.com/monax/bosmarmot/vent/test"
	"github.com/monax/bosmarmot/vent/types"
	"github.com/stretchr/testify/require"
)

func TestConsumer(t *testing.T) {
	tCli := test.NewTransactClient(t, testConfig.RPC.GRPC.ListenAddress)
	create := test.CreateContract(t, tCli, inputAccount.Address())

	// generate events
	name := "TestEvent1"
	description := "Description of TestEvent1"
	test.CallAddEvent(t, tCli, inputAccount.Address(), create.Receipt.ContractAddress, name, description)

	name = "TestEvent2"
	description = "Description of TestEvent2"
	test.CallAddEvent(t, tCli, inputAccount.Address(), create.Receipt.ContractAddress, name, description)

	name = "TestEvent3"
	description = "Description of TestEvent3"
	test.CallAddEvent(t, tCli, inputAccount.Address(), create.Receipt.ContractAddress, name, description)

	name = "TestEvent4"
	description = "Description of TestEvent4"
	test.CallAddEvent(t, tCli, inputAccount.Address(), create.Receipt.ContractAddress, name, description)

	// workaround for off-by-one on latest bound fixed in burrow
	time.Sleep(time.Second * 2)

	// create test db
	db, closeDB := test.NewTestDB(t, types.PostgresDB)
	defer closeDB()

	// run consumer to listen to events
	cfg := config.DefaultFlags()

	cfg.DBSchema = db.Schema
	cfg.SpecFile = os.Getenv("GOPATH") + "/src/github.com/monax/bosmarmot/vent/test/sqlsol_example.json"
	cfg.AbiFile = os.Getenv("GOPATH") + "/src/github.com/monax/bosmarmot/vent/test/EventsTest.abi"
	cfg.GRPCAddr = testConfig.RPC.GRPC.ListenAddress

	log := logger.NewLogger(cfg.LogLevel)
	consumer := service.NewConsumer(cfg, log, make(chan types.EventData))

	parser, err := sqlsol.SpecLoader(cfg.SpecFile, "")
	abiSpec, err := sqlsol.AbiLoader(cfg.AbiFile, "")

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := consumer.Run(parser, abiSpec, false)
		require.NoError(t, err)
	}()

	// shutdown consumer in a few secs and wait for its end
	time.Sleep(time.Second * 2)
	consumer.Shutdown()

	wg.Wait()

	// test data stored in database for two different block ids
	eventName := "EventTest"
	filter := "EventType = 'LogEvent'"

	blockID := "2"
	eventData, err := db.GetBlock(filter, blockID)
	require.NoError(t, err)
	require.Equal(t, "2", eventData.Block)
	require.Equal(t, 1, len(eventData.Tables))

	tblData := eventData.Tables[strings.ToLower(eventName)]
	require.Equal(t, 1, len(tblData))
	require.Equal(t, "0", tblData[0]["_index"].(string))
	require.Equal(t, "2", tblData[0]["_height"].(string))
	require.Equal(t, "LogEvent", tblData[0]["_eventtype"].(string))
	require.Equal(t, "UpdateTestEvents", tblData[0]["_eventname"].(string))

	blockID = "5"
	eventData, err = db.GetBlock(filter, blockID)
	require.NoError(t, err)
	require.Equal(t, "5", eventData.Block)
	require.Equal(t, 1, len(eventData.Tables))

	tblData = eventData.Tables[strings.ToLower(eventName)]
	require.Equal(t, 1, len(tblData))
	require.Equal(t, "0", tblData[0]["_index"].(string))
	require.Equal(t, "5", tblData[0]["_height"].(string))
	require.Equal(t, "LogEvent", tblData[0]["_eventtype"].(string))
	require.Equal(t, "UpdateTestEvents", tblData[0]["_eventname"].(string))
}
