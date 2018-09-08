// +build integration

package service_test

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/hyperledger/burrow/core"
	"github.com/hyperledger/burrow/integration"
	"github.com/monax/bosmarmot/vent/config"
	"github.com/monax/bosmarmot/vent/logger"
	"github.com/monax/bosmarmot/vent/service"
	"github.com/monax/bosmarmot/vent/test"
	"github.com/monax/bosmarmot/vent/types"
	"github.com/stretchr/testify/require"
)

var privateAccounts = integration.MakePrivateAccounts(5) // make keys
var genesisDoc = integration.TestGenesisDoc(privateAccounts)
var inputAccount = privateAccounts[0]
var testConfig = integration.NewTestConfig(genesisDoc)
var kern *core.Kernel

func TestMain(m *testing.M) {
	cleanup := integration.EnterTestDirectory()
	defer cleanup()

	kern = integration.TestKernel(inputAccount, privateAccounts, testConfig, nil)

	err := kern.Boot()
	if err != nil {
		panic(err)
	}
	// Sometimes better to not shutdown as logging errors on shutdown may obscure real issue
	defer func() {
		kern.Shutdown(context.Background())
	}()

	returnValue := m.Run()

	time.Sleep(3 * time.Second)
	os.Exit(returnValue)
}

func TestRun(t *testing.T) {
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
	cfg := config.DefaultFlags(types.PostgresDB)

	cfg.DBSchema = db.Schema
	cfg.CfgFile = os.Getenv("GOPATH") + "/src/github.com/monax/bosmarmot/vent/test/sqlsol_example.json"
	cfg.GRPCAddr = testConfig.RPC.GRPC.ListenAddress

	log := logger.NewLogger(cfg.LogLevel)
	consumer := service.NewConsumer(cfg, log)

	err := consumer.Run()
	require.NoError(t, err)

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
	require.Equal(t, "0", tblData[0]["_index"])
	require.Equal(t, "2", tblData[0]["_height"])
	require.Equal(t, "LogEvent", tblData[0]["_eventtype"])
	require.Equal(t, "UpdateTestEvents", tblData[0]["_eventname"])
	require.Equal(t, "TestEvent1", tblData[0]["testname"])
	require.Equal(t, "Description of TestEvent1", tblData[0]["testdescription"])

	blockID = "5"
	eventData, err = db.GetBlock(filter, blockID)
	require.NoError(t, err)
	require.Equal(t, "5", eventData.Block)
	require.Equal(t, 1, len(eventData.Tables))

	tblData = eventData.Tables[strings.ToLower(eventName)]
	require.Equal(t, 1, len(tblData))
	require.Equal(t, "0", tblData[0]["_index"])
	require.Equal(t, "5", tblData[0]["_height"])
	require.Equal(t, "LogEvent", tblData[0]["_eventtype"])
	require.Equal(t, "UpdateTestEvents", tblData[0]["_eventname"])
	require.Equal(t, "TestEvent4", tblData[0]["testname"])
	require.Equal(t, "Description of TestEvent4", tblData[0]["testdescription"])
}
