// +build integration

package service_test

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/hyperledger/burrow/core"
	"github.com/hyperledger/burrow/execution/exec"
	"github.com/hyperledger/burrow/integration"
	"github.com/monax/bosmarmot/vent/config"
	"github.com/monax/bosmarmot/vent/logger"
	"github.com/monax/bosmarmot/vent/service"
	"github.com/monax/bosmarmot/vent/test"
	"github.com/stretchr/testify/require"
)

var privateAccounts = integration.MakePrivateAccounts(5) // make keys
var genesisDoc = integration.TestGenesisDoc(privateAccounts)
var inputAccount = privateAccounts[0]
var testConfig = integration.NewTestConfig(genesisDoc)
var kern *core.Kernel

func testEventLogDecoder(log *exec.LogEvent, data map[string]string) {
	data["name"] = strings.Trim(log.Topics[2].String(), "\x00")
	data["description"] = strings.Trim(log.Topics[3].String(), "\x00")
}

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

	// Here is how we can generate events
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

	// This is a workaround for off-by-one on latest bound fixed in burrow
	time.Sleep(time.Second * 2)

	// create test db
	db, closeDB := test.NewTestDB(t)
	defer closeDB()

	// Run consumer to listen to events
	cfg := config.DefaultFlags()

	cfg.DBSchema = db.Schema
	cfg.CfgFile = os.Getenv("GOPATH") + "/src/github.com/monax/bosmarmot/vent/test/sqlsol_example.json"
	cfg.GRPCAddr = testConfig.RPC.GRPC.ListenAddress

	log := logger.NewLogger(cfg.LogLevel)

	// add event decoder for specific event name
	// this will be no longer needed once event name can be standardize
	consumer := service.NewConsumer(cfg, log)
	consumer.AddEventLogDecoder("TEST_EVENTS", testEventLogDecoder)

	err := consumer.Run()
	require.NoError(t, err)

	// test data stored in database for two different block ids
	blockID := "2"
	eventName := "EventTest"
	eventData, err := db.GetBlock(blockID)
	require.NoError(t, err)
	require.Equal(t, "2", eventData.Block)
	require.Equal(t, 2, len(eventData.Tables))

	tblData := eventData.Tables[strings.ToLower(eventName)]
	require.Equal(t, 1, len(tblData))
	require.Equal(t, "0", tblData[0]["index"])
	require.Equal(t, "2", tblData[0]["height"])
	require.Equal(t, "LogEvent", tblData[0]["eventtype"])
	require.Equal(t, "TEST_EVENTS", tblData[0]["eventname"])
	require.Equal(t, "TestEvent1", tblData[0]["testname"])
	require.Equal(t, "Description of TestEvent1", tblData[0]["testdescription"])

	blockID = "5"
	eventName = "EventTest"
	eventData, err = db.GetBlock(blockID)
	require.NoError(t, err)
	require.Equal(t, "5", eventData.Block)
	require.Equal(t, 2, len(eventData.Tables))

	tblData = eventData.Tables[strings.ToLower(eventName)]
	require.Equal(t, 1, len(tblData))
	require.Equal(t, "0", tblData[0]["index"])
	require.Equal(t, "5", tblData[0]["height"])
	require.Equal(t, "5", tblData[0]["height"])
	require.Equal(t, "LogEvent", tblData[0]["eventtype"])
	require.Equal(t, "TEST_EVENTS", tblData[0]["eventname"])
	require.Equal(t, "TestEvent4", tblData[0]["testname"])
	require.Equal(t, "Description of TestEvent4", tblData[0]["testdescription"])
}
