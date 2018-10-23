// +build integration

package service_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
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

func TestServer(t *testing.T) {
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

	parser, err := sqlsol.SpecLoader("", cfg.SpecFile, false)
	abiSpec, err := sqlsol.AbiLoader("", cfg.AbiFile)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		err := consumer.Run(parser, abiSpec, false)
		require.NoError(t, err)

		wg.Done()
	}()

	time.Sleep(2 * time.Second)

	// setup test server
	server := service.NewServer(cfg, log, consumer)

	httpServer := httptest.NewServer(server)
	defer httpServer.Close()

	// call health endpoint should return OK
	healthURL := fmt.Sprintf("%s/health", httpServer.URL)

	resp, err := http.Get(healthURL)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// shutdown consumer and wait for its end
	consumer.Shutdown()
	wg.Wait()

	// call health endpoint again should return error
	resp, err = http.Get(healthURL)
	require.NoError(t, err)
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
}
