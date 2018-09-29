package cmd

import (
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/monax/bosmarmot/vent/config"
	"github.com/monax/bosmarmot/vent/logger"
	"github.com/monax/bosmarmot/vent/service"
	"github.com/spf13/cobra"
)

var ventCmd = &cobra.Command{
	Use:   "vent",
	Short: "Vent - an EVM event to SQL database mapping layer",
	Run:   runVentCmd,
}

var cfg = config.DefaultFlags()

func init() {
	ventCmd.Flags().StringVar(&cfg.DBAdapter, "db-adapter", cfg.DBAdapter, "Database adapter, 'postgres' or 'sqlite' are fully supported")
	ventCmd.Flags().StringVar(&cfg.DBURL, "db-url", cfg.DBURL, "PostgreSQL database URL or SQLite db file path")
	ventCmd.Flags().StringVar(&cfg.DBSchema, "db-schema", cfg.DBSchema, "PostgreSQL database schema (empty for SQLite)")
	ventCmd.Flags().StringVar(&cfg.GRPCAddr, "grpc-addr", cfg.GRPCAddr, "Address to connect to the Hyperledger Burrow gRPC server")
	ventCmd.Flags().StringVar(&cfg.HTTPAddr, "http-addr", cfg.HTTPAddr, "Address to bind the HTTP server")
	ventCmd.Flags().StringVar(&cfg.LogLevel, "log-level", cfg.LogLevel, "Logging level (error, warn, info, debug)")
	ventCmd.Flags().StringVar(&cfg.SpecFile, "spec-file", cfg.SpecFile, "SQLSol json specification file full path")
	ventCmd.Flags().StringVar(&cfg.SpecDir, "spec-dir", cfg.SpecDir, "Path of a folder to look for SQLSol json specification files")
}

// Execute executes the vent command
func Execute() {
	if err := ventCmd.Execute(); err != nil {
		os.Exit(-1)
	}
}

func runVentCmd(cmd *cobra.Command, args []string) {
	log := logger.NewLogger(cfg.LogLevel)
	consumer := service.NewConsumer(cfg, log)
	server := service.NewServer(cfg, log, consumer)

	var wg sync.WaitGroup

	// setup channel for termination signals
	ch := make(chan os.Signal)

	signal.Notify(ch, syscall.SIGTERM)
	signal.Notify(ch, syscall.SIGINT)

	// start the events consumer
	wg.Add(1)

	go func() {
		if err := consumer.Run(); err != nil {
			log.Error("err", err)
			os.Exit(1)
		}

		wg.Done()
	}()

	// start the http server
	wg.Add(1)

	go func() {
		server.Run()
		wg.Done()
	}()

	// wait for a termination signal from the OS and
	// gracefully shutdown the events consumer and the http server
	go func() {
		<-ch
		consumer.Shutdown()
		server.Shutdown()
	}()

	// wait until the events consumer and the http server are done
	wg.Wait()
	os.Exit(0)
}
