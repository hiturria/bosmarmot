package cmd

import (
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/monax/bosmarmot/vent/config"
	"github.com/monax/bosmarmot/vent/logger"
	"github.com/monax/bosmarmot/vent/service"
	"github.com/monax/bosmarmot/vent/types"
	"github.com/spf13/cobra"
)

var ventCmd = &cobra.Command{
	Use:   "vent",
	Short: "Vent - an EVM event to SQL database mapping layer",
	Run:   runVentCmd,
}

//TODO: Generalize for SQLiteDB
var cfg = config.DefaultFlags(types.PostgresDB)

func init() {
	ventCmd.Flags().StringVar(&cfg.DBAdapter, "db-adapter", cfg.DBAdapter, "Database adapter, 'postgres' or 'sqlite' are fully supported")
	ventCmd.Flags().StringVar(&cfg.DBURL, "db-url", cfg.DBURL, "PostgreSQL database URL or SQLite db file path")
	ventCmd.Flags().StringVar(&cfg.DBSchema, "db-schema", cfg.DBSchema, "PostgreSQL database schema or empty for SQLite")
	ventCmd.Flags().StringVar(&cfg.GRPCAddr, "grpc-addr", cfg.GRPCAddr, "Address to listen to gRPC Hyperledger Burrow server")
	ventCmd.Flags().StringVar(&cfg.LogLevel, "log-level", cfg.LogLevel, "Logging level (error, warn, info, debug)")
	ventCmd.Flags().StringVar(&cfg.CfgFile, "cfg-file", cfg.CfgFile, "SQLSol json specification file full path")
}

// Execute executes the vent command
func Execute() {
	ventCmd.Execute()
}

func runVentCmd(cmd *cobra.Command, args []string) {
	// create the events consumer
	log := logger.NewLogger(cfg.LogLevel)
	consumer := service.NewConsumer(cfg, log)

	// setup channel for termination signals
	ch := make(chan os.Signal)

	signal.Notify(ch, syscall.SIGTERM)
	signal.Notify(ch, syscall.SIGINT)

	// start the events consumer
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		if err := consumer.Run(); err != nil {
			log.Error("err", err)
			os.Exit(1)
		}

		wg.Done()
	}()

	// wait for a termination signal from the OS and
	// gracefully shutdown the events consumer in that case
	go func() {
		<-ch
		consumer.Shutdown()
	}()

	// wait until the events consumer is done
	wg.Wait()
	os.Exit(0)
}
