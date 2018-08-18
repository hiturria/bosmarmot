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
	Short: "Vent is an events consumer for Bos",
	Run:   runVentCmd,
}

var cfg = config.DefaultFlags()

func init() {
	ventCmd.Flags().StringVar(&cfg.DBAdapter, "db-adapter", cfg.DBAdapter, "Database adatper (only 'postgres' is accepted for now)")
	ventCmd.Flags().StringVar(&cfg.DBURL, "db-url", cfg.DBURL, "Postgres database URL")
	ventCmd.Flags().StringVar(&cfg.DBSchema, "db-schema", cfg.DBSchema, "Postgres database schema")
	ventCmd.Flags().StringVar(&cfg.GRPCAddr, "grpc-addr", cfg.GRPCAddr, "Burrow gRPC address")
	ventCmd.Flags().StringVar(&cfg.LogLevel, "log-level", cfg.LogLevel, "Logging level (error, warn, info, debug)")
	ventCmd.Flags().StringVar(&cfg.CfgFile, "cfg-file", cfg.CfgFile, "Event configuration file (full path)")
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
