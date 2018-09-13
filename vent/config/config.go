package config

import (
	"github.com/monax/bosmarmot/vent/types"
)

// Flags is a set of configuration parameters
type Flags struct {
	DBAdapter string
	DBURL     string
	DBSchema  string
	GRPCAddr  string
	LogLevel  string
	SpecFile  string
}

// DefaultFlags returns a configuration with default values
func DefaultFlags() *Flags {
	return &Flags{
		DBAdapter: types.PostgresDB,
		DBURL:     "postgres://user:pass@localhost:5432/vent?sslmode=disable",
		DBSchema:  "vent",
		GRPCAddr:  "localhost:10997",
		LogLevel:  "debug",
		SpecFile:  "",
	}
}
