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
	CfgFile   string
}

// DefaultFlags returns a configuration with default values
func DefaultFlags(database string) *Flags {
	switch database {
	case types.PostgresDB:
		return &Flags{
			DBAdapter: database,
			DBURL:     "postgres://user:pass@localhost:5432/vent?sslmode=disable",
			DBSchema:  "vent",
			GRPCAddr:  "localhost:10997",
			LogLevel:  "debug",
			CfgFile:   "",
		}

	case types.SQLite:

		return &Flags{
			DBAdapter: database,
			DBURL:     "./vent.db",
			DBSchema:  "",
			GRPCAddr:  "",
			LogLevel:  "debug",
			CfgFile:   "",
		}
	}

	return &Flags{}
}
