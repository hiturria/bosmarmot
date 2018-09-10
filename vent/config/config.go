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
func DefaultFlags(database ...string) *Flags {
	db := ""
	if len(database) > 0 {
		db = database[0]
	} else {
		db = types.PostgresDB
	}

	switch db {
	case types.PostgresDB:
		return &Flags{
			DBAdapter: db,
			DBURL:     "postgres://user:pass@localhost:5432/vent?sslmode=disable",
			DBSchema:  "vent",
			GRPCAddr:  "localhost:10997",
			LogLevel:  "debug",
			CfgFile:   "",
		}

	case types.SQLiteDB:

		return &Flags{
			DBAdapter: db,
			DBURL:     "./vent",
			DBSchema:  "",
			GRPCAddr:  "",
			LogLevel:  "debug",
			CfgFile:   "",
		}
	}

	return &Flags{}
}
