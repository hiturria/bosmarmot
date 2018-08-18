package config

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
func DefaultFlags() *Flags {
	return &Flags{
		DBAdapter: "postgres",
		DBURL:     "postgres://user:pass@localhost:5432/bosmarmot?sslmode=disable",
		DBSchema:  "bosmarmot",
		GRPCAddr:  "localhost:10997",
		LogLevel:  "debug",
		CfgFile:   "",
	}
}
