package types

// SQLDatabaseType stores supported databases
type SQLDatabaseType int

// supported databases
const (
	PostgresDB SQLDatabaseType = iota
	SQLite
)
