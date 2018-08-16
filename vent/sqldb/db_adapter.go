package sqldb

import "database/sql"

// DBAdapter database access interface
type DBAdapter interface {
	Open() (*sql.DB, error)

	GetQueryLastBlockID() string
	GetQueryFindSchema() string
	GetQueryCreateSchema() string
	GetQueryDropSchema() string
	GetQueryFindTable(string) string
	GetQueryTableDefinition(string) string

	ErrorIsDupSchema(error) bool

	SQLDataType(string) string

}
