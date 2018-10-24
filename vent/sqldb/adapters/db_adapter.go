package adapters

import (
	"database/sql"

	"github.com/monax/bosmarmot/vent/types"
)

// DBAdapter implements database dependent interface
type DBAdapter interface {
	// Open opens a db connection and creates a new schema if the adapter supports that
	Open(dbURL string) (*sql.DB, error)
	// TypeMapping maps generic SQL column types to db adapter dependent column types
	TypeMapping(sqlColumnType types.SQLColumnType) (string, error)
	// ErrorEquals compares generic SQL errors to db adapter dependent errors
	ErrorEquals(err error, sqlErrorType types.SQLErrorType) bool
	// SecureColumnName returns columns with proper delimiters to ensure well formed column names
	SecureColumnName(columnName string) string
	// CreateTableQuery builds a CREATE TABLE query to create a new table
	CreateTableQuery(tableName string, columns []types.SQLTableColumn) (string, string)
	// LastBlockIDQuery builds a SELECT query to return the last block# from the Log table
	LastBlockIDQuery() string
	// FindTableQuery builds a SELECT query to check if a table exists
	FindTableQuery() string
	// TableDefinitionQuery builds a SELECT query to get a table structure from the Dictionary table
	TableDefinitionQuery() string
	// AlterColumnQuery builds an ALTER COLUMN query to alter a table structure (only adding columns is supported)
	AlterColumnQuery(tableName, columnName string, sqlColumnType types.SQLColumnType, length, order int) (string, string)
	// SelectRowQuery builds a SELECT query to get row values
	SelectRowQuery(tableName, fields, indexValue string) string
	// SelectLogQuery builds a SELECT query to get all tables involved in a given block transaction
	SelectLogQuery() string
	// InsertLogQuery builds an INSERT query to store data in Log table
	InsertLogQuery() string
	// UpsertQuery builds an INSERT... ON CONFLICT (or similar) query to upsert data in event tables based on PK
	UpsertQuery(table types.SQLTable, row types.EventDataRow) (string, string, []interface{}, error)
	//DeleteQuery builds a DELETE FROM event tables query based on PK
	DeleteQuery(table types.SQLTable, row types.EventDataRow) (string, string, []interface{}, error)
}
