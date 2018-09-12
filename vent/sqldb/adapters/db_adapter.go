package adapters

import (
	"database/sql"

	"github.com/monax/bosmarmot/vent/types"
)

// DBAdapter database access interface
type DBAdapter interface {
	Open(dbURL string) (*sql.DB, error)
	TypeMapping(sqlColumnType types.SQLColumnType) (string, error)
	ErrorEquals(err error, sqlErrorType types.SQLErrorType) bool
	SecureColumnName(columnName string) string
	CreateTableQuery(tableName string, columns []types.SQLTableColumn) (string, string)
	UpsertQuery(table types.SQLTable) types.UpsertQuery
	LastBlockIDQuery() string
	FindTableQuery() string
	TableDefinitionQuery() string
	AlterColumnQuery(tableName, columnName string, sqlColumnType types.SQLColumnType, length, order int) (string, string)
	SelectRowQuery(tableName, fields, indexValue string) string
	SelectLogQuery() string
	InsertLogQuery() string
}
