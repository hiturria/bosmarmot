package sqldb

import (
	"database/sql"

	"github.com/monax/bosmarmot/vent/types"
)

// DBAdapter database access interface
type DBAdapter interface {
	Open(dbURL string) (*sql.DB, error)
	TypeMapping(sqlColumnType types.SQLColumnType) (string, error)
	CreateTableQuery(tableName string, columns []types.SQLTableColumn) string
	UpsertQuery(table types.SQLTable) types.UpsertQuery
	LastBlockIDQuery() string
	FindSchemaQuery() string
	CreateSchemaQuery() string
	DropSchemaQuery() string
	FindTableQuery(tableName string) string
	TableDefinitionQuery(tableName string) string
	AlterColumnQuery(tableName string, columnName string, sqlColumnType types.SQLColumnType) string
	SelectRowQuery(tableName string, fields string, indexValue string) string
	SelectLogQuery() string
	InsertLogQuery() string
	InsertLogDetailQuery() string
	ErrorEquals(err error, sqlErrorType types.SQLErrorType) bool
}
