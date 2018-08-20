package sqldb

import (
	"database/sql"

	"github.com/monax/bosmarmot/vent/types"
)

// DBAdapter database access interface
type DBAdapter interface {
	Open(dbURL string) (*sql.DB, error)
	GetTypeMapping(sqlGenericType string) (string, error)
	GetCreateTableQuery(tableName string, columns []types.SQLTableColumn) string
	GetUpsertQuery(table types.SQLTable) types.UpsertQuery
	GetLastBlockIDQuery() string
	GetFindSchemaQuery() string
	GetCreateSchemaQuery() string
	GetDropSchemaQuery() string
	GetFindTableQuery(tableName string) string
	GetTableDefinitionQuery(tableName string) string
	GetAlterColumnQuery(tableName string, columnName string, sqlGenericType string) string
	GetSelectRowQuery(tableName string, fields string, indexValue string) string
	GetSelectLogQuery() string
	GetInsertLogQuery() string
	GetInsertLogDetailQuery() string
	ErrorEquals(err error, SQLError string) bool
}
