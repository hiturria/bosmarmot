package sqldb

import (
	"database/sql"

	"github.com/monax/bosmarmot/vent/sqldb/adapters"
	"github.com/monax/bosmarmot/vent/types"
)

// DBAdapter database access interface
type DBAdapter interface {
	Open(dbURL string) (*sql.DB, error)
	GetTypeMapping(string) (string, error)
	GetCreateTableQuery(tableName string, columns []types.SQLTableColumn) string
	GetUpsertQuery(table types.SQLTable) adapters.UpsertQuery
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

	// TODO: generalize error management (similar to what we are doing with the types)
	ErrorIsDupSchema(error) bool
	ErrorIsDupColumn(error) bool
	ErrorIsDupTable(error) bool
	ErrorIsInvalidType(error) bool
	ErrorIsUndefinedTable(err error) bool
	ErrorIsUndefinedColumn(err error) bool
	ErrorIsSQL(err error) bool
}
