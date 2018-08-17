package sqldb

import (
	"database/sql"
	"github.com/monax/bosmarmot/vent/types"
	"github.com/monax/bosmarmot/vent/sqldb/adapters"
)

// DBAdapter database access interface
type DBAdapter interface {
	Open() (*sql.DB, error)
	SQLDataType(string) (string, error)

	GetQueryCreateTable(tableName string, columns []types.SQLTableColumn) string
	GetQueryUpsert(table types.SQLTable) adapters.UpsertQuery

	GetQueryLastBlockID() string
	GetQueryFindSchema() string
	GetQueryCreateSchema() string
	GetQueryDropSchema() string
	GetQueryFindTable(tableName string) string
	GetQueryTableDefinition(tableName string) string
	GetQueryAlterColumn(tableName string, columnName string, sqlGenericType string) string
	GetQueryCommentColumn(tableName string, columnName string, comment string) string
	GetQuerySelectRow(tableName string, fields string, indexValue string) string
	GetQuerySelectLog() string
	GetQueryInsertLog() string
	GetQueryInsertLogDetail() string

	ErrorIsDupSchema(error) bool
	ErrorIsDupColumn(error) bool
	ErrorIsDupTable(error) bool
	ErrorIsInvalidType(error) bool
	ErrorIsUndefinedTable(err error) bool
	ErrorIsUndefinedColumn(err error) bool
	ErrorIsSQL(err error) bool
}
