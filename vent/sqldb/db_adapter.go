package sqldb

import (
	"database/sql"

	"github.com/monax/bosmarmot/vent/types"
)

/*
This are the SQL Commands to be implemented:

LastBlockIDQuery
----------------
WITH ll AS (SELECT MAX({ID}) AS {ID} FROM {SCHEMA.LOG_TABLE} WHERE {EVENT_FILTER} = $1)
SELECT COALESCE({HEIGHT}, '0') AS {HEIGHT} FROM ll LEFT OUTER JOIN {SCHEMA.LOG_TABLE} log ON (ll.{ID} = log.{ID});


*/

// DBAdapter database access interface
type DBAdapter interface {
	//Programmatic interface
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
