package postgres

import (
	"database/sql"
	"github.com/monax/bosmarmot/vent/logger"
	"fmt"
	"github.com/lib/pq"
)

// PostgreSQL specific error codes
const (
	errDupSchema = "42P06"
)

var sqlDataTypes = map[string]string{
	"INTEGER":           "INTEGER",
	"TEXT":              "TEXT",
	"VARCHAR(100)":      "VARCHAR(100)",
	"BOOLEAN":           "BOOLEAN",
	"BYTEA":             "BYTEA",
	"TIMESTAMP":         "TIMESTAMP",
	"TIMESTAMP_DEFAULT": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
	"SERIAL":            "SERIAL",
}

// Adapter implements DBAdapter fro Postgres
type Adapter struct {
	DB     *sql.DB
	Log    *logger.Logger
	Schema string
	DBURL  string
}

// NewSQLDB connects to a SQL database and creates default schema and _bosmarmot_log if missing
func NewSQLDB(dbURL string, schema string, l *logger.Logger) *Adapter {
	return &Adapter{
		Log:    l,
		Schema: schema,
		DBURL:  dbURL,
	}
}

// Open connects to a SQL database and creates default schema and _bosmarmot_log if missing
func (adapter *Adapter) Open() (*sql.DB, error) {

	db, err := sql.Open("postgres", adapter.DBURL)
	if err != nil {
		adapter.Log.Error("msg", "Error opening database connection", "err", err)
		return nil, err
	}

	adapter.DB = db
	return db, err
}

func (adapter *Adapter) SQLDataType(sqlGenericType string) string {
	if sqlDataType, ok := sqlDataTypes[sqlGenericType]; ok {
		return sqlDataType
	}
	return sqlDataTypes["VARCHAR(100)"]
}

//--------------------------------------------------------------------------------------------------------------------

// GetQueryLastBlockID returns query for last inserted blockId in log table
func (adapter *Adapter) GetQueryLastBlockID() string {
	query := `
		WITH ll AS (
			SELECT
				MAX(id) id
			FROM
				%s._bosmarmot_log
		)
		SELECT
			COALESCE(height, '0') AS height
		FROM
			ll
			LEFT OUTER JOIN %s._bosmarmot_log log ON ll.id = log.id
	;`

	return fmt.Sprintf(query, adapter.Schema, adapter.Schema)
}

// GetQueryFindSchema returns query that checks if the default schema exists (true/false)
func (adapter *Adapter) GetQueryFindSchema() string {
	query := `	SELECT
					EXISTS (
						SELECT
							1
						FROM
							pg_catalog.pg_namespace n
						WHERE
							n.nspname = '%s'
					);`

	return fmt.Sprintf(query, adapter.Schema)
}

// GetQueryCreateSchema returns query that creates schema
func (adapter *Adapter) GetQueryCreateSchema() string {
	return fmt.Sprintf("CREATE SCHEMA %s;", adapter.Schema)
}

// GetQueryDropSchema returns query that creates schema
func (adapter *Adapter) GetQueryDropSchema() string {
	return fmt.Sprintf("DROP SCHEMA %s CASCADE;", adapter.Schema)
}

// GetQueryFindTable returns query that checks if a table exists (true/false)
func (adapter *Adapter) GetQueryFindTable(table string) string {
	query := `
		SELECT
			EXISTS (
				SELECT
					1
				FROM
					pg_catalog.pg_class c
					JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
				WHERE
					n.nspname = '%s'
					AND c.relname = '%s'
					AND c.relkind = 'r'
			)
	;`

	return fmt.Sprintf(query, adapter.Schema, table)
}

// GetQueryTableDefinition returns query with table structure
func (adapter *Adapter) GetQueryTableDefinition(table string) string {
	query := `
	WITH dsc AS (
		SELECT pgd.objsubid,st.schemaname,st.relname,pgd.description
		FROM pg_catalog.pg_statio_all_tables AS st
		INNER JOIN pg_catalog.pg_description pgd ON (pgd.objoid=st.relid)
	)
	SELECT
		c.column_name ColumnName,
		(CASE 
		WHEN c.data_type='integer' AND is_nullable='NO' THEN 'SERIAL'
		WHEN c.data_type='integer' THEN 'INTEGER'
		WHEN c.data_type='boolean' THEN 'BOOLEAN'
		WHEN c.data_type='bytea' THEN 'BYTEA'
		WHEN c.data_type='text' THEN 'TEXT'
		WHEN c.udt_name='timestamp' THEN 'TIMESTAMP'
		WHEN c.udt_name='varchar' THEN CONCAT('VARCHAR(',COALESCE(c.character_maximum_length, 0),')')
		ELSE CONCAT(c.data_type ,' - ' ,c.udt_name,'(',COALESCE(c.character_maximum_length, 0),')')
		END) ColumnSQLType,
		(CASE WHEN is_nullable='NO' THEN true ELSE false END) ColumnIsPK,
		(CASE
		WHEN TRIM(COALESCE(dsc.description, '')) <>'' THEN  TRIM(COALESCE(dsc.description, '')) 
		ELSE c.column_name 
		END) ColumnDescription
	FROM
		information_schema.columns AS c
	LEFT OUTER JOIN
		dsc ON (c.ordinal_position = dsc.objsubid AND c.table_schema = dsc.schemaname AND c.table_name = dsc.relname)
	WHERE
		c.table_schema = '%s'
		AND c.table_name = '%s'
	;`

	return fmt.Sprintf(query, adapter.Schema, table)
}

//---------------------------------------------------------------------------------------------------------------------

func (adapter *Adapter) ErrorIsDupSchema(err error) bool {
	if err, ok := err.(*pq.Error); ok {
		if err.Code == errDupSchema {
			return true
		}
	}
	return false
}
