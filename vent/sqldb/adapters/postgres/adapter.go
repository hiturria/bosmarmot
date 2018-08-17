package postgres

import (
	"database/sql"
	"github.com/monax/bosmarmot/vent/logger"
	"fmt"
	"github.com/lib/pq"
	"github.com/monax/bosmarmot/vent/types"
	"github.com/monax/bosmarmot/vent/sqldb/adapters"
)

// PostgreSQL specific error codes
const (
	errDupSchema       = "42P06"
	errDupColumn       = "42701"
	errDupTable        = "42P07"
	errInvalidType     = "42704"
	errUndefinedTable  = "42P01"
	errUndefinedColumn = "42703"
)

var sqlDataTypes = map[string]string{
	types.SQLColumnTypeInt:              "INTEGER",
	types.SQLColumnTypeText:             "TEXT",
	types.SQLColumnTypeVarchar100:       "VARCHAR(100)",
	types.SQLColumnTypeBool:             "BOOLEAN",
	types.SQLColumnTypeByteA:            "BYTEA",
	types.SQLColumnTypeTimeStamp:        "TIMESTAMP",
	types.SQLColumnTypeDefaultTimeStamp: "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
	types.SQLColumnTypeSerial:           "SERIAL",
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

// SQLDataType convert generic dataTypes to database dependent dataTypes
func (adapter *Adapter) SQLDataType(sqlGenericType string) (string, error) {
	if sqlDataType, ok := sqlDataTypes[sqlGenericType]; ok {
		return sqlDataType, nil
	}
	err := fmt.Errorf("datatype %s not recognized", sqlGenericType)
	return "", err
}

// GetQueryCreateTable build query for creating a table
func (adapter *Adapter) GetQueryCreateTable(tableName string, columns []types.SQLTableColumn) string {

	// build query
	columnsDef := ""
	primaryKey := ""

	for _, tableColumn := range columns {
		colName := adapters.Safe(tableColumn.Name)
		colType := adapters.Safe(tableColumn.Type)

		if columnsDef != "" {
			columnsDef += ", "
		}

		columnsDef += fmt.Sprintf("%s %s", colName, colType)

		if tableColumn.Primary {
			columnsDef += " NOT NULL"
			if primaryKey != "" {
				primaryKey += ", "
			}
			primaryKey += colName
		}
	}

	query := fmt.Sprintf("CREATE TABLE %s.%s (%s", adapter.Schema, tableName, columnsDef)
	if primaryKey != "" {
		query += "," + fmt.Sprintf("CONSTRAINT %s_pkey PRIMARY KEY (%s)", tableName, primaryKey)
	}
	query += ");"

	return query
}

// getUpsertQuery builds a query for upsert
func (adapter *Adapter) GetQueryUpsert(table types.SQLTable) adapters.UpsertQuery {
	columns := ""
	insValues := ""
	updValues := ""
	cols := len(table.Columns)
	nKeys := 0
	cKey := 0

	upsertQuery := adapters.UpsertQuery{
		Query:   "",
		Length:  0,
		Columns: make(map[string]adapters.UpsertColumn),
	}

	i := 0

	for _, tableColumn := range table.Columns {
		isNum := adapters.IsNumeric(tableColumn.Type)
		safeCol := adapters.Safe(tableColumn.Name)
		cKey = 0
		i++

		// INSERT INTO TABLE (*columns).........
		if columns != "" {
			columns += ", "
			insValues += ", "
		}
		columns += safeCol
		insValues += "$" + fmt.Sprintf("%d", i)

		if !tableColumn.Primary {
			cKey = cols + nKeys
			nKeys++

			// INSERT........... ON CONFLICT......DO UPDATE (*updValues)
			if updValues != "" {
				updValues += ", "
			}
			updValues += safeCol + " = $" + fmt.Sprintf("%d", cKey+1)
		}

		upsertQuery.Columns[safeCol] = adapters.UpsertColumn{
			IsNumeric:   isNum,
			InsPosition: i - 1,
			UpdPosition: cKey,
		}
	}
	upsertQuery.Length = cols + nKeys

	safeTable := adapters.Safe(table.Name)
	query := fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES (%s) ", adapter.Schema, safeTable, columns, insValues)

	if nKeys != 0 {
		query += fmt.Sprintf("ON CONFLICT ON CONSTRAINT %s_pkey DO UPDATE SET ", safeTable)
		query += updValues
	} else {
		query += fmt.Sprintf("ON CONFLICT ON CONSTRAINT %s_pkey DO NOTHING", safeTable)
	}
	query += ";"

	upsertQuery.Query = query
	return upsertQuery
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
func (adapter *Adapter) GetQueryFindTable(tableName string) string {
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

	return fmt.Sprintf(query, adapter.Schema, tableName)
}

// GetQueryTableDefinition returns query with table structure
func (adapter *Adapter) GetQueryTableDefinition(tableName string) string {
	//WHEN c.data_type='integer' AND is_nullable='NO' THEN 'SERIAL'

	query := `
	WITH dsc AS (
		SELECT pgd.objsubid,st.schemaname,st.relname,pgd.description
		FROM pg_catalog.pg_statio_all_tables AS st
		INNER JOIN pg_catalog.pg_description pgd ON (pgd.objoid=st.relid)
	)
	SELECT
		c.column_name ColumnName,
		(CASE 
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

	return fmt.Sprintf(query, adapter.Schema, tableName)
}

// GetQueryAlterTable returns query for adding a new column to a table
func (adapter *Adapter) GetQueryAlterColumn(tableName string, columnName string, sqlGenericType string) string {
	sqlType, _ := adapter.SQLDataType(sqlGenericType)
	return fmt.Sprintf("ALTER TABLE %s.%s ADD COLUMN %s %s;", adapter.Schema, tableName, tableName, sqlType)
}

// GetQueryCommentColumn returns query for commenting a column
func (adapter *Adapter) GetQueryCommentColumn(tableName string, columnName string, comment string) string {
	return fmt.Sprintf("COMMENT ON COLUMN %s.%s.%s IS '%s';", adapter.Schema, tableName, columnName, comment)
}

// GetQuerySelectRow returns query for selecting row values
func (adapter *Adapter) GetQuerySelectRow(tableName string, fields string, indexValue string) string {
	return fmt.Sprintf("SELECT %s FROM %s.%s WHERE height='%s';", fields, adapter.Schema, tableName, indexValue)
}

// GetQuerySelectLog returns query for selecting all tables in a block trn
func (adapter *Adapter) GetQuerySelectLog() string {
	query := `
		SELECT
			tblname,
			tblmap
		FROM
			%s._bosmarmot_log l
			INNER JOIN %s._bosmarmot_logdet d ON l.id = d.id
		WHERE
			height = $1;
	`
	query = fmt.Sprintf(query, adapter.Schema, adapter.Schema)
	return query
}

// GetQueryInsertLog returns query for inserting into log
func (adapter *Adapter) GetQueryInsertLog() string {
	return fmt.Sprintf("INSERT INTO %s._bosmarmot_log (registers, height) VALUES ($1, $2) RETURNING id", adapter.Schema)
}

// GetQueryInsertLogDetail returns query for inserting into logdetail
func (adapter *Adapter) GetQueryInsertLogDetail() string {
	return fmt.Sprintf("INSERT INTO %s._bosmarmot_logdet (id,tblname,tblmap,registers) VALUES ($1,$2,$3,$4)", adapter.Schema)
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

func (adapter *Adapter) ErrorIsDupColumn(err error) bool {
	if err, ok := err.(*pq.Error); ok {
		if err.Code == errDupColumn {
			return true
		}
	}
	return false
}

func (adapter *Adapter) ErrorIsDupTable(err error) bool {
	if err, ok := err.(*pq.Error); ok {
		if err.Code == errDupTable {
			return true
		}
	}
	return false
}

func (adapter *Adapter) ErrorIsInvalidType(err error) bool {
	if err, ok := err.(*pq.Error); ok {
		if err.Code == errInvalidType {
			return true
		}
	}
	return false
}

func (adapter *Adapter) ErrorIsUndefinedTable(err error) bool {
	if err, ok := err.(*pq.Error); ok {
		if err.Code == errUndefinedTable {
			return true
		}
	}
	return false
}

func (adapter *Adapter) ErrorIsUndefinedColumn(err error) bool {
	if err, ok := err.(*pq.Error); ok {
		if err.Code == errUndefinedColumn {
			return true
		}
	}
	return false
}

func (adapter *Adapter) ErrorIsSQL(err error) bool {
	if _, ok := err.(*pq.Error); ok {
		return true
	}
	return false
}
