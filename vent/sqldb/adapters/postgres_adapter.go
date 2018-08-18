package adapters

import (
	"database/sql"
	"fmt"

	"github.com/lib/pq"
	"github.com/monax/bosmarmot/vent/logger"
	"github.com/monax/bosmarmot/vent/types"
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
	types.SQLColumnTypeBool:      "BOOLEAN",
	types.SQLColumnTypeByteA:     "BYTEA",
	types.SQLColumnTypeInt:       "INTEGER",
	types.SQLColumnTypeSerial:    "SERIAL",
	types.SQLColumnTypeText:      "TEXT",
	types.SQLColumnTypeVarchar:   "VARCHAR",
	types.SQLColumnTypeTimeStamp: "TIMESTAMP",
}

// PostgresAdapter implements DBAdapter for Postgres
type PostgresAdapter struct {
	Log    *logger.Logger
	Schema string
}

// NewPostgresAdapter connects to a SQL database and creates default schema and _bosmarmot_log if missing
func NewPostgresAdapter(schema string, log *logger.Logger) *PostgresAdapter {
	return &PostgresAdapter{
		Log:    log,
		Schema: schema,
	}
}

// Open connects to a SQL database and creates default schema and _bosmarmot_log if missing
func (adapter *PostgresAdapter) Open(dbURL string) (*sql.DB, error) {
	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		adapter.Log.Debug("msg", "Error opening database connection", "err", err)
		return nil, err
	}

	return db, err
}

// GetTypeMapping convert generic dataTypes to database dependent dataTypes
func (adapter *PostgresAdapter) GetTypeMapping(sqlGenericType string) (string, error) {
	if sqlDataType, ok := sqlDataTypes[sqlGenericType]; ok {
		return sqlDataType, nil
	}
	err := fmt.Errorf("datatype %s not recognized", sqlGenericType)
	return "", err
}

// GetCreateTableQuery build query for creating a table
func (adapter *PostgresAdapter) GetCreateTableQuery(tableName string, columns []types.SQLTableColumn) string {
	// build query
	columnsDef := ""
	primaryKey := ""

	for _, tableColumn := range columns {
		colName := Safe(tableColumn.Name)
		colType := Safe(tableColumn.Type)

		if columnsDef != "" {
			columnsDef += ", "
		}

		columnsDef += fmt.Sprintf("%s %s", colName, colType)

		if tableColumn.Length > 0 {
			columnsDef += fmt.Sprintf("(%v)", tableColumn.Length)
		}

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

// GetUpsertQuery builds a query for upsert
func (adapter *PostgresAdapter) GetUpsertQuery(table types.SQLTable) UpsertQuery {
	columns := ""
	insValues := ""
	updValues := ""
	cols := len(table.Columns)
	nKeys := 0
	cKey := 0

	upsertQuery := UpsertQuery{
		Query:   "",
		Length:  0,
		Columns: make(map[string]UpsertColumn),
	}

	i := 0

	for _, tableColumn := range table.Columns {
		isNum := IsNumeric(tableColumn.Type)
		safeCol := Safe(tableColumn.Name)
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

		upsertQuery.Columns[safeCol] = UpsertColumn{
			IsNumeric:   isNum,
			InsPosition: i - 1,
			UpdPosition: cKey,
		}
	}
	upsertQuery.Length = cols + nKeys

	safeTable := Safe(table.Name)
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

// GetLastBlockIDQuery returns query for last inserted blockId in log table
func (adapter *PostgresAdapter) GetLastBlockIDQuery() string {
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

// GetFindSchemaQuery returns query that checks if the default schema exists (true/false)
func (adapter *PostgresAdapter) GetFindSchemaQuery() string {
	query := `
		SELECT
			EXISTS (
				SELECT
					1
				FROM
					pg_catalog.pg_namespace n
				WHERE
					n.nspname = '%s'
			)
	;`

	return fmt.Sprintf(query, adapter.Schema)
}

// GetCreateSchemaQuery returns query that creates schema
func (adapter *PostgresAdapter) GetCreateSchemaQuery() string {
	return fmt.Sprintf("CREATE SCHEMA %s;", adapter.Schema)
}

// GetDropSchemaQuery returns query that creates schema
func (adapter *PostgresAdapter) GetDropSchemaQuery() string {
	return fmt.Sprintf("DROP SCHEMA %s CASCADE;", adapter.Schema)
}

// GetFindTableQuery returns query that checks if a table exists (true/false)
func (adapter *PostgresAdapter) GetFindTableQuery(tableName string) string {
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

// GetTableDefinitionQuery returns query with table structure
func (adapter *PostgresAdapter) GetTableDefinitionQuery(tableName string) string {

	return fmt.Sprintf(`
		WITH dsc AS (
			SELECT
				pgd.objsubid,
				st.schemaname,
				st.relname,
				pgd.description
			FROM
				pg_catalog.pg_statio_all_tables AS st
				INNER JOIN pg_catalog.pg_description pgd ON (pgd.objoid = st.relid)
		)
		SELECT
			c.column_name ColumnName,
			(
				CASE
					WHEN c.data_type = 'integer' THEN '%s'
					WHEN c.data_type = 'boolean' THEN '%s'
					WHEN c.data_type = 'bytea' THEN '%s'
					WHEN c.data_type = 'text' THEN '%s'
					WHEN c.udt_name = 'timestamp' THEN '%s'
					WHEN c.udt_name = 'varchar' THEN '%s'
					ELSE CONCAT(c.data_type, ' - ', c.udt_name, '(', COALESCE(c.character_maximum_length, 0), ')')
				END
			) ColumnSQLType,
			(
				CASE
					WHEN is_nullable = 'NO' THEN true
					ELSE false
				END
			) ColumnIsPK,
			(
				CASE
					WHEN TRIM(COALESCE(dsc.description, '')) <> '' THEN TRIM(COALESCE(dsc.description, ''))
					ELSE c.column_name
				END
			) ColumnDescription,
			COALESCE(c.character_maximum_length,0) ColumnLength
		FROM
			information_schema.columns AS c
		LEFT OUTER JOIN
			dsc ON (c.ordinal_position = dsc.objsubid AND c.table_schema = dsc.schemaname AND c.table_name = dsc.relname)
		WHERE
			c.table_schema = '%s'
			AND c.table_name = '%s'
	;`,
		types.SQLColumnTypeInt,
		types.SQLColumnTypeBool,
		types.SQLColumnTypeByteA,
		types.SQLColumnTypeText,
		types.SQLColumnTypeTimeStamp,
		types.SQLColumnTypeVarchar,
		adapter.Schema,
		tableName,
	)
}

// GetQueryAlterTable returns query for adding a new column to a table
func (adapter *PostgresAdapter) GetAlterColumnQuery(tableName string, columnName string, sqlGenericType string) string {
	sqlType, _ := adapter.GetTypeMapping(sqlGenericType)
	return fmt.Sprintf("ALTER TABLE %s.%s ADD COLUMN %s %s;", adapter.Schema, tableName, tableName, sqlType)
}

// GetCommentColumnQuery returns query for commenting a column
func (adapter *PostgresAdapter) GetCommentColumnQuery(tableName string, columnName string, comment string) string {
	return fmt.Sprintf("COMMENT ON COLUMN %s.%s.%s IS '%s';", adapter.Schema, tableName, columnName, comment)
}

// GetSelectRowQuery returns query for selecting row values
func (adapter *PostgresAdapter) GetSelectRowQuery(tableName string, fields string, indexValue string) string {
	return fmt.Sprintf("SELECT %s FROM %s.%s WHERE height='%s';", fields, adapter.Schema, tableName, indexValue)
}

// GetSelectLogQuery returns query for selecting all tables in a block trn
func (adapter *PostgresAdapter) GetSelectLogQuery() string {
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

// GetInsertLogQuery returns query for inserting into log
func (adapter *PostgresAdapter) GetInsertLogQuery() string {
	return fmt.Sprintf("INSERT INTO %s._bosmarmot_log (timestamp, registers, height) VALUES (CURRENT_TIMESTAMP, $1, $2) RETURNING id", adapter.Schema)
}

// GetInsertLogDetailQuery returns query for inserting into logdetail
func (adapter *PostgresAdapter) GetInsertLogDetailQuery() string {
	return fmt.Sprintf("INSERT INTO %s._bosmarmot_logdet (id, tblname, tblmap, registers) VALUES ($1, $2, $3, $4)", adapter.Schema)
}

//---------------------------------------------------------------------------------------------------------------------

func (adapter *PostgresAdapter) ErrorIsDupSchema(err error) bool {
	if err, ok := err.(*pq.Error); ok {
		if err.Code == errDupSchema {
			return true
		}
	}
	return false
}

func (adapter *PostgresAdapter) ErrorIsDupColumn(err error) bool {
	if err, ok := err.(*pq.Error); ok {
		if err.Code == errDupColumn {
			return true
		}
	}
	return false
}

func (adapter *PostgresAdapter) ErrorIsDupTable(err error) bool {
	if err, ok := err.(*pq.Error); ok {
		if err.Code == errDupTable {
			return true
		}
	}
	return false
}

func (adapter *PostgresAdapter) ErrorIsInvalidType(err error) bool {
	if err, ok := err.(*pq.Error); ok {
		if err.Code == errInvalidType {
			return true
		}
	}
	return false
}

func (adapter *PostgresAdapter) ErrorIsUndefinedTable(err error) bool {
	if err, ok := err.(*pq.Error); ok {
		if err.Code == errUndefinedTable {
			return true
		}
	}
	return false
}

func (adapter *PostgresAdapter) ErrorIsUndefinedColumn(err error) bool {
	if err, ok := err.(*pq.Error); ok {
		if err.Code == errUndefinedColumn {
			return true
		}
	}
	return false
}

func (adapter *PostgresAdapter) ErrorIsSQL(err error) bool {
	if _, ok := err.(*pq.Error); ok {
		return true
	}
	return false
}
