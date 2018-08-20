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

// NewPostgresAdapter constructs a new db adapter
func NewPostgresAdapter(schema string, log *logger.Logger) *PostgresAdapter {
	return &PostgresAdapter{
		Log:    log,
		Schema: schema,
	}
}

// Open connects to a SQL database and opens it
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

// GetCreateTableQuery builds query for creating a new table
func (adapter *PostgresAdapter) GetCreateTableQuery(tableName string, columns []types.SQLTableColumn) string {
	// build query
	columnsDef := ""
	primaryKey := ""

	for _, tableColumn := range columns {
		colName := Safe(tableColumn.Name)
		sqlType, _ := adapter.GetTypeMapping(tableColumn.Type)

		if columnsDef != "" {
			columnsDef += ", "
		}

		columnsDef += fmt.Sprintf("%s %s", colName, Safe(sqlType))

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

// GetUpsertQuery builds a query for upserting rows
func (adapter *PostgresAdapter) GetUpsertQuery(table types.SQLTable) types.UpsertQuery {
	columns := ""
	insValues := ""
	updValues := ""
	cols := len(table.Columns)
	nKeys := 0
	cKey := 0

	upsertQuery := types.UpsertQuery{
		Query:   "",
		Length:  0,
		Columns: make(map[string]types.UpsertColumn),
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

		upsertQuery.Columns[safeCol] = types.UpsertColumn{
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

// GetLastBlockIDQuery returns a query for last inserted blockId in log table
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

// GetFindSchemaQuery returns a query that checks if the default schema exists
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

// GetCreateSchemaQuery returns a query that creates a PostgreSQL schema
func (adapter *PostgresAdapter) GetCreateSchemaQuery() string {
	return fmt.Sprintf("CREATE SCHEMA %s;", adapter.Schema)
}

// GetDropSchemaQuery returns a query that drops a PostgreSQL schema
func (adapter *PostgresAdapter) GetDropSchemaQuery() string {
	return fmt.Sprintf("DROP SCHEMA %s CASCADE;", adapter.Schema)
}

// GetFindTableQuery returns a query that checks if a table exists
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

// GetTableDefinitionQuery returns a query with table structure
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

// GetAlterColumnQuery returns a query for adding a new column to a table
func (adapter *PostgresAdapter) GetAlterColumnQuery(tableName string, columnName string, sqlGenericType string) string {
	sqlType, _ := adapter.GetTypeMapping(sqlGenericType)
	return fmt.Sprintf("ALTER TABLE %s.%s ADD COLUMN %s %s;", adapter.Schema, tableName, tableName, sqlType)
}

// GetSelectRowQuery returns a query for selecting row values
func (adapter *PostgresAdapter) GetSelectRowQuery(tableName string, fields string, indexValue string) string {
	return fmt.Sprintf("SELECT %s FROM %s.%s WHERE height='%s';", fields, adapter.Schema, tableName, indexValue)
}

// GetSelectLogQuery returns a query for selecting all tables involved in a block trn
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

// GetInsertLogQuery returns a query to insert a row in log table
func (adapter *PostgresAdapter) GetInsertLogQuery() string {
	return fmt.Sprintf("INSERT INTO %s._bosmarmot_log (timestamp, registers, height) VALUES (CURRENT_TIMESTAMP, $1, $2) RETURNING id", adapter.Schema)
}

// GetInsertLogDetailQuery returns a query to insert a row into logdetail table
func (adapter *PostgresAdapter) GetInsertLogDetailQuery() string {
	return fmt.Sprintf("INSERT INTO %s._bosmarmot_logdet (id, tblname, tblmap, registers) VALUES ($1, $2, $3, $4)", adapter.Schema)
}

// ErrorEquals verify if an error is of a given SQL type
func (adapter *PostgresAdapter) ErrorEquals(err error, SQLError string) bool {
	if err, ok := err.(*pq.Error); ok {
		switch SQLError {
		case types.ErrGenericSQL:
			return true

		case types.ErrDupColumn:
			if err.Code == errDupColumn {
				return true
			}

		case types.ErrDupTable:
			if err.Code == errDupTable {
				return true
			}

		case types.ErrDupSchema:
			if err.Code == errDupSchema {
				return true
			}

		case types.ErrUndefinedTable:
			if err.Code == errUndefinedTable {
				return true
			}

		case types.ErrUndefinedColumn:
			if err.Code == errUndefinedColumn {
				return true
			}

		case types.ErrInvalidType:
			if err.Code == errInvalidType {
				return true
			}

		}
	}

	return false
}
