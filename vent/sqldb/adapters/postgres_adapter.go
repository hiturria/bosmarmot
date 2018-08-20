package adapters

import (
	"database/sql"
	"fmt"

	"github.com/lib/pq"
	"github.com/monax/bosmarmot/vent/logger"
	"github.com/monax/bosmarmot/vent/types"
)

var sqlDataTypes = map[types.SQLColumnType]string{
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

// TypeMapping convert generic dataTypes to database dependent dataTypes
func (adapter *PostgresAdapter) TypeMapping(sqlColumnType types.SQLColumnType) (string, error) {
	if sqlDataType, ok := sqlDataTypes[sqlColumnType]; ok {
		return sqlDataType, nil
	}
	err := fmt.Errorf("datatype %v not recognized", sqlColumnType)
	return "", err
}

// CreateTableQuery builds query for creating a new table
func (adapter *PostgresAdapter) CreateTableQuery(tableName string, columns []types.SQLTableColumn) string {
	// build query
	columnsDef := ""
	primaryKey := ""

	for _, tableColumn := range columns {
		sqlType, _ := adapter.TypeMapping(tableColumn.Type)

		if columnsDef != "" {
			columnsDef += ", "
		}

		columnsDef += fmt.Sprintf("%s %s", tableColumn.Name, sqlType)

		if tableColumn.Length > 0 {
			columnsDef += fmt.Sprintf("(%v)", tableColumn.Length)
		}

		if tableColumn.Primary {
			columnsDef += " NOT NULL"
			if primaryKey != "" {
				primaryKey += ", "
			}
			primaryKey += tableColumn.Name
		}
	}

	query := fmt.Sprintf("CREATE TABLE %s.%s (%s", adapter.Schema, tableName, columnsDef)
	if primaryKey != "" {
		query += "," + fmt.Sprintf("CONSTRAINT %s_pkey PRIMARY KEY (%s)", tableName, primaryKey)
	}
	query += ");"

	return query
}

// UpsertQuery builds a query for upserting rows
func (adapter *PostgresAdapter) UpsertQuery(table types.SQLTable) types.UpsertQuery {
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
		cKey = 0
		i++

		// INSERT INTO TABLE (*columns).........
		if columns != "" {
			columns += ", "
			insValues += ", "
		}
		columns += tableColumn.Name
		insValues += "$" + fmt.Sprintf("%d", i)

		if !tableColumn.Primary {
			cKey = cols + nKeys
			nKeys++

			// INSERT........... ON CONFLICT......DO UPDATE (*updValues)
			if updValues != "" {
				updValues += ", "
			}
			updValues += tableColumn.Name + " = $" + fmt.Sprintf("%d", cKey+1)
		}

		upsertQuery.Columns[tableColumn.Name] = types.UpsertColumn{
			IsNumeric:   tableColumn.Type.IsNumeric(),
			InsPosition: i - 1,
			UpdPosition: cKey,
		}
	}
	upsertQuery.Length = cols + nKeys

	query := fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES (%s) ", adapter.Schema, table.Name, columns, insValues)

	if nKeys != 0 {
		query += fmt.Sprintf("ON CONFLICT ON CONSTRAINT %s_pkey DO UPDATE SET ", table.Name)
		query += updValues
	} else {
		query += fmt.Sprintf("ON CONFLICT ON CONSTRAINT %s_pkey DO NOTHING", table.Name)
	}
	query += ";"

	upsertQuery.Query = query
	return upsertQuery
}

// LastBlockIDQuery returns a query for last inserted blockId in log table
func (adapter *PostgresAdapter) LastBlockIDQuery() string {
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

// FindSchemaQuery returns a query that checks if the default schema exists
func (adapter *PostgresAdapter) FindSchemaQuery() string {
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

// CreateSchemaQuery returns a query that creates a PostgreSQL schema
func (adapter *PostgresAdapter) CreateSchemaQuery() string {
	return fmt.Sprintf("CREATE SCHEMA %s;", adapter.Schema)
}

// DropSchemaQuery returns a query that drops a PostgreSQL schema
func (adapter *PostgresAdapter) DropSchemaQuery() string {
	return fmt.Sprintf("DROP SCHEMA %s CASCADE;", adapter.Schema)
}

// FindTableQuery returns a query that checks if a table exists
func (adapter *PostgresAdapter) FindTableQuery(tableName string) string {
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

// TableDefinitionQuery returns a query with table structure
func (adapter *PostgresAdapter) TableDefinitionQuery(tableName string) string {
	return fmt.Sprintf(`
		WITH dsc AS (
			SELECT
				pgd.objsubid,
				st.schemaname,
				st.relname
			FROM
				pg_catalog.pg_statio_all_tables AS st
				INNER JOIN pg_catalog.pg_description pgd ON (pgd.objoid = st.relid)
		)
		SELECT
			c.column_name ColumnName,
			(
				CASE
					WHEN c.data_type = 'integer' THEN %v
					WHEN c.data_type = 'boolean' THEN %v
					WHEN c.data_type = 'bytea' THEN %v
					WHEN c.data_type = 'text' THEN %v
					WHEN c.udt_name = 'timestamp' THEN %v
					WHEN c.udt_name = 'varchar' THEN %v
					ELSE 0
				END
			) ColumnSQLType,
			(
				CASE
					WHEN is_nullable = 'NO' THEN true
					ELSE false
				END
			) ColumnIsPK,
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

// AlterColumnQuery returns a query for adding a new column to a table
func (adapter *PostgresAdapter) AlterColumnQuery(tableName string, columnName string, sqlColumnType types.SQLColumnType) string {
	sqlType, _ := adapter.TypeMapping(sqlColumnType)
	return fmt.Sprintf("ALTER TABLE %s.%s ADD COLUMN %s %s;", adapter.Schema, tableName, tableName, sqlType)
}

// SelectRowQuery returns a query for selecting row values
func (adapter *PostgresAdapter) SelectRowQuery(tableName string, fields string, indexValue string) string {
	return fmt.Sprintf("SELECT %s FROM %s.%s WHERE height='%s';", fields, adapter.Schema, tableName, indexValue)
}

// SelectLogQuery returns a query for selecting all tables involved in a block trn
func (adapter *PostgresAdapter) SelectLogQuery() string {
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

// InsertLogQuery returns a query to insert a row in log table
func (adapter *PostgresAdapter) InsertLogQuery() string {
	return fmt.Sprintf("INSERT INTO %s._bosmarmot_log (timestamp, registers, height) VALUES (CURRENT_TIMESTAMP, $1, $2) RETURNING id", adapter.Schema)
}

// InsertLogDetailQuery returns a query to insert a row into logdetail table
func (adapter *PostgresAdapter) InsertLogDetailQuery() string {
	return fmt.Sprintf("INSERT INTO %s._bosmarmot_logdet (id, tblname, tblmap, registers) VALUES ($1, $2, $3, $4)", adapter.Schema)
}

// ErrorEquals verify if an error is of a given SQL type
func (adapter *PostgresAdapter) ErrorEquals(err error, sqlErrorType types.SQLErrorType) bool {
	if err, ok := err.(*pq.Error); ok {
		switch sqlErrorType {
		case types.SQLErrorTypeGeneric:
			return true
		case types.SQLErrorTypeDuplicatedColumn:
			return err.Code == "42701"
		case types.SQLErrorTypeDuplicatedTable:
			return err.Code == "42P07"
		case types.SQLErrorTypeDuplicatedSchema:
			return err.Code == "42P06"
		case types.SQLErrorTypeUndefinedTable:
			return err.Code == "42P01"
		case types.SQLErrorTypeUndefinedColumn:
			return err.Code == "42703"
		case types.SQLErrorTypeInvalidType:
			return err.Code == "42704"
		}
	}

	return false
}
