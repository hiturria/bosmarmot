package adapters

import (
	"database/sql"
	"fmt"

	"github.com/lib/pq"
	"github.com/monax/bosmarmot/vent/logger"
	"github.com/monax/bosmarmot/vent/types"
)

var pgDataTypes = map[types.SQLColumnType]string{
	types.SQLColumnTypeBool:      "BOOLEAN",
	types.SQLColumnTypeByteA:     "BYTEA",
	types.SQLColumnTypeInt:       "INTEGER",
	types.SQLColumnTypeSerial:    "SERIAL",
	types.SQLColumnTypeText:      "TEXT",
	types.SQLColumnTypeVarchar:   "VARCHAR",
	types.SQLColumnTypeTimeStamp: "TIMESTAMP",
	types.SQLColumnTypeNumeric:   "NUMERIC",
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

// Open connects to a PostgreSQL database, opens it & create default schema if provided
func (adapter *PostgresAdapter) Open(dbURL string) (*sql.DB, error) {
	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		adapter.Log.Debug("msg", "Error creating database connection", "err", err)
		return nil, err
	}

	// if there is a supplied Schema
	if adapter.Schema != "" {
		if err = db.Ping(); err != nil {
			adapter.Log.Debug("msg", "Error opening database connection", "err", err)
			return nil, err
		}

		var found bool

		query := fmt.Sprintf(`SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace n WHERE n.nspname = '%s');`, adapter.Schema)
		adapter.Log.Debug("msg", "FIND SCHEMA", "query", query)

		if err := db.QueryRow(query).Scan(&found); err == nil {
			if !found {
				adapter.Log.Warn("msg", "Schema not found")
			}
			adapter.Log.Info("msg", "Creating schema")

			query = fmt.Sprintf("CREATE SCHEMA %s;", adapter.Schema)
			adapter.Log.Debug("msg", "CREATE SCHEMA", "query", query)

			if _, err = db.Exec(query); err != nil {
				if adapter.ErrorEquals(err, types.SQLErrorTypeDuplicatedSchema) {
					adapter.Log.Warn("msg", "Duplicated schema")
					return db, nil
				}
			}
		} else {
			adapter.Log.Debug("msg", "Error searching schema", "err", err)
			return nil, err
		}
	}

	return db, err
}

// TypeMapping convert generic dataTypes to database dependent dataTypes
func (adapter *PostgresAdapter) TypeMapping(sqlColumnType types.SQLColumnType) (string, error) {
	if sqlDataType, ok := pgDataTypes[sqlColumnType]; ok {
		return sqlDataType, nil
	}

	return "", fmt.Errorf("datatype %v not recognized", sqlColumnType)
}

// SecureColumnName return columns between appropriate security containers
func (adapter *PostgresAdapter) SecureColumnName(columnName string) string {
	return `"` + columnName + `"`
}

// CreateTableQuery builds query for creating a new table
func (adapter *PostgresAdapter) CreateTableQuery(tableName string, columns []types.SQLTableColumn) (string, string) {
	// build query
	columnsDef := ""
	primaryKey := ""
	dictionaryValues := ""

	for i, tableColumn := range columns {
		secureColumn := adapter.SecureColumnName(tableColumn.Name)
		sqlType, _ := adapter.TypeMapping(tableColumn.Type)
		pKey := 0

		if columnsDef != "" {
			columnsDef += ", "
			dictionaryValues += ", "
		}

		columnsDef += fmt.Sprintf("%s %s", secureColumn, sqlType)

		if tableColumn.Length > 0 {
			columnsDef += fmt.Sprintf("(%v)", tableColumn.Length)
		}

		if tableColumn.Primary {
			pKey = 1
			columnsDef += " NOT NULL"
			if primaryKey != "" {
				primaryKey += ", "
			}
			primaryKey += secureColumn
		}

		dictionaryValues += fmt.Sprintf("('%s','%s',%d,%d,%d,%d)",
			tableName,
			tableColumn.Name,
			tableColumn.Type,
			tableColumn.Length,
			pKey,
			i)
	}

	query := fmt.Sprintf("CREATE TABLE %s.%s (%s", adapter.Schema, tableName, columnsDef)
	if primaryKey != "" {
		query += "," + fmt.Sprintf("CONSTRAINT %s_pkey PRIMARY KEY (%s)", tableName, primaryKey)
	}
	query += ");"

	dictionaryQuery := fmt.Sprintf("INSERT INTO %s.%s (%s,%s,%s,%s,%s,%s) VALUES %s;",
		adapter.Schema, types.SQLDictionaryTableName,
		types.SQLColumnNameTableName, types.SQLColumnNameColumnName,
		types.SQLColumnNameColumnType, types.SQLColumnNameColumnLength,
		types.SQLColumnNamePrimaryKey, types.SQLColumnNameColumnOrder,
		dictionaryValues)

	return query, dictionaryQuery
}

// LastBlockIDQuery returns a query for last inserted blockId in log table
func (adapter *PostgresAdapter) LastBlockIDQuery() string {
	query := `
		WITH ll AS (
			SELECT MAX(%s) AS %s FROM %s.%s
		)
		SELECT COALESCE(%s, '0') AS %s
			FROM ll LEFT OUTER JOIN %s.%s log ON (ll.%s = log.%s);`

	return fmt.Sprintf(query,
		types.SQLColumnNameId,                 // max
		types.SQLColumnNameId,                 // as
		adapter.Schema, types.SQLLogTableName, // from
		types.SQLColumnNameHeight,             // coalesce
		types.SQLColumnNameHeight,             // as
		adapter.Schema, types.SQLLogTableName, // from
		types.SQLColumnNameId, types.SQLColumnNameId) // on

}

// FindTableQuery returns a query that checks if a table exists
func (adapter *PostgresAdapter) FindTableQuery() string {
	query := "SELECT COUNT(*) found FROM %s.%s WHERE %s = $1;"

	return fmt.Sprintf(query,
		adapter.Schema, types.SQLDictionaryTableName, // from
		types.SQLColumnNameTableName) // where
}

// TableDefinitionQuery returns a query with table structure
func (adapter *PostgresAdapter) TableDefinitionQuery() string {
	query := `
		SELECT
			%s,%s,%s,%s
		FROM
			%s.%s
		WHERE
			%s = $1
		ORDER BY
			%s;`

	return fmt.Sprintf(query,
		types.SQLColumnNameColumnName, types.SQLColumnNameColumnType, // select
		types.SQLColumnNameColumnLength, types.SQLColumnNamePrimaryKey, // select
		adapter.Schema, types.SQLDictionaryTableName, // from
		types.SQLColumnNameTableName,   // where
		types.SQLColumnNameColumnOrder) // order by

}

// AlterColumnQuery returns a query for adding a new column to a table
func (adapter *PostgresAdapter) AlterColumnQuery(tableName, columnName string, sqlColumnType types.SQLColumnType, length, order int) (string, string) {
	sqlType, _ := adapter.TypeMapping(sqlColumnType)
	if length > 0 {
		sqlType = fmt.Sprintf("%s(%d)", sqlType, length)
	}

	query := fmt.Sprintf("ALTER TABLE %s.%s ADD COLUMN %s %s;",
		adapter.Schema,
		tableName,
		adapter.SecureColumnName(columnName),
		sqlType)

	dictionaryQuery := fmt.Sprintf(`
		INSERT INTO %s.%s (%s,%s,%s,%s,%s,%s)
		VALUES ('%s','%s',%d,%d,%d,%d);`,

		adapter.Schema, types.SQLDictionaryTableName,

		types.SQLColumnNameTableName, types.SQLColumnNameColumnName,
		types.SQLColumnNameColumnType, types.SQLColumnNameColumnLength,
		types.SQLColumnNamePrimaryKey, types.SQLColumnNameColumnOrder,

		tableName, columnName, sqlColumnType, length, 0, order)

	return query, dictionaryQuery
}

// SelectRowQuery returns a query for selecting row values
func (adapter *PostgresAdapter) SelectRowQuery(tableName, fields, indexValue string) string {
	return fmt.Sprintf("SELECT %s FROM %s.%s WHERE %s = '%s';", fields, adapter.Schema, tableName, types.SQLColumnNameHeight, indexValue)
}

// SelectLogQuery returns a query for selecting all tables involved in a block trn
func (adapter *PostgresAdapter) SelectLogQuery() string {
	query := `
		SELECT DISTINCT %s,%s FROM %s.%s l WHERE %s = $1 AND %s = $2;`

	return fmt.Sprintf(query,
		types.SQLColumnNameTableName, types.SQLColumnNameEventName, // select
		adapter.Schema, types.SQLLogTableName, // from
		types.SQLColumnNameEventFilter, types.SQLColumnNameHeight) // where
}

// InsertLogQuery returns a query to insert a row in log table
func (adapter *PostgresAdapter) InsertLogQuery() string {
	query := `
		INSERT INTO %s.%s (%s,%s,%s,%s,%s,%s)
		VALUES (CURRENT_TIMESTAMP, $1, $2, $3, $4, $5);`

	return fmt.Sprintf(query,
		adapter.Schema, types.SQLLogTableName, // insert
		types.SQLColumnNameTimeStamp, types.SQLColumnNameRowCount, types.SQLColumnNameTableName, // fields
		types.SQLColumnNameEventName, types.SQLColumnNameEventFilter, types.SQLColumnNameHeight) // fields
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

func (adapter *PostgresAdapter) UpsertQuery(table types.SQLTable, row types.EventDataRow) (string, string, []interface{}, error) {

	pointers := make([]interface{}, 0)
	null := sql.NullString{Valid: false}

	columns := ""
	insValues := ""
	updValues := ""
	values := ""

	i := 0

	// for each column in table
	for _, tableColumn := range table.Columns {
		secureColumn := adapter.SecureColumnName(tableColumn.Name)

		i++

		// INSERT INTO TABLE (*columns).........
		if columns != "" {
			columns += ", "
			insValues += ", "
			values += ", "
		}
		columns += secureColumn
		insValues += "$" + fmt.Sprintf("%d", i)

		//find data for column
		if value, ok := row[tableColumn.Name]; ok {
			// column found (not null)
			// load values
			pointers = append(pointers, &value)
			values += fmt.Sprint(value)

			if !tableColumn.Primary {
				// column is no PK
				// add to update list
				// INSERT........... ON CONFLICT......DO UPDATE (*updValues)
				if updValues != "" {
					updValues += ", "
				}
				updValues += secureColumn + " = $" + fmt.Sprintf("%d", i)
			}
		} else if tableColumn.Primary {
			// column NOT found (is null) and is PK
			return "", "", nil, fmt.Errorf("error null primary key for column %s", secureColumn)
		} else {
			// column NOT found (is null) and is NOT PK
			pointers = append(pointers, &null)
			values += "null"
		}
	}

	query := fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES (%s) ", adapter.Schema, table.Name, columns, insValues)

	if updValues != "" {
		query += fmt.Sprintf("ON CONFLICT ON CONSTRAINT %s_pkey DO UPDATE SET %s", table.Name, updValues)
	} else {
		query += fmt.Sprintf("ON CONFLICT ON CONSTRAINT %s_pkey DO NOTHING", table.Name)
	}
	query += ";"

	return query, values, pointers, nil
}
