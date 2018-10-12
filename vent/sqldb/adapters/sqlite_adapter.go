package adapters

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/mattn/go-sqlite3"
	"github.com/monax/bosmarmot/vent/logger"
	"github.com/monax/bosmarmot/vent/types"
)

var sqliteDataTypes = map[types.SQLColumnType]string{
	types.SQLColumnTypeBool:      "BOOLEAN",
	types.SQLColumnTypeByteA:     "BLOB",
	types.SQLColumnTypeInt:       "INTEGER",
	types.SQLColumnTypeSerial:    "SERIAL",
	types.SQLColumnTypeText:      "TEXT",
	types.SQLColumnTypeVarchar:   "VARCHAR",
	types.SQLColumnTypeTimeStamp: "TIMESTAMP",
	types.SQLColumnTypeNumeric:   "NUMERIC",
}

// SQLiteAdapter implements DBAdapter for SQLiteDB
type SQLiteAdapter struct {
	Log *logger.Logger
}

// NewSQLiteAdapter constructs a new db adapter
func NewSQLiteAdapter(log *logger.Logger) *SQLiteAdapter {
	return &SQLiteAdapter{
		Log: log,
	}
}

// Open connects to a SQLiteQL database, opens it & create default schema if provided
func (adapter *SQLiteAdapter) Open(dbURL string) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", dbURL)
	if err != nil {
		adapter.Log.Debug("msg", "Error creating database connection", "err", err)
		return nil, err
	}

	return db, nil
}

// TypeMapping convert generic dataTypes to database dependent dataTypes
func (adapter *SQLiteAdapter) TypeMapping(sqlColumnType types.SQLColumnType) (string, error) {
	if sqlDataType, ok := sqliteDataTypes[sqlColumnType]; ok {
		return sqlDataType, nil
	}

	return "", fmt.Errorf("datatype %v not recognized", sqlColumnType)
}

// SecureColumnName return columns between appropriate security containers
func (adapter *SQLiteAdapter) SecureColumnName(columnName string) string {
	return fmt.Sprintf("[%s]", columnName)
}

// CreateTableQuery builds query for creating a new table
func (adapter *SQLiteAdapter) CreateTableQuery(tableName string, columns []types.SQLTableColumn) (string, string) {
	// build query
	columnsDef := ""
	primaryKey := ""
	dictionaryValues := ""
	hasSerial := false

	for i, tableColumn := range columns {
		secureColumn := adapter.SecureColumnName(tableColumn.Name)
		sqlType, _ := adapter.TypeMapping(tableColumn.Type)
		pKey := 0

		if columnsDef != "" {
			columnsDef += ", "
			dictionaryValues += ", "
		}

		if tableColumn.Type == types.SQLColumnTypeSerial {
			// SQLITE AUTOINCREMENT LIMITATION
			columnsDef += fmt.Sprintf("%s %s", secureColumn, "INTEGER PRIMARY KEY AUTOINCREMENT")
			hasSerial = true
		} else {
			columnsDef += fmt.Sprintf("%s %s", secureColumn, sqlType)
		}

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

	query := fmt.Sprintf("CREATE TABLE %s (%s", tableName, columnsDef)
	if primaryKey != "" {
		if hasSerial {
			// SQLITE AUTOINCREMENT LIMITATION
			query += "," + fmt.Sprintf("UNIQUE (%s)", primaryKey)
		} else {
			query += "," + fmt.Sprintf("CONSTRAINT %s_pkey PRIMARY KEY (%s)", tableName, primaryKey)
		}
	}
	query += ");"

	dictionaryQuery := fmt.Sprintf("INSERT INTO %s (%s,%s,%s,%s,%s,%s) VALUES %s;",
		types.SQLDictionaryTableName,
		types.SQLColumnNameTableName, types.SQLColumnNameColumnName,
		types.SQLColumnNameColumnType, types.SQLColumnNameColumnLength,
		types.SQLColumnNamePrimaryKey, types.SQLColumnNameColumnOrder,
		dictionaryValues)

	return query, dictionaryQuery
}

// LastBlockIDQuery returns a query for last inserted blockId in log table
func (adapter *SQLiteAdapter) LastBlockIDQuery() string {
	query := `
		WITH ll AS (
			SELECT MAX(%s) AS %s FROM %s
		)
		SELECT COALESCE(%s, '0') AS %s
			FROM ll LEFT OUTER JOIN %s log ON (ll.%s = log.%s);`

	return fmt.Sprintf(query,
		types.SQLColumnNameId,                        // max
		types.SQLColumnNameId,                        // as
		types.SQLLogTableName,                        // from
		types.SQLColumnNameHeight,                    // coalesce
		types.SQLColumnNameHeight,                    // as
		types.SQLLogTableName,                        // from
		types.SQLColumnNameId, types.SQLColumnNameId) // on
}

// FindTableQuery returns a query that checks if a table exists
func (adapter *SQLiteAdapter) FindTableQuery() string {
	query := "SELECT COUNT(*) found FROM %s WHERE %s = $1;"

	return fmt.Sprintf(query,
		types.SQLDictionaryTableName, // from
		types.SQLColumnNameTableName) // where

}

// TableDefinitionQuery returns a query with table structure
func (adapter *SQLiteAdapter) TableDefinitionQuery() string {
	query := `
		SELECT
			%s,%s,%s,%s
		FROM
			%s
		WHERE
			%s = $1
		ORDER BY
			%s;`

	return fmt.Sprintf(query,
		types.SQLColumnNameColumnName, types.SQLColumnNameColumnType, // select
		types.SQLColumnNameColumnLength, types.SQLColumnNamePrimaryKey, // select
		types.SQLDictionaryTableName,   // from
		types.SQLColumnNameTableName,   // where
		types.SQLColumnNameColumnOrder) // order by

}

// AlterColumnQuery returns a query for adding a new column to a table
func (adapter *SQLiteAdapter) AlterColumnQuery(tableName, columnName string, sqlColumnType types.SQLColumnType, length, order int) (string, string) {
	sqlType, _ := adapter.TypeMapping(sqlColumnType)
	if length > 0 {
		sqlType = fmt.Sprintf("%s(%d)", sqlType, length)
	}

	query := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s;",
		tableName,
		adapter.SecureColumnName(columnName),
		sqlType)

	dictionaryQuery := fmt.Sprintf(`
		INSERT INTO %s (%s,%s,%s,%s,%s,%s)
		VALUES ('%s','%s',%d,%d,%d,%d);`,

		types.SQLDictionaryTableName,

		types.SQLColumnNameTableName, types.SQLColumnNameColumnName,
		types.SQLColumnNameColumnType, types.SQLColumnNameColumnLength,
		types.SQLColumnNamePrimaryKey, types.SQLColumnNameColumnOrder,

		tableName, columnName, sqlColumnType, length, 0, order)

	return query, dictionaryQuery
}

// SelectRowQuery returns a query for selecting row values
func (adapter *SQLiteAdapter) SelectRowQuery(tableName, fields, indexValue string) string {
	return fmt.Sprintf("SELECT %s FROM %s WHERE %s = '%s';", fields, tableName, types.SQLColumnNameHeight, indexValue)
}

// SelectLogQuery returns a query for selecting all tables involved in a block trn
func (adapter *SQLiteAdapter) SelectLogQuery() string {
	query := `
		SELECT DISTINCT %s,%s FROM %s l WHERE %s = $1 AND %s = $2;`

	return fmt.Sprintf(query,
		types.SQLColumnNameTableName, types.SQLColumnNameEventName, // select
		types.SQLLogTableName,                                     // from
		types.SQLColumnNameEventFilter, types.SQLColumnNameHeight) // where
}

// InsertLogQuery returns a query to insert a row in log table
func (adapter *SQLiteAdapter) InsertLogQuery() string {
	query := `
		INSERT INTO %s (%s,%s,%s,%s,%s,%s)
		VALUES (CURRENT_TIMESTAMP, $1, $2, $3, $4, $5);`

	return fmt.Sprintf(query,
		types.SQLLogTableName,                                                                   // insert
		types.SQLColumnNameTimeStamp, types.SQLColumnNameRowCount, types.SQLColumnNameTableName, // fields
		types.SQLColumnNameEventName, types.SQLColumnNameEventFilter, types.SQLColumnNameHeight) // fields
}

// ErrorEquals verify if an error is of a given SQL type
func (adapter *SQLiteAdapter) ErrorEquals(err error, sqlErrorType types.SQLErrorType) bool {
	if err, ok := err.(sqlite3.Error); ok {
		errDescription := err.Error()

		switch sqlErrorType {
		case types.SQLErrorTypeGeneric:
			return true
		case types.SQLErrorTypeDuplicatedColumn:
			return err.Code == 1 && strings.Contains(errDescription, "duplicate column")
		case types.SQLErrorTypeDuplicatedTable:
			return err.Code == 1 && strings.Contains(errDescription, "table") && strings.Contains(errDescription, "already exists")
		case types.SQLErrorTypeUndefinedTable:
			return err.Code == 1 && strings.Contains(errDescription, "no such table")
		case types.SQLErrorTypeUndefinedColumn:
			return err.Code == 1 && strings.Contains(errDescription, "table") && strings.Contains(errDescription, "has no column named")
		case types.SQLErrorTypeInvalidType:
			// NOT SUPPORTED
			return false
		}
	}

	return false
}

func (adapter *SQLiteAdapter) UpsertQuery(table types.SQLTable, row types.EventDataRow) (string, string, []interface{}, error) {

	pointers := make([]interface{}, 0)
	null := sql.NullString{Valid: false}
	columns := ""
	insValues := ""
	updValues := ""
	pkColumns := ""
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

		if tableColumn.Primary {
			// ON CONFLICT (....values....)
			if pkColumns != "" {
				pkColumns += ", "
			}
			pkColumns += secureColumn
		}

	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) ", table.Name, columns, insValues)

	if pkColumns != "" {
		if updValues != "" {
			query += fmt.Sprintf("ON CONFLICT (%s) DO UPDATE SET %s", pkColumns, updValues)
		} else {
			query += fmt.Sprintf("ON CONFLICT (%s) DO NOTHING", pkColumns)
		}
	}
	query += ";"

	return query, values, pointers, nil
}
