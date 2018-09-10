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

	return db, err
}

// TypeMapping convert generic dataTypes to database dependent dataTypes
func (adapter *SQLiteAdapter) TypeMapping(sqlColumnType types.SQLColumnType) (string, error) {
	if sqlDataType, ok := sqliteDataTypes[sqlColumnType]; ok {
		return sqlDataType, nil
	}
	err := fmt.Errorf("datatype %v not recognized", sqlColumnType)
	return "", err
}

//SecureColumnName return columns between appropriate security containers
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
		secureColumn := fmt.Sprintf("%s", adapter.SecureColumnName(tableColumn.Name))
		sqlType, _ := adapter.TypeMapping(tableColumn.Type)
		pKey := 0

		if columnsDef != "" {
			columnsDef += ", "
			dictionaryValues += ", "
		}

		if tableColumn.Type == types.SQLColumnTypeSerial {
			//SQLITE AUTOINCREMENT LIMITATION
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
			primaryKey += fmt.Sprintf("%s", secureColumn)
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
			//SQLITE AUTOINCREMENT LIMITATION
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

// UpsertQuery builds a query for row upsert
func (adapter *SQLiteAdapter) UpsertQuery(table types.SQLTable) types.UpsertQuery {
	columns := ""
	insValues := ""
	updValues := ""
	pkColumns := ""
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
		secureColumn := fmt.Sprintf("%s", adapter.SecureColumnName(tableColumn.Name))
		cKey = 0
		i++

		// INSERT INTO TABLE (*columns).........
		if columns != "" {
			columns += ", "
			insValues += ", "
		}
		columns += secureColumn
		insValues += "$" + fmt.Sprintf("%d", i)

		if !tableColumn.Primary {
			cKey = cols + nKeys
			nKeys++

			// INSERT........... ON CONFLICT......DO UPDATE (*updValues)
			if updValues != "" {
				updValues += ", "
			}
			updValues += secureColumn + " = $" + fmt.Sprintf("%d", cKey+1)
		} else {
			//ON CONFLICT (....values....)
			if pkColumns != "" {
				pkColumns += ", "
			}
			pkColumns += secureColumn
		}

		upsertQuery.Columns[tableColumn.Name] = types.UpsertColumn{
			IsNumeric:   tableColumn.Type.IsNumeric(),
			InsPosition: i - 1,
			UpdPosition: cKey,
		}
	}
	upsertQuery.Length = cols + nKeys

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) ", table.Name, columns, insValues)

	if pkColumns != "" {
		if nKeys != 0 {
			query += fmt.Sprintf("ON CONFLICT (%s) DO UPDATE SET ", pkColumns)
			query += updValues
		} else {
			query += fmt.Sprintf("ON CONFLICT (%s) DO NOTHING", pkColumns)
		}
	}
	query += ";"

	upsertQuery.Query = query
	return upsertQuery
}

// LastBlockIDQuery returns a query for last inserted blockId in log table
func (adapter *SQLiteAdapter) LastBlockIDQuery() string {
	query := `
		WITH ll AS (
			SELECT MAX(%s) AS %s FROM %s WHERE %s = $1 
		)
		SELECT COALESCE(%s, '0') AS %s 
			FROM ll LEFT OUTER JOIN %s log ON (ll.%s = log.%s);`

	return fmt.Sprintf(query,
		types.SQLColumnNameId,          //max
		types.SQLColumnNameId,          //as
		types.SQLLogTableName,          //from
		types.SQLColumnNameEventFilter, //where

		types.SQLColumnNameHeight,                    //coalesce
		types.SQLColumnNameHeight,                    //as
		types.SQLLogTableName,                        //from
		types.SQLColumnNameId, types.SQLColumnNameId) //on

}

// FindTableQuery returns a query that checks if a table exists
func (adapter *SQLiteAdapter) FindTableQuery() string {
	query := "SELECT COUNT(*) found FROM %s WHERE %s = $1;"

	return fmt.Sprintf(query,
		types.SQLDictionaryTableName, //from
		types.SQLColumnNameTableName) //where

}

// TableDefinitionQuery returns a query with table structure
func (adapter *SQLiteAdapter) TableDefinitionQuery() string {
	query := `
		SELECT 
			%s,%s,%s,%s 
		FROM 
			%s 
		WHERE 
			%s=$1 
		ORDER BY
			%s;`

	return fmt.Sprintf(query,
		types.SQLColumnNameColumnName, types.SQLColumnNameColumnType, //select
		types.SQLColumnNameColumnLength, types.SQLColumnNamePrimaryKey, //select
		types.SQLDictionaryTableName,   //from
		types.SQLColumnNameTableName,   //where
		types.SQLColumnNameColumnOrder) //order by

}

// AlterColumnQuery returns a query for adding a new column to a table
func (adapter *SQLiteAdapter) AlterColumnQuery(tableName string, columnName string, sqlColumnType types.SQLColumnType, length, order int) (string, string) {
	sqlType, _ := adapter.TypeMapping(sqlColumnType)
	if length > 0 {
		sqlType = fmt.Sprintf("%s(%d)", sqlType, length)
	}

	query := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s;",
		tableName,
		fmt.Sprintf("%s", adapter.SecureColumnName(columnName)),
		sqlType)

	dictionaryQuery := fmt.Sprintf(
		`INSERT INTO %s 
					(%s,%s,%s,%s,%s,%s) 
				VALUES 
					('%s','%s',%d,%d,%d,%d);`,

		types.SQLDictionaryTableName,

		types.SQLColumnNameTableName, types.SQLColumnNameColumnName,
		types.SQLColumnNameColumnType, types.SQLColumnNameColumnLength,
		types.SQLColumnNamePrimaryKey, types.SQLColumnNameColumnOrder,

		tableName, columnName,
		sqlColumnType, length,
		0, order)

	return query, dictionaryQuery
}

// SelectRowQuery returns a query for selecting row values
func (adapter *SQLiteAdapter) SelectRowQuery(tableName string, fields string, indexValue string) string {
	return fmt.Sprintf("SELECT %s FROM %s WHERE %s='%s';", fields, tableName, types.SQLColumnNameHeight, indexValue)
}

// SelectLogQuery returns a query for selecting all tables involved in a block trn
func (adapter *SQLiteAdapter) SelectLogQuery() string {
	query := `
		SELECT DISTINCT %s,%s FROM %s l  WHERE %s = $1 AND %s = $2;`

	return fmt.Sprintf(query,
		types.SQLColumnNameTableName, types.SQLColumnNameEventName, // select
		types.SQLLogTableName,                                     //from
		types.SQLColumnNameEventFilter, types.SQLColumnNameHeight) //where
}

// InsertLogQuery returns a query to insert a row in log table
func (adapter *SQLiteAdapter) InsertLogQuery() string {
	query := `
		INSERT INTO %s (%s,%s,%s,%s,%s,%s)
		VALUES (CURRENT_TIMESTAMP, $1, $2, $3, $4, $5);`

	return fmt.Sprintf(query,
		types.SQLLogTableName,                                                                   //insert
		types.SQLColumnNameTimeStamp, types.SQLColumnNameRowCount, types.SQLColumnNameTableName, //fields
		types.SQLColumnNameEventName, types.SQLColumnNameEventFilter, types.SQLColumnNameHeight) //fields
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
			//NOT SUPPORTED
			return false
		}
	}

	return false
}
