package sqldb

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/monax/bosmarmot/vent/logger"
	"github.com/monax/bosmarmot/vent/sqldb/adapters"
	"github.com/monax/bosmarmot/vent/types"
)

// SQLDB implements the access to a sql database
type SQLDB struct {
	DB        *sql.DB
	DBAdapter adapters.DBAdapter
	Schema    string
	Log       *logger.Logger
}

// NewSQLDB delegates work to a specific database adapter implementation,
// opens database connection and create log tables
func NewSQLDB(dbAdapter, dbURL, schema string, log *logger.Logger) (*SQLDB, error) {
	db := &SQLDB{
		Schema: schema,
		Log:    log,
	}

	url := dbURL

	switch dbAdapter {
	case types.PostgresDB:
		db.DBAdapter = adapters.NewPostgresAdapter(safe(schema), log)
	case types.SQLiteDB:
		db.DBAdapter = adapters.NewSQLiteAdapter(log)
	default:
		return nil, errors.New("invalid database adapter")
	}

	dbc, err := db.DBAdapter.Open(url)
	if err != nil {
		db.Log.Debug("msg", "Error opening database connection", "err", err)
		return nil, err
	}
	db.DB = dbc

	if err = db.Ping(); err != nil {
		db.Log.Debug("msg", "Error database not available", "err", err)
		return nil, err
	}

	db.Log.Info("msg", "Initializing DB")
	eventTables := db.getSysTablesDefinition()

	// IMPORTANT: DO NOT CHANGE TABLE CREATION ORDER (1)
	table := eventTables[types.SQLDictionaryTableName]
	if err = db.createTable(table); err != nil {
		if !db.DBAdapter.ErrorEquals(err, types.SQLErrorTypeDuplicatedTable) {
			db.Log.Debug("msg", "Error Initializing DB", "err", err)
			return nil, err
		}
	}

	// IMPORTANT: DO NOT CHANGE TABLE CREATION ORDER (2)
	table = eventTables[types.SQLLogTableName]
	if err = db.createTable(table); err != nil {
		if !db.DBAdapter.ErrorEquals(err, types.SQLErrorTypeDuplicatedTable) {
			db.Log.Debug("msg", "Error Initializing DB", "err", err)
			return nil, err
		}
	}

	return db, nil
}

// Close database connection
func (db *SQLDB) Close() {
	if err := db.DB.Close(); err != nil {
		db.Log.Error("msg", "Error closing database", "err", err)
	}
}

// Ping database
func (db *SQLDB) Ping() error {
	if err := db.DB.Ping(); err != nil {
		db.Log.Debug("msg", "Error database not available", "err", err)
		return err
	}

	return nil
}

// GetLastBlockID returns last inserted blockId for a given events filter from log table
func (db *SQLDB) GetLastBlockID(eventFilter string) (string, error) {
	query := clean(db.DBAdapter.LastBlockIDQuery())
	id := ""

	db.Log.Debug("msg", "MAX ID", "query", query)

	if err := db.DB.QueryRow(query, eventFilter).Scan(&id); err != nil {
		db.Log.Debug("msg", "Error selecting last block id", "err", err)
		return "", err
	}

	return id, nil
}

// SynchronizeDB synchronize db tables structures from given tables specifications
func (db *SQLDB) SynchronizeDB(eventTables types.EventTables) error {
	db.Log.Info("msg", "Synchronizing DB")

	for _, table := range eventTables {
		found, err := db.findTable(table.Name)
		if err != nil {
			return err
		}

		if found {
			err = db.alterTable(table)
		} else {
			err = db.createTable(table)
		}
		if err != nil {
			return err
		}
	}

	return nil
}

// SetBlock inserts or updates multiple rows and stores log info in SQL tables
func (db *SQLDB) SetBlock(eventTables types.EventTables, eventData types.EventData) error {
	db.Log.Debug("msg", "Set Block..........")

	var safeTable string
	var logStmt *sql.Stmt
	var err error

	// begin tx
	tx, err := db.DB.Begin()
	if err != nil {
		db.Log.Debug("msg", "Error beginning transaction", "err", err)
		return err
	}
	defer tx.Rollback()

	// prepare log statement
	logQuery := clean(db.DBAdapter.InsertLogQuery())
	logStmt, err = tx.Prepare(logQuery)
	if err != nil {
		db.Log.Debug("msg", "Error preparing log stmt", "err", err)
		return err
	}

loop:
	// for each table in the block
	for eventName, table := range eventTables {
		safeTable = safe(table.Name)

		// insert in log table
		dataRows := eventData.Tables[table.Name]
		length := len(dataRows)
		db.Log.Debug("msg", "INSERT LOG", "query", logQuery, "value", fmt.Sprintf("rows = %d tableName = %s eventName = %s filter = %s block = %s", length, safeTable, eventName, table.Filter, eventData.Block))
		_, err = logStmt.Exec(length, safeTable, eventName, table.Filter, eventData.Block)
		if err != nil {
			db.Log.Debug("msg", "Error inserting into log", "err", err)
			return err
		}

		// for Each Row
		for _, row := range dataRows {
			query, values, pointers, errQuery := db.DBAdapter.UpsertQuery(table, row)
			query = clean(query)

			if errQuery != nil {
				db.Log.Debug("msg", "Error building query", "err", errQuery, "value", fmt.Sprintf("%v %v", table, row))
				return err
			}

			// upsert row data
			db.Log.Debug("msg", "UPSERT", "query", query, "value", values)
			_, err = tx.Exec(query, pointers...)
			if err != nil {
				db.Log.Debug("msg", "Error upserting row", "err", err, "value", values)
				// exits from all loops -> continue in close log stmt
				break loop
			}
		}
	}

	// close log statement
	if err == nil {
		if err = logStmt.Close(); err != nil {
			db.Log.Debug("msg", "Error closing log stmt", "err", err)
		}
	}

	// error handling
	if err != nil {
		// rollback error
		if errRb := tx.Rollback(); errRb != nil {
			db.Log.Debug("msg", "Error on rollback", "err", errRb)
			return errRb
		}

		if db.DBAdapter.ErrorEquals(err, types.SQLErrorTypeGeneric) {
			// table does not exists
			if db.DBAdapter.ErrorEquals(err, types.SQLErrorTypeUndefinedTable) {
				db.Log.Warn("msg", "Table not found", "value", safeTable)
				if err = db.SynchronizeDB(eventTables); err != nil {
					return err
				}
				return db.SetBlock(eventTables, eventData)
			}

			// columns do not match
			if db.DBAdapter.ErrorEquals(err, types.SQLErrorTypeUndefinedColumn) {
				db.Log.Warn("msg", "Column not found", "value", safeTable)
				if err = db.SynchronizeDB(eventTables); err != nil {
					return err
				}
				return db.SetBlock(eventTables, eventData)
			}
			return err
		}

		return err
	}

	db.Log.Debug("msg", "COMMIT")

	if err := tx.Commit(); err != nil {
		db.Log.Debug("msg", "Error on commit", "err", err)
		return err
	}

	return nil
}

// GetBlock returns all tables structures and row data for given block & events filter
func (db *SQLDB) GetBlock(eventFilter, block string) (types.EventData, error) {
	var data types.EventData
	data.Block = block
	data.Tables = make(map[string]types.EventDataTable)

	// get all table structures involved in the block
	tables, err := db.getBlockTables(eventFilter, block)
	if err != nil {
		return data, err
	}

	query := ""

	// for each table
	for _, table := range tables {
		// get query for table
		query, err = db.getSelectQuery(table, block)
		if err != nil {
			db.Log.Debug("msg", "Error building table query", "err", err)
			return data, err
		}
		query = clean(query)
		db.Log.Debug("msg", "Query table data", "query", query)
		rows, err := db.DB.Query(query)
		if err != nil {
			db.Log.Debug("msg", "Error querying table data", "err", err)
			return data, err
		}
		defer rows.Close()

		cols, err := rows.Columns()
		if err != nil {
			db.Log.Debug("msg", "Error getting row columns", "err", err)
			return data, err
		}

		// builds pointers
		length := len(cols)
		pointers := make([]interface{}, length)
		containers := make([]sql.NullString, length)

		for i := range pointers {
			pointers[i] = &containers[i]
		}

		// for each row in table
		var dataRows []types.EventDataRow

		for rows.Next() {
			row := make(map[string]interface{})

			if err = rows.Scan(pointers...); err != nil {
				db.Log.Debug("msg", "Error scanning data", "err", err)
				return data, err
			}
			db.Log.Debug("msg", "Query resultset", "value", fmt.Sprintf("%+v", containers))

			// for each column in row
			for i, col := range cols {
				// add value if not null
				if containers[i].Valid {
					row[col] = containers[i].String
				}
			}

			dataRows = append(dataRows, row)
		}

		if err = rows.Err(); err != nil {
			db.Log.Debug("msg", "Error during rows iteration", "err", err)
			return data, err
		}

		data.Tables[table.Name] = dataRows
	}

	return data, nil
}
