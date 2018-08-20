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
	DBAdapter DBAdapter
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

	switch dbAdapter {
	case "postgres":
		db.DBAdapter = adapters.NewPostgresAdapter(safe(schema), log)
	default:
		return nil, errors.New("Invalid database adapter")
	}

	dbc, err := db.DBAdapter.Open(dbURL)
	if err != nil {
		db.Log.Debug("msg", "Error opening database connection", "err", err)
		return nil, err
	}
	db.DB = dbc

	if err = db.Ping(); err != nil {
		db.Log.Debug("msg", "Error database not available", "err", err)
		return nil, err
	}

	var found bool
	found, err = db.findDefaultSchema()
	if err != nil {
		return nil, err
	}

	if !found {
		if err = db.createDefaultSchema(); err != nil {
			return nil, err
		}
	}

	// create _bosmarmot_log
	if err = db.SynchronizeDB(db.getLogTableDef()); err != nil {
		return nil, err
	}

	return db, err
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

// GetLastBlockID returns last inserted blockId from log table
func (db *SQLDB) GetLastBlockID() (string, error) {
	query := db.DBAdapter.LastBlockIDQuery()
	id := ""

	db.Log.Debug("msg", "MAX ID", "query", clean(query))

	if err := db.DB.QueryRow(query).Scan(&id); err != nil {
		db.Log.Debug("msg", "Error selecting last block id", "err", err)
		return "", err
	}

	return id, nil
}

// DestroySchema deletes the default schema
func (db *SQLDB) DestroySchema() error {
	db.Log.Info("msg", "Dropping schema")
	found, err := db.findDefaultSchema()

	if err != nil {
		return err
	}

	if found {
		query := db.DBAdapter.DropSchemaQuery()

		db.Log.Info("msg", "Drop schema", "query", query)

		if _, err := db.DB.Exec(query); err != nil {
			db.Log.Debug("msg", "Error dropping schema", "err", err)
			return err
		}
	}

	return nil
}

// SynchronizeDB synchronize config structures with SQL database table structures
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
	var pointers []interface{}
	var value string
	var safeTable string
	var logStmt *sql.Stmt

	// begin tx
	tx, err := db.DB.Begin()
	if err != nil {
		db.Log.Debug("msg", "Error beginning transaction", "err", err)
		return err
	}
	defer tx.Rollback()

	// insert into log tables
	id := 0
	length := len(eventTables)
	query := db.DBAdapter.InsertLogQuery()

	db.Log.Debug("msg", "INSERT LOG", "query", clean(query), "value", fmt.Sprintf("%d %s", length, eventData.Block))
	err = tx.QueryRow(query, length, eventData.Block).Scan(&id)
	if err != nil {
		db.Log.Debug("msg", "Error inserting into _bosmarmot_log", "err", err)
		return err
	}

	// prepare log detail statement
	logQuery := db.DBAdapter.InsertLogDetailQuery()
	logStmt, err = tx.Prepare(logQuery)
	if err != nil {
		db.Log.Debug("msg", "Error preparing log stmt", "err", err)
		return err
	}

loop:
	// for each table in the block
	for tblMap, table := range eventTables {
		safeTable = safe(table.Name)

		// insert in logdet table
		dataRows := eventData.Tables[table.Name]
		length = len(dataRows)
		db.Log.Debug("msg", "INSERT LOGDET", "query", logQuery, "value", fmt.Sprintf("%d %s %s %d", id, safeTable, tblMap, length))
		_, err = logStmt.Exec(id, safeTable, tblMap, length)
		if err != nil {
			db.Log.Debug("msg", "Error inserting into logdet", "err", err)
			return err
		}

		// get table upsert query
		uQuery := db.DBAdapter.UpsertQuery(table)

		// for Each Row
		for _, row := range dataRows {
			// get parameter interface
			pointers, value, err = getUpsertParams(uQuery, row)
			if err != nil {
				db.Log.Debug("msg", "Error building parameters", "err", err, "value", fmt.Sprintf("%v", row))
				return err
			}

			// upsert row data
			db.Log.Debug("msg", "UPSERT", "query", clean(uQuery.Query), "value", value)
			_, err = tx.Exec(uQuery.Query, pointers...)
			if err != nil {
				db.Log.Debug("msg", "Error Upserting", "err", err)
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

	//------------------------error handling----------------------
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

			db.Log.Debug("msg", "Error upserting row", "err", err)
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

// GetBlock returns a table's structure and row data for given block id
func (db *SQLDB) GetBlock(block string) (types.EventData, error) {
	var data types.EventData
	data.Block = block
	data.Tables = make(map[string]types.EventDataTable)

	// get all table structures involved in the block
	tables, err := db.getBlockTables(block)
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

		db.Log.Debug("msg", "Query table data", "query", clean(query))
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
			row := make(map[string]string)

			err = rows.Scan(pointers...)
			if err != nil {
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

		data.Tables[table.Name] = dataRows
	}

	return data, nil
}
