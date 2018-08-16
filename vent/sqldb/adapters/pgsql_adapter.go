package adapters

import (
	"database/sql"
	"fmt"

	"github.com/lib/pq"
	"github.com/monax/bosmarmot/vent/logger"
	"github.com/monax/bosmarmot/vent/types"
)

//SQLDB implements DBAdapter
type SQLDB struct {
	DB     *sql.DB
	Log    *logger.Logger
	Schema string
	DBURL  string
}

// NewSQLDB connects to a SQL database and creates default schema and _bosmarmot_log if missing
func NewSQLDB(dbURL string, schema string, l *logger.Logger) DBAdapter {
	return &SQLDB{
		Log:    l,
		Schema: schema,
		DBURL:  dbURL,
	}
}

// Open connects to a SQL database and creates default schema and _bosmarmot_log if missing
func (db *SQLDB) Open() error {
	db.Log.Info("msg", "Connecting to database", "value", db.DBURL)

	dbc, err := sql.Open("postgres", db.DBURL)
	if err != nil {
		db.Log.Error("msg", "Error opening database connection", "err", err)
		return err
	}
	db.DB = dbc

	if err = db.Ping(); err != nil {
		db.Log.Error("msg", "Error database not available", "err", err)
		return err
	}

	var found bool
	found, err = db.findDefaultSchema()
	if err != nil {
		return err
	}

	if !found {
		if err = db.createDefaultSchema(); err != nil {
			return err
		}
	}

	// create _bosmarmot_log
	if err = db.SynchronizeDB(getLogTableDef()); err != nil {
		return err
	}

	return nil
}

// Close database connection
func (db *SQLDB) Close() {
	if err := db.DB.Close(); err != nil {
		db.Log.Error("msg", "Error closing database", "err", err)
	}
}

// Ping database
func (db *SQLDB) Ping() error {
	err := db.DB.Ping()
	if err != nil {
		db.Log.Debug("msg", "Error database not available", "err", err)
	}
	return err
}

// GetLastBlockID returns last inserted blockId from log table
func (db *SQLDB) GetLastBlockID() (string, error) {
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

	query = fmt.Sprintf(query, db.Schema, db.Schema)
	id := ""

	db.Log.Debug("msg", "MAX ID", "query", clean(query))
	err := db.DB.QueryRow(query).Scan(&id)

	if err != nil {
		db.Log.Debug("msg", "Error selecting last block id", "err", err)
		return "", err
	}

	return id, nil
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
	query := fmt.Sprintf("INSERT INTO %s._bosmarmot_log (registers, height) VALUES ($1, $2) RETURNING id", db.Schema)
	db.Log.Debug("msg", "INSERT LOG", "query", clean(query), "value", fmt.Sprintf("%d %s", length, eventData.Block))
	err = tx.QueryRow(query, length, eventData.Block).Scan(&id)
	if err != nil {
		db.Log.Debug("msg", "Error inserting into _bosmarmot_log", "err", err)
		return err
	}

	// prepare log detail statement
	logQuery := fmt.Sprintf("INSERT INTO %s._bosmarmot_logdet (id,tblname,tblmap,registers) VALUES ($1,$2,$3,$4)", db.Schema)
	logStmt, err = tx.Prepare(logQuery)
	if err != nil {
		db.Log.Debug("msg", "Error preparing log stmt", "err", err)
		return err
	}

loop:
	// For Each table in the block
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
		uQuery := getUpsertQuery(db.Schema, table)

		// for Each Row
		for _, row := range dataRows {
			// get parameter interface
			pointers, value, err = getUpsertParams(uQuery, row)
			if err != nil {
				db.Log.Debug("msg", "Error building parameters", "err", err, "value", fmt.Sprintf("%v", row))
				return err
			}

			// upsert row data
			db.Log.Debug("msg", "UPSERT", "query", clean(uQuery.query), "value", value)
			_, err = tx.Exec(uQuery.query, pointers...)
			if err != nil {
				db.Log.Debug("msg", "Error Upserting", "err", err)
				// exits from all loops -> continue in close log stmt
				break loop
			}
		}
	}

	// close log statement
	if err == nil {
		err = logStmt.Close()
		if err != nil {
			db.Log.Debug("msg", "Error closing log stmt", "err", err)
		}
	}

	//------------------------error handling----------------------
	if err != nil {
		// rollback error
		errRb := tx.Rollback()
		if errRb != nil {
			db.Log.Debug("msg", "Error on rollback", "err", errRb)
			return errRb
		}

		if pqErr, ok := err.(*pq.Error); ok {
			// table does not exists
			if pqErr.Code == errUndefinedTable {
				db.Log.Warn("msg", "Table not found", "value", safeTable)
				if err = db.SynchronizeDB(eventTables); err != nil {
					return err
				}
				return db.SetBlock(eventTables, eventData)
			}

			// columns do not match
			if pqErr.Code == errUndefinedColumn {
				db.Log.Warn("msg", "Column not found", "value", safeTable)
				if err = db.SynchronizeDB(eventTables); err != nil {
					return err
				}
				return db.SetBlock(eventTables, eventData)
			}
			db.Log.Debug("msg", "Error upserting row", "err", pqErr)
			return pqErr
		}
		return err
	}

	db.Log.Debug("msg", "COMMIT")
	err = tx.Commit()
	if err != nil {
		db.Log.Debug("msg", "Error on commit", "err", err)
		return err
	}

	return nil
}

// GetBlock returns a table's structure and row data
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
		query, err = getTableQuery(db.Schema, table, block)
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

		cols, err := rows.Columns()
		if err != nil {
			db.Log.Debug("msg", "Error getting row columns", "err", err)
			return data, err
		}
		rows.Close()
		
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

			//for each column in row
			for i, col := range cols {
				//if not null add  value
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

// DestroySchema deletes the default schema
func (db *SQLDB) DestroySchema() error {
	db.Log.Info("msg", "Dropping schema", "value", db.Schema)
	found, err := db.findDefaultSchema()

	if err != nil {
		return err
	}

	if found {
		query := fmt.Sprintf("DROP SCHEMA %s CASCADE;", db.Schema)

		db.Log.Info("msg", "Drop schema", "query", query)
		_, err := db.DB.Exec(query)
		if err != nil {
			db.Log.Debug("msg", "Error dropping schema", "err", err)
		}

		return err
	}

	return nil
}
