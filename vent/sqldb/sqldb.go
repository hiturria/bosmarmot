package sqldb

import (
	"errors"

	"github.com/monax/bosmarmot/vent/logger"
	"github.com/monax/bosmarmot/vent/sqldb/adapters"
	"github.com/monax/bosmarmot/vent/types"
)

// SQLDB implements the access to a sql database
type SQLDB struct {
	DBAdapter adapters.DBAdapter
}

// NewSQLDB connects to a SQL database and creates default schema and _bosmarmot_log if missing
func NewSQLDB(dbAdapter string, dbURL string, schema string, l *logger.Logger) (*SQLDB, error) {
	db := &SQLDB{}

	switch dbAdapter {
	case "postgres":
		db.DBAdapter = adapters.NewSQLDB(dbURL, schema, l)

	default:
		return nil, errors.New("invalid database adapter")
	}

	err := db.DBAdapter.Open()

	return db, err
}

// Close database connection
func (db *SQLDB) Close() {
	db.DBAdapter.Close()
}

// Ping database
func (db *SQLDB) Ping() error {
	return db.DBAdapter.Ping()
}

// GetLastBlockID returns last inserted blockId from log table
func (db *SQLDB) GetLastBlockID() (string, error) {
	return db.DBAdapter.GetLastBlockID()
}

// SynchronizeDB synchronize config structures with SQL database table structures
func (db *SQLDB) SynchronizeDB(eventTables types.EventTables) error {
	return db.DBAdapter.SynchronizeDB(eventTables)
}

// SetBlock inserts or updates multiple rows and stores log info in SQL tables
func (db *SQLDB) SetBlock(eventTables types.EventTables, eventData types.EventData) error {
	return db.DBAdapter.SetBlock(eventTables, eventData)
}

// GetBlock returns a table's structure and row data
func (db *SQLDB) GetBlock(block string) (types.EventData, error) {
	return db.DBAdapter.GetBlock(block)
}

// DestroySchema deletes the default schema
func (db *SQLDB) DestroySchema() error {
	return db.DBAdapter.DestroySchema()
}
