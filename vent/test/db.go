package test

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/monax/bosmarmot/vent/config"
	"github.com/monax/bosmarmot/vent/logger"
	"github.com/monax/bosmarmot/vent/sqldb"
	"github.com/monax/bosmarmot/vent/types"
	"os"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

var letters = []rune("abcdefghijklmnopqrstuvwxyz")

func randString(n int) string {
	b := make([]rune, n)

	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}

	return string(b)
}

func destroySchema(db *sqldb.SQLDB, dbSchema string) error {
	db.Log.Info("msg", "Dropping schema")
	query := fmt.Sprintf("DROP SCHEMA %s CASCADE;", dbSchema)

	db.Log.Info("msg", "Drop schema", "query", query)

	if _, err := db.DB.Exec(query); err != nil {
		db.Log.Debug("msg", "Error dropping schema", "err", err)
		return err
	}

	return nil
}

func deleteFile(dbURL, schema string) error {
	url := dbURL

	if schema != "" {
		url = url + "_" + schema
	}
	url += ".sqlite"
	return os.Remove(url)
}

// NewTestDB creates a database connection for testing
func NewTestDB(t *testing.T, database string) (*sqldb.SQLDB, func()) {
	t.Helper()

	cfg := config.DefaultFlags(database)

	dbSchema := fmt.Sprintf("test_%s", randString(10))
	log := logger.NewLogger("debug")

	db, err := sqldb.NewSQLDB(cfg.DBAdapter, cfg.DBURL, dbSchema, log)
	if err != nil {
		t.Fatal()
	}

	return db, func() {
		if database != types.SQLiteDB {
			destroySchema(db, dbSchema)
			db.Close()
		} else {
			db.Close()
			deleteFile(cfg.DBURL, dbSchema)
		}
	}
}
