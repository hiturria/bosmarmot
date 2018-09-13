package test

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/monax/bosmarmot/vent/config"
	"github.com/monax/bosmarmot/vent/logger"
	"github.com/monax/bosmarmot/vent/sqldb"
	"github.com/monax/bosmarmot/vent/types"
)

var letters = []rune("abcdefghijklmnopqrstuvwxyz")

func init() {
	rand.Seed(time.Now().UnixNano())
}

// NewTestDB creates a database connection for testing
func NewTestDB(t *testing.T, dbAdapter string) (*sqldb.SQLDB, func()) {
	t.Helper()

	log := logger.NewLogger("debug")
	cfg := config.DefaultFlags()
	randName := randString(10)

	switch dbAdapter {
	case types.PostgresDB:
		cfg.DBSchema = fmt.Sprintf("test_%s", randName)
	case types.SQLiteDB:
		cfg.DBAdapter = dbAdapter
		cfg.DBURL = fmt.Sprintf("./test_%s.sqlite", randName)
	default:
		t.Fatal("invalid database adapter")
	}

	db, err := sqldb.NewSQLDB(cfg.DBAdapter, cfg.DBURL, cfg.DBSchema, log)
	if err != nil {
		t.Fatal(err.Error())
	}

	return db, func() {
		if dbAdapter == types.SQLiteDB {
			db.Close()
			os.Remove(cfg.DBURL)
		} else {
			destroySchema(db, cfg.DBSchema)
			db.Close()
		}
	}
}

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
