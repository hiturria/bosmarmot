package test

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/monax/bosmarmot/vent/config"
	"github.com/monax/bosmarmot/vent/logger"
	"github.com/monax/bosmarmot/vent/sqldb"
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

// NewTestDB creates a database connection for testing
func NewTestDB(t *testing.T) (*sqldb.SQLDB, func()) {
	t.Helper()

	cfg := config.DefaultFlags()
	dbSchema := fmt.Sprintf("test_%s", randString(10))
	log := logger.NewLogger("debug")

	db, err := sqldb.NewSQLDB(cfg.DBAdapter, cfg.DBURL, dbSchema, log)
	if err != nil {
		t.Fatal()
	}

	return db, func() {
		db.DestroySchema()
		db.Close()
	}
}
