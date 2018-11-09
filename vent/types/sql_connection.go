package types

import "github.com/monax/bosmarmot/vent/logger"

type SqlConnection struct {
	DBAdapter     string
	DBURL         string
	DBSchema      string
	ChainID       string
	BurrowVersion string

	Log *logger.Logger
}
