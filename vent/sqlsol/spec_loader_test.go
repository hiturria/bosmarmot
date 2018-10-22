package sqlsol_test

import (
	"os"
	"strings"
	"testing"

	"github.com/monax/bosmarmot/vent/sqlsol"
	"github.com/monax/bosmarmot/vent/types"
	"github.com/stretchr/testify/require"
)

func TestSpecLoader(t *testing.T) {

	specFile := os.Getenv("GOPATH") + "/src/github.com/monax/bosmarmot/vent/test/sqlsol_example.json"
	dBBlockTx := true

	t.Run("successfully add block and transaction tables to event structures", func(t *testing.T) {

		parser, err := sqlsol.SpecLoader(specFile, "", dBBlockTx)

		require.NoError(t, err)
		require.Equal(t, 4, len(parser.Tables))
		require.Equal(t, types.SQLBlockTableName, parser.Tables[types.SQLBlockTableName].Name)
		require.Equal(t, strings.ToLower("_height"), parser.Tables[types.SQLBlockTableName].Columns["height"].Name)
		require.Equal(t, types.SQLTxTableName, parser.Tables[types.SQLTxTableName].Name)
		require.Equal(t, strings.ToLower("_txhash"), parser.Tables[types.SQLTxTableName].Columns["txHash"].Name)
	})
}
