package sqlsol_test

import (
	"strings"
	"testing"

	"github.com/monax/bosmarmot/vent/sqlsol"
	"github.com/monax/bosmarmot/vent/test"
	"github.com/monax/bosmarmot/vent/types"
	"github.com/stretchr/testify/require"
)

func TestNewParser(t *testing.T) {
	t.Run("returns an error if the json is malformed", func(t *testing.T) {
		badJSON := test.BadJSONConfFile(t)

		byteValue := []byte(badJSON)
		_, err := sqlsol.NewParser(byteValue)
		require.Error(t, err)
	})

	t.Run("returns an error if needed json fields are missing", func(t *testing.T) {
		missingFields := test.MissingFieldsJSONConfFile(t)

		byteValue := []byte(missingFields)
		_, err := sqlsol.NewParser(byteValue)
		require.Error(t, err)
	})

	t.Run("successfully builds table structure based on json events config", func(t *testing.T) {
		goodJSON := test.GoodJSONConfFile(t)

		byteValue := []byte(goodJSON)
		tableStruct, err := sqlsol.NewParser(byteValue)
		require.NoError(t, err)

		// table structure contents
		table, _ := tableStruct.GetTableName("UpdateUserAccount")
		require.Equal(t, "useraccounts", table)

		// columns map
		col, err := tableStruct.GetColumn("UpdateUserAccount", "userName")
		require.NoError(t, err)
		require.Equal(t, false, col.Primary)
		require.Equal(t, types.SQLColumnTypeText, col.Type)
		require.Equal(t, "username", col.Name)

		col, err = tableStruct.GetColumn("UpdateUserAccount", "userAddress")
		require.NoError(t, err)
		require.Equal(t, true, col.Primary)
		require.Equal(t, types.SQLColumnTypeVarchar, col.Type)
		require.Equal(t, "address", col.Name)

		col, err = tableStruct.GetColumn("UpdateUserAccount", "index")
		require.NoError(t, err)
		require.Equal(t, false, col.Primary)
		require.Equal(t, types.SQLColumnTypeInt, col.Type)
		require.Equal(t, "_index", col.Name)
		require.Equal(t, 3, col.Order)

		col, err = tableStruct.GetColumn("UpdateUserAccount", "height")
		require.NoError(t, err)
		require.Equal(t, false, col.Primary)
		require.Equal(t, types.SQLColumnTypeVarchar, col.Type)
		require.Equal(t, "_height", col.Name)
		require.Equal(t, 1, col.Order)
	})

	t.Run("returns an error if the event type of a given column is unknown", func(t *testing.T) {
		typeUnknownJSON := test.UnknownTypeJSONConfFile(t)

		byteValue := []byte(typeUnknownJSON)
		_, err := sqlsol.NewParser(byteValue)
		require.Error(t, err)
	})

	t.Run("returns an error if there are duplicated table names in json file", func(t *testing.T) {
		duplicatedTableNameJSON := test.DuplicatedTableNameJSONConfFile(t)

		byteValue := []byte(duplicatedTableNameJSON)
		_, err := sqlsol.NewParser(byteValue)
		require.Error(t, err)
	})

	t.Run("returns an error if there are duplicated column names for a given table in json file", func(t *testing.T) {
		duplicatedColNameJSON := test.DuplicatedColNameJSONConfFile(t)

		byteValue := []byte(duplicatedColNameJSON)
		_, err := sqlsol.NewParser(byteValue)
		require.Error(t, err)
	})

}

func TestGetTableName(t *testing.T) {
	goodJSON := test.GoodJSONConfFile(t)

	byteValue := []byte(goodJSON)
	tableStruct, _ := sqlsol.NewParser(byteValue)

	t.Run("successfully gets the mapping table name for a given event name", func(t *testing.T) {
		tableName, err := tableStruct.GetTableName("UpdateTable")
		require.NoError(t, err)
		require.Equal(t, strings.ToLower("TEST_TABLE"), tableName)
	})

	t.Run("unsuccessfully gets the mapping table name for a non existing event name", func(t *testing.T) {
		tableName, err := tableStruct.GetTableName("NOT_EXISTS")
		require.Error(t, err)
		require.Equal(t, "", tableName)
	})
}

func TestGetColumnName(t *testing.T) {
	goodJSON := test.GoodJSONConfFile(t)

	byteValue := []byte(goodJSON)
	tableStruct, _ := sqlsol.NewParser(byteValue)

	t.Run("successfully gets the mapping column name for a given event name/item", func(t *testing.T) {
		columnName, err := tableStruct.GetColumnName("UpdateTable", "blocknum")
		require.NoError(t, err)
		require.Equal(t, strings.ToLower("Block"), columnName)
	})

	t.Run("unsuccessfully gets the mapping column name for a non existent event name", func(t *testing.T) {
		columnName, err := tableStruct.GetColumnName("NOT_EXISTS", "userName")
		require.Error(t, err)
		require.Equal(t, "", columnName)
	})

	t.Run("unsuccessfully gets the mapping column name for a non existent event item", func(t *testing.T) {
		columnName, err := tableStruct.GetColumnName("UpdateUserAccount", "NOT_EXISTS")
		require.Error(t, err)
		require.Equal(t, "", columnName)
	})
}

func TestGetColumn(t *testing.T) {
	goodJSON := test.GoodJSONConfFile(t)

	byteValue := []byte(goodJSON)
	tableStruct, _ := sqlsol.NewParser(byteValue)

	t.Run("successfully gets the mapping column info for a given event name/item", func(t *testing.T) {
		column, err := tableStruct.GetColumn("UpdateTable", "blocknum")
		require.NoError(t, err)
		require.Equal(t, strings.ToLower("block"), column.Name)
		require.Equal(t, types.SQLColumnTypeInt, column.Type)
		require.Equal(t, false, column.Primary)
	})

	t.Run("unsuccessfully gets the mapping column info for a non existent event name", func(t *testing.T) {
		_, err := tableStruct.GetColumn("NOT_EXISTS", "userName")
		require.Error(t, err)
	})

	t.Run("unsuccessfully gets the mapping column info for a non existent event item", func(t *testing.T) {
		_, err := tableStruct.GetColumn("UpdateUserAccount", "NOT_EXISTS")
		require.Error(t, err)
	})
}

func TestSetTableName(t *testing.T) {
	goodJSON := test.GoodJSONConfFile(t)

	byteValue := []byte(goodJSON)
	tableStruct, _ := sqlsol.NewParser(byteValue)

	t.Run("successfully updates table name for a given event name", func(t *testing.T) {
		err := tableStruct.SetTableName("UpdateTable", "TEST_TABLE")

		tableName, _ := tableStruct.GetTableName("UpdateTable")
		require.NoError(t, err)
		require.Equal(t, strings.ToLower("TEST_TABLE"), tableName)
	})
}

func TestGetTables(t *testing.T) {
	goodJSON := test.GoodJSONConfFile(t)

	byteValue := []byte(goodJSON)
	tableStruct, _ := sqlsol.NewParser(byteValue)

	t.Run("successfully returns event tables structures", func(t *testing.T) {
		tables := tableStruct.GetTables()
		require.Equal(t, 2, len(tables))
		require.Equal(t, "useraccounts", tables["UpdateUserAccount"].Name)
		require.Equal(t, "LOG0 = 'UserAccounts'", tables["UpdateUserAccount"].Filter)

	})
}

func TestGetEventSpec(t *testing.T) {
	goodJSON := test.GoodJSONConfFile(t)

	byteValue := []byte(goodJSON)
	tableStruct, _ := sqlsol.NewParser(byteValue)

	t.Run("successfully returns event specification structures", func(t *testing.T) {
		eventSpec := tableStruct.GetEventSpec()
		require.Equal(t, 2, len(eventSpec))
		require.Equal(t, "LOG0 = 'UserAccounts'", eventSpec[0].Filter)
		require.Equal(t, "UserAccounts", eventSpec[0].TableName)
		require.Equal(t, "UpdateUserAccount", eventSpec[0].Event.Name)

		require.Equal(t, "Log1Text = 'EVENT_TEST'", eventSpec[1].Filter)
		require.Equal(t, "TEST_TABLE", eventSpec[1].TableName)
		require.Equal(t, "UpdateTable", eventSpec[1].Event.Name)
	})
}
