// +build integration

package sqldb_test

import (
	"testing"

	"github.com/monax/bosmarmot/vent/sqlsol"
	"github.com/monax/bosmarmot/vent/test"
	"github.com/monax/bosmarmot/vent/types"
	"github.com/stretchr/testify/require"
)

func TestSynchronizeDB(t *testing.T) {
	t.Run("POSTGRES: successfully creates database tables and synchronizes db", func(t *testing.T) {
		goodJSON := test.GoodJSONConfFile(t)

		byteValue := []byte(goodJSON)
		tableStructure, _ := sqlsol.NewParserFromBytes(byteValue)

		db, cleanUpDB := test.NewTestDB(t, types.PostgresDB)
		defer cleanUpDB()

		err := db.Ping()
		require.NoError(t, err)

		err = db.SynchronizeDB(tableStructure.GetTables())
		require.NoError(t, err)
	})

	t.Run("SQLITE: successfully creates database tables and synchronizes db", func(t *testing.T) {
		goodJSON := test.GoodJSONConfFile(t)

		byteValue := []byte(goodJSON)
		tableStructure, _ := sqlsol.NewParserFromBytes(byteValue)

		db, closeDB := test.NewTestDB(t, types.SQLiteDB)
		defer closeDB()

		err := db.Ping()
		require.NoError(t, err)

		err = db.SynchronizeDB(tableStructure.GetTables())
		require.NoError(t, err)
	})
}

func TestSetBlock(t *testing.T) {
	t.Run("POSTGRES: successfully inserts a block", func(t *testing.T) {
		db, closeDB := test.NewTestDB(t, types.PostgresDB)
		defer closeDB()

		errp := db.Ping()
		require.NoError(t, errp)

		// new
		str, dat := getBlock()
		err := db.SetBlock(str, dat)
		require.NoError(t, err)

		// read
		_, err = db.GetLastBlockID()
		require.NoError(t, err)

		_, err = db.GetBlock(dat.Block)
		require.NoError(t, err)

		// alter
		str, dat = getAlterBlock()
		err = db.SetBlock(str, dat)
		require.NoError(t, err)
	})

	t.Run("SQLITE: successfully inserts a block", func(t *testing.T) {
		db, closeDB := test.NewTestDB(t, types.SQLiteDB)
		defer closeDB()

		errp := db.Ping()
		require.NoError(t, errp)

		// new
		str, dat := getBlock()
		err := db.SetBlock(str, dat)
		require.NoError(t, err)

		// read
		_, err = db.GetLastBlockID()
		require.NoError(t, err)

		_, err = db.GetBlock(dat.Block)
		require.NoError(t, err)

		// alter
		str, dat = getAlterBlock()
		err = db.SetBlock(str, dat)
		require.NoError(t, err)
	})

	t.Run("POSTGRES: successfully creates an empty table", func(t *testing.T) {
		db, closeDB := test.NewTestDB(t, types.PostgresDB)
		defer closeDB()

		errp := db.Ping()
		require.NoError(t, errp)

		//table 1
		cols1 := make(map[string]types.SQLTableColumn)
		cols1["ID"] = types.SQLTableColumn{Name: "test_id", Type: types.SQLColumnTypeSerial, Primary: true, Order: 1}
		cols1["Column1"] = types.SQLTableColumn{Name: "col1", Type: types.SQLColumnTypeBool, Primary: false, Order: 2}
		cols1["Column2"] = types.SQLTableColumn{Name: "col2", Type: types.SQLColumnTypeByteA, Primary: false, Order: 3}
		cols1["Column3"] = types.SQLTableColumn{Name: "col3", Type: types.SQLColumnTypeInt, Primary: false, Order: 4}
		cols1["Column4"] = types.SQLTableColumn{Name: "col4", Type: types.SQLColumnTypeText, Primary: false, Order: 5}
		cols1["Column5"] = types.SQLTableColumn{Name: "col5", Type: types.SQLColumnTypeTimeStamp, Primary: false, Order: 6}
		cols1["Column6"] = types.SQLTableColumn{Name: "col6", Type: types.SQLColumnTypeVarchar, Length: 100, Primary: false, Order: 7}
		table1 := types.SQLTable{Name: "AllDataTypesTable", Columns: cols1}
		tables := make(map[string]types.SQLTable)
		tables["AllDataTypesTable"] = table1

		err := db.SynchronizeDB(tables)
		require.NoError(t, err)
	})

	t.Run("SQLITE: successfully creates an empty table", func(t *testing.T) {
		db, closeDB := test.NewTestDB(t, types.SQLiteDB)
		defer closeDB()

		errp := db.Ping()
		require.NoError(t, errp)

		//table 1
		cols1 := make(map[string]types.SQLTableColumn)
		cols1["ID"] = types.SQLTableColumn{Name: "test_id", Type: types.SQLColumnTypeSerial, Primary: true, Order: 1}
		cols1["Column1"] = types.SQLTableColumn{Name: "col1", Type: types.SQLColumnTypeBool, Primary: false, Order: 2}
		cols1["Column2"] = types.SQLTableColumn{Name: "col2", Type: types.SQLColumnTypeByteA, Primary: false, Order: 3}
		cols1["Column3"] = types.SQLTableColumn{Name: "col3", Type: types.SQLColumnTypeInt, Primary: false, Order: 4}
		cols1["Column4"] = types.SQLTableColumn{Name: "col4", Type: types.SQLColumnTypeText, Primary: false, Order: 5}
		cols1["Column5"] = types.SQLTableColumn{Name: "col5", Type: types.SQLColumnTypeTimeStamp, Primary: false, Order: 6}
		cols1["Column6"] = types.SQLTableColumn{Name: "col6", Type: types.SQLColumnTypeVarchar, Length: 100, Primary: false, Order: 7}
		table1 := types.SQLTable{Name: "AllDataTypesTable", Columns: cols1}
		tables := make(map[string]types.SQLTable)
		tables["AllDataTypesTable"] = table1

		err := db.SynchronizeDB(tables)
		require.NoError(t, err)
	})
}

func getBlock() (types.EventTables, types.EventData) {
	//table 1
	cols1 := make(map[string]types.SQLTableColumn)
	cols1["ID"] = types.SQLTableColumn{Name: "test_id", Type: types.SQLColumnTypeInt, Primary: true, Order: 1}
	cols1["Column1"] = types.SQLTableColumn{Name: "col1", Type: types.SQLColumnTypeVarchar, Length: 100, Primary: false, Order: 2}
	cols1["Column2"] = types.SQLTableColumn{Name: "col2", Type: types.SQLColumnTypeVarchar, Length: 100, Primary: false, Order: 3}
	cols1["Column3"] = types.SQLTableColumn{Name: "_height", Type: types.SQLColumnTypeVarchar, Length: 100, Primary: false, Order: 4}
	cols1["Column4"] = types.SQLTableColumn{Name: "col4", Type: types.SQLColumnTypeText, Primary: false, Order: 5}
	table1 := types.SQLTable{Name: "test_table1", Filter: "TEST", Columns: cols1}

	//table 2
	cols2 := make(map[string]types.SQLTableColumn)
	cols2["ID"] = types.SQLTableColumn{Name: "_height", Type: types.SQLColumnTypeVarchar, Length: 100, Primary: true, Order: 1}
	cols2["SID"] = types.SQLTableColumn{Name: "sid_id", Type: types.SQLColumnTypeInt, Primary: true, Order: 2}
	cols2["Field 1"] = types.SQLTableColumn{Name: "field_1", Type: types.SQLColumnTypeVarchar, Length: 100, Primary: false, Order: 3}
	cols2["Field 2"] = types.SQLTableColumn{Name: "field_2", Type: types.SQLColumnTypeVarchar, Length: 100, Primary: false, Order: 4}
	table2 := types.SQLTable{Name: "test_table2", Filter: "TEST", Columns: cols2}

	//table 3
	cols3 := make(map[string]types.SQLTableColumn)
	cols3["Code"] = types.SQLTableColumn{Name: "_height", Type: types.SQLColumnTypeVarchar, Length: 100, Primary: true, Order: 1}
	cols3["Value A"] = types.SQLTableColumn{Name: "val", Type: types.SQLColumnTypeInt, Primary: false, Order: 2}
	table3 := types.SQLTable{Name: "test_table3", Filter: "TEST", Columns: cols3}

	//table 4
	cols4 := make(map[string]types.SQLTableColumn)
	cols4["index"] = types.SQLTableColumn{Name: "index", Type: types.SQLColumnTypeInt, Primary: true, Order: 1}
	cols4["time"] = types.SQLTableColumn{Name: "time", Type: types.SQLColumnTypeTimeStamp, Primary: false, Order: 2}
	cols4["Code"] = types.SQLTableColumn{Name: "_height", Type: types.SQLColumnTypeVarchar, Length: 100, Primary: false, Order: 3}
	table4 := types.SQLTable{Name: "test_table4", Filter: "TEST", Columns: cols4}

	str := make(types.EventTables)
	str["1"] = table1
	str["2"] = table2
	str["3"] = table3
	str["4"] = table4

	//---------------------------------------data-------------------------------------
	var dat types.EventData
	dat.Block = "0123456789ABCDEF0"
	dat.Tables = make(map[string]types.EventDataTable)

	var rows1 []types.EventDataRow
	rows1 = append(rows1, types.EventDataRow{Action: types.ActionUpsert, RowData: map[string]interface{}{"test_id": "1", "col1": "text11", "col2": "text12", "_height": "0123456789ABCDEF0", "col4": "14"}})
	rows1 = append(rows1, types.EventDataRow{Action: types.ActionUpsert, RowData: map[string]interface{}{"test_id": "2", "col1": "text21", "col2": "text22", "_height": "0123456789ABCDEF0", "col4": "24"}})
	rows1 = append(rows1, types.EventDataRow{Action: types.ActionUpsert, RowData: map[string]interface{}{"test_id": "3", "col1": "text31", "col2": "text32", "_height": "0123456789ABCDEF0", "col4": "34"}})
	rows1 = append(rows1, types.EventDataRow{Action: types.ActionUpsert, RowData: map[string]interface{}{"test_id": "4", "col1": "text41", "col3": "text43", "_height": "0123456789ABCDEF0"}})
	rows1 = append(rows1, types.EventDataRow{Action: types.ActionUpsert, RowData: map[string]interface{}{"test_id": "1", "col1": "upd", "col2": "upd", "_height": "0123456789ABCDEF0", "col4": "upd"}})
	dat.Tables["test_table1"] = rows1

	var rows2 []types.EventDataRow
	rows2 = append(rows2, types.EventDataRow{Action: types.ActionUpsert, RowData: map[string]interface{}{"_height": "0123456789ABCDEF0", "sid_id": "1", "field_1": "A", "field_2": "B"}})
	rows2 = append(rows2, types.EventDataRow{Action: types.ActionUpsert, RowData: map[string]interface{}{"_height": "0123456789ABCDEF0", "sid_id": "2", "field_1": "C", "field_2": ""}})
	rows2 = append(rows2, types.EventDataRow{Action: types.ActionUpsert, RowData: map[string]interface{}{"_height": "0123456789ABCDEF0", "sid_id": "3", "field_1": "D", "field_2": "E"}})
	rows2 = append(rows2, types.EventDataRow{Action: types.ActionUpsert, RowData: map[string]interface{}{"_height": "0123456789ABCDEF0", "sid_id": "1", "field_1": "F"}})
	rows2 = append(rows2, types.EventDataRow{Action: types.ActionUpsert, RowData: map[string]interface{}{"_height": "0123456789ABCDEF0", "sid_id": "1", "field_2": "U"}})
	dat.Tables["test_table2"] = rows2

	var rows3 []types.EventDataRow
	rows3 = append(rows3, types.EventDataRow{Action: types.ActionUpsert, RowData: map[string]interface{}{"_height": "0123456789ABCDEF1", "val": "1"}})
	rows3 = append(rows3, types.EventDataRow{Action: types.ActionUpsert, RowData: map[string]interface{}{"_height": "0123456789ABCDEF2", "val": "2"}})
	rows3 = append(rows3, types.EventDataRow{Action: types.ActionUpsert, RowData: map[string]interface{}{"_height": "0123456789ABCDEFX", "val": "-1"}})
	rows3 = append(rows3, types.EventDataRow{Action: types.ActionUpsert, RowData: map[string]interface{}{"_height": "0123456789ABCDEF0"}})
	dat.Tables["test_table3"] = rows3

	var rows4 []types.EventDataRow
	rows4 = append(rows4, types.EventDataRow{Action: types.ActionUpsert, RowData: map[string]interface{}{"_height": "0123456789ABCDEF0", "time": "2006-01-01 15:04:05", "index": "1"}})
	rows4 = append(rows4, types.EventDataRow{Action: types.ActionUpsert, RowData: map[string]interface{}{"_height": "0123456789ABCDEF0", "time": "2006-01-02 15:04:05", "index": "2"}})
	rows4 = append(rows4, types.EventDataRow{Action: types.ActionUpsert, RowData: map[string]interface{}{"_height": "0123456789ABCDEF0", "time": "2006-01-03 15:04:05", "index": "3"}})
	rows4 = append(rows4, types.EventDataRow{Action: types.ActionUpsert, RowData: map[string]interface{}{"_height": "0123456789ABCDEF0", "time": "2006-01-03 15:04:05", "index": "4"}})
	rows4 = append(rows4, types.EventDataRow{Action: types.ActionDelete, RowData: map[string]interface{}{"_height": "0123456789ABCDEF0", "time": "2006-01-03 15:04:05", "index": "3"}})
	dat.Tables["test_table4"] = rows4

	return str, dat
}

func getAlterBlock() (types.EventTables, types.EventData) {
	//table 3
	cols3 := make(map[string]types.SQLTableColumn)
	cols3["Code"] = types.SQLTableColumn{Name: "_height", Type: types.SQLColumnTypeVarchar, Length: 100, Primary: true, Order: 1}
	cols3["Value A"] = types.SQLTableColumn{Name: "val", Type: types.SQLColumnTypeInt, Primary: false, Order: 2}
	cols3["Value B"] = types.SQLTableColumn{Name: "val_alter", Type: types.SQLColumnTypeInt, Primary: false, Order: 3}
	table3 := types.SQLTable{Name: "test_table3", Filter: "TEST", Columns: cols3}

	str := make(types.EventTables)
	str["3"] = table3

	//---------------------------------------data-------------------------------------
	var dat types.EventData
	dat.Block = "AAAAAAAAAAAAAA"
	dat.Tables = make(map[string]types.EventDataTable)

	var rows5 []types.EventDataRow
	rows5 = append(rows5, types.EventDataRow{Action: types.ActionUpsert, RowData: map[string]interface{}{"_height": "AAAAAAAAAAAAAAAAA", "val": "1", "val_alter": "1"}})
	dat.Tables["test_table3"] = rows5

	return str, dat
}
