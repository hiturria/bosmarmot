// +build integration

package sqldb_test

import (
	"fmt"
	"testing"

	"github.com/monax/bosmarmot/vent/sqlsol"
	"github.com/monax/bosmarmot/vent/test"
	"github.com/monax/bosmarmot/vent/types"
	"github.com/stretchr/testify/require"
)

func TestSynchronizeDB(t *testing.T) {
	t.Run("successfully creates default schema, log tables and synchronizes db", func(t *testing.T) {
		goodJSON := test.GoodJSONConfFile(t)

		byteValue := []byte(goodJSON)
		tableStruct, _ := sqlsol.NewParser(byteValue)

		db, closeDB := test.NewTestDB(t)
		defer closeDB()

		errp := db.Ping()
		require.NoError(t, errp)

		err := db.SynchronizeDB(tableStruct.GetTables())
		require.NoError(t, err)
	})
}

func TestSetBlock(t *testing.T) {
	t.Run("successfully inserts a block", func(t *testing.T) {

		db, closeDB := test.NewTestDB(t)
		defer closeDB()

		errp := db.Ping()
		require.NoError(t, errp)

		str, dat := getBlock()

		err := db.SetBlock(str, dat)
		require.NoError(t, err)

		id, erri := db.GetLastBlockID()
		fmt.Println("id=", id)
		require.NoError(t, erri)

		eventData, erre := db.GetBlock(dat.Block)
		fmt.Println(eventData)
		require.NoError(t, erre)
	})

	t.Run("successfully creates a table", func(t *testing.T) {
		db, closeDB := test.NewTestDB(t)
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
		table1 := types.SQLTable{Name: "FullDataTable", Columns: cols1}
		tables := make(map[string]types.SQLTable)
		tables["FullDataTable"] = table1

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
	cols1["Column3"] = types.SQLTableColumn{Name: "height", Type: types.SQLColumnTypeVarchar, Length: 100, Primary: false, Order: 4}
	cols1["Column4"] = types.SQLTableColumn{Name: "col4", Type: types.SQLColumnTypeText, Primary: false, Order: 5}
	table1 := types.SQLTable{Name: "test_table1", Columns: cols1}

	//table 2
	cols2 := make(map[string]types.SQLTableColumn)
	cols2["ID"] = types.SQLTableColumn{Name: "height", Type: types.SQLColumnTypeVarchar, Length: 100, Primary: true, Order: 1}
	cols2["SID"] = types.SQLTableColumn{Name: "sid_id", Type: types.SQLColumnTypeInt, Primary: true, Order: 2}
	cols2["Field 1"] = types.SQLTableColumn{Name: "field_1", Type: types.SQLColumnTypeVarchar, Length: 100, Primary: false, Order: 3}
	cols2["Field 2"] = types.SQLTableColumn{Name: "field_2", Type: types.SQLColumnTypeVarchar, Length: 100, Primary: false, Order: 4}
	table2 := types.SQLTable{Name: "test_table2", Columns: cols2}

	//table 3
	cols3 := make(map[string]types.SQLTableColumn)
	cols3["Code"] = types.SQLTableColumn{Name: "height", Type: types.SQLColumnTypeVarchar, Length: 100, Primary: true, Order: 1}
	cols3["Value A"] = types.SQLTableColumn{Name: "val", Type: types.SQLColumnTypeInt, Primary: false, Order: 2}
	table3 := types.SQLTable{Name: "test_table3", Columns: cols3}

	str := make(types.EventTables)
	str["First"] = table1
	str["Second"] = table2
	str["Third"] = table3

	//---------------------------------------data-------------------------------------
	var dat types.EventData
	dat.Block = "0123456789ABCDEF0"
	dat.Tables = make(map[string]types.EventDataTable)

	row11 := map[string]string{"test_id": "1", "col1": "text11", "col2": "text12", "height": "0123456789ABCDEF0", "col4": "14"}
	row12 := map[string]string{"test_id": "2", "col1": "text21", "col2": "text22", "height": "0123456789ABCDEF0", "col4": "24"}
	row13 := map[string]string{"test_id": "3", "col1": "text31", "col2": "text32", "height": "0123456789ABCDEF0", "col4": "34"}
	row14 := map[string]string{"test_id": "4", "col1": "text41", "col3": "text43", "height": "0123456789ABCDEF0"}
	row15 := map[string]string{"test_id": "1", "col1": "upd", "col2": "upd", "height": "0123456789ABCDEF0", "col4": "upd"}
	var rows1 []types.EventDataRow
	rows1 = append(rows1, row11)
	rows1 = append(rows1, row12)
	rows1 = append(rows1, row13)
	rows1 = append(rows1, row14)
	rows1 = append(rows1, row15)
	dat.Tables["test_table1"] = rows1

	row21 := map[string]string{"height": "0123456789ABCDEF0", "sid_id": "1", "field_1": "A", "field_2": "B"}
	row22 := map[string]string{"height": "0123456789ABCDEF0", "sid_id": "2", "field_1": "C", "field_2": ""}
	row23 := map[string]string{"height": "0123456789ABCDEF0", "sid_id": "3", "field_1": "D", "field_2": "E"}
	row24 := map[string]string{"height": "0123456789ABCDEF0", "sid_id": "4", "field_1": "F"}
	row25 := map[string]string{"height": "0123456789ABCDEF0", "sid_id": "1", "field_1": "U", "field_2": "U"}
	var rows2 []types.EventDataRow
	rows2 = append(rows2, row21)
	rows2 = append(rows2, row22)
	rows2 = append(rows2, row23)
	rows2 = append(rows2, row24)
	rows2 = append(rows2, row25)
	dat.Tables["test_table2"] = rows2

	row31 := map[string]string{"height": "0123456789ABCDEF0", "val": "1"}
	row32 := map[string]string{"height": "0123456789ABCDEF0", "val": "2"}
	row33 := map[string]string{"height": "0123456789ABCDEF0", "val": "-1"}
	row34 := map[string]string{"height": "0123456789ABCDEF0"}
	var rows3 []types.EventDataRow
	rows3 = append(rows3, row31)
	rows3 = append(rows3, row32)
	rows3 = append(rows3, row33)
	rows3 = append(rows3, row34)
	dat.Tables["test_table3"] = rows3

	return str, dat
}
