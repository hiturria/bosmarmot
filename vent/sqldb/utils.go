package sqldb

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/monax/bosmarmot/vent/types"
)

// findDefaultSchema checks if the default schema exists in SQL database
func (db *SQLDB) findDefaultSchema() (bool, error) {
	var found bool

	query := db.DBAdapter.FindSchemaQuery()

	db.Log.Debug("msg", "FIND SCHEMA", "query", clean(query))
	err := db.DB.QueryRow(query).Scan(&found)
	if err == nil {
		if !found {
			db.Log.Warn("msg", "Schema not found")
		}
	} else {
		db.Log.Debug("msg", "Error searching schema", "err", err)
	}

	return found, err
}

// createDefaultSchema creates the default schema in SQL database
func (db *SQLDB) createDefaultSchema() error {
	db.Log.Info("msg", "Creating schema")

	query := db.DBAdapter.CreateSchemaQuery()

	db.Log.Debug("msg", "CREATE SCHEMA", "query", clean(query))
	_, err := db.DB.Exec(query)
	if err != nil {
		if db.DBAdapter.ErrorEquals(err, types.SQLErrorTypeDuplicatedSchema) {
			db.Log.Warn("msg", "Duplicated schema")
			return nil
		}
	}
	return err
}

// findTable checks if a table exists in the default schema
func (db *SQLDB) findTable(tableName string) (bool, error) {
	found := false
	safeTable := safe(tableName)
	query := db.DBAdapter.FindTableQuery(safeTable)

	db.Log.Debug("msg", "FIND TABLE", "query", clean(query), "value", safeTable)
	err := db.DB.QueryRow(query).Scan(&found)

	if err == nil {
		if !found {
			db.Log.Warn("msg", "Table not found", "value", safeTable)
		}
	} else {
		db.Log.Debug("msg", "Error finding table", "err", err)
	}

	return found, err
}

// getLogTableDef returns log structures
func (db *SQLDB) getLogTableDef() types.EventTables {
	tables := make(types.EventTables)
	logCol := make(map[string]types.SQLTableColumn)

	logCol["id"] = types.SQLTableColumn{
		Name:    "id",
		Type:    types.SQLColumnTypeSerial,
		Primary: true,
		Order:   1,
	}

	logCol["timestamp"] = types.SQLTableColumn{
		Name:    "timestamp",
		Type:    types.SQLColumnTypeTimeStamp,
		Primary: false,
		Order:   2,
	}

	logCol["registers"] = types.SQLTableColumn{
		Name:    "registers",
		Type:    types.SQLColumnTypeInt,
		Primary: false,
		Order:   3,
	}

	logCol["height"] = types.SQLTableColumn{
		Name:    "height",
		Type:    types.SQLColumnTypeVarchar,
		Length:  100,
		Primary: false,
		Order:   4,
	}

	tables["log"] = types.SQLTable{
		Name:    "_bosmarmot_log",
		Columns: logCol,
	}

	detCol := make(map[string]types.SQLTableColumn)

	detCol["id"] = types.SQLTableColumn{
		Name:    "id",
		Type:    types.SQLColumnTypeInt,
		Primary: true,
		Order:   1,
	}

	detCol["tableName"] = types.SQLTableColumn{
		Name:    "tblname",
		Type:    types.SQLColumnTypeVarchar,
		Length:  100,
		Primary: true,
		Order:   2,
	}

	detCol["tableMap"] = types.SQLTableColumn{
		Name:    "tblmap",
		Type:    types.SQLColumnTypeVarchar,
		Length:  100,
		Primary: true,
		Order:   3,
	}

	detCol["registers"] = types.SQLTableColumn{
		Name:    "registers",
		Type:    types.SQLColumnTypeInt,
		Primary: false,
		Order:   4,
	}

	tables["detail"] = types.SQLTable{
		Name:    "_bosmarmot_logdet",
		Columns: detCol,
	}

	return tables
}

// getTableDef returns the structure of a given SQL table
func (db *SQLDB) getTableDef(tableName string) (types.SQLTable, error) {
	var table types.SQLTable

	safeTable := safe(tableName)

	found, err := db.findTable(safeTable)
	if err != nil {
		return table, err
	}

	if !found {
		db.Log.Debug("msg", "Error table not found", "value", safeTable)
		return table, errors.New("Error table not found " + safeTable)
	}

	table.Name = safeTable
	query := db.DBAdapter.TableDefinitionQuery(safeTable)

	db.Log.Debug("msg", "QUERY STRUCTURE", "query", clean(query), "value", safeTable)
	rows, err := db.DB.Query(query)
	if err != nil {
		db.Log.Debug("msg", "Error querying table structure", "err", err)
		return table, err
	}
	defer rows.Close()

	columns := make(map[string]types.SQLTableColumn)
	i := 0

	for rows.Next() {
		i++
		var columnName string
		var columnSQLType types.SQLColumnType
		var columnIsPK bool
		var columnLength int
		var column types.SQLTableColumn

		if err = rows.Scan(&columnName, &columnSQLType, &columnIsPK, &columnLength); err != nil {
			db.Log.Debug("msg", "Error scanning table structure", "err", err)
			return table, err
		}

		if _, err = db.DBAdapter.TypeMapping(columnSQLType); err != nil {
			return table, err
		}

		column.Order = i
		column.Name = columnName
		column.Primary = columnIsPK
		column.Type = columnSQLType

		if column.Type == types.SQLColumnTypeVarchar {
			column.Length = columnLength
		} else {
			column.Length = 0
		}

		columns[columnName] = column
	}

	if err = rows.Err(); err != nil {
		db.Log.Debug("msg", "Error during rows iteration", "err", err)
		return table, err
	}

	table.Columns = columns
	return table, nil
}

// alterTable alters the structure of a SQL table
func (db *SQLDB) alterTable(newTable types.SQLTable) error {
	db.Log.Info("msg", "Altering table", "value", newTable.Name)

	safeTable := safe(newTable.Name)

	// current table structure
	currentTable, err := db.getTableDef(safeTable)
	if err != nil {
		return err
	}

	// for each column in the new table structure
	for _, newColumn := range newTable.Columns {
		found := false

		// check if exists in the current table structure
		for _, currentColumn := range currentTable.Columns {
			// if column exists
			if currentColumn.Name == newColumn.Name {
				found = true
				break
			}
		}

		if !found {
			safeCol := safe(newColumn.Name)
			query := db.DBAdapter.AlterColumnQuery(safeTable, safeCol, newColumn.Type)

			db.Log.Debug("msg", "ALTER TABLE", "query", clean(query))
			_, err = db.DB.Exec(query)
			if err != nil {
				if db.DBAdapter.ErrorEquals(err, types.SQLErrorTypeDuplicatedColumn) {
					db.Log.Warn("msg", "Duplicate column", "value", safeCol)
				} else {
					db.Log.Debug("msg", "Error altering table", "err", err)
					return err
				}
			}
		}
	}
	return nil
}

// getSelectQuery builds a select query for a specific SQL table
func (db *SQLDB) getSelectQuery(table types.SQLTable, height string) (string, error) {
	fields := ""

	for _, tableColumn := range table.Columns {
		colName := tableColumn.Name

		if fields != "" {
			fields += ", "
		}
		fields += colName
	}

	if fields == "" {
		return "", errors.New("error table does not contain any fields")
	}

	query := db.DBAdapter.SelectRowQuery(table.Name, fields, height)
	return query, nil
}

// createTable creates a new table in the default schema
func (db *SQLDB) createTable(table types.SQLTable) error {
	db.Log.Info("msg", "Creating Table", "value", table.Name)

	safeTable := safe(table.Name)

	// sort columns
	sortedColumns := make([]types.SQLTableColumn, len(table.Columns))
	for _, tableColumn := range table.Columns {
		sortedColumns[tableColumn.Order-1] = tableColumn
	}

	query := db.DBAdapter.CreateTableQuery(safeTable, sortedColumns)
	if query == "" {
		db.Log.Debug("msg", "empty CREATE TABLE query")
		return errors.New("empty CREATE TABLE query")
	}

	// create table
	db.Log.Debug("msg", "CREATE TABLE", "query", clean(query))
	_, err := db.DB.Exec(query)
	if err != nil {
		if db.DBAdapter.ErrorEquals(err, types.SQLErrorTypeDuplicatedColumn) {
			db.Log.Warn("msg", "Duplicate table", "value", safeTable)
			return nil

		} else if db.DBAdapter.ErrorEquals(err, types.SQLErrorTypeInvalidType) {
			db.Log.Debug("msg", "Error creating table, invalid datatype", "err", err)
			return err

		}
		db.Log.Debug("msg", "Error creating table", "err", err)
		return err
	}

	return nil
}

// getBlockTables return all SQL tables that had been involved
// in a given batch transaction for a specific block id
func (db *SQLDB) getBlockTables(block string) (types.EventTables, error) {
	tables := make(types.EventTables)

	query := db.DBAdapter.SelectLogQuery()
	db.Log.Debug("msg", "QUERY LOG", "query", clean(query), "value", block)
	rows, err := db.DB.Query(query, block)
	if err != nil {
		db.Log.Debug("msg", "Error querying log", "err", err)
		return tables, err
	}
	defer rows.Close()

	for rows.Next() {
		var tblMap string
		var tblName string
		var table types.SQLTable

		err = rows.Scan(&tblName, &tblMap)
		if err != nil {
			db.Log.Debug("msg", "Error scanning table structure", "err", err)
			return tables, err
		}

		err = rows.Err()
		if err != nil {
			db.Log.Debug("msg", "Error scanning table structure", "err", err)
			return tables, err
		}

		table, err = db.getTableDef(tblName)
		if err != nil {
			return tables, err
		}

		tables[tblMap] = table
	}
	return tables, nil
}

// getUpsertParams builds parameters in preparation for an upsert query
func getUpsertParams(upsertQuery types.UpsertQuery, row types.EventDataRow) ([]interface{}, string, error) {
	pointers := make([]interface{}, upsertQuery.Length)
	containers := make([]sql.NullString, upsertQuery.Length)

	for colName, col := range upsertQuery.Columns {
		// interface=data
		pointers[col.InsPosition] = &containers[col.InsPosition]
		if col.UpdPosition > 0 {
			pointers[col.UpdPosition] = &containers[col.UpdPosition]
		}

		// build parameter list
		if value, ok := row[colName]; ok {
			// column found (not null)
			containers[col.InsPosition] = sql.NullString{String: value, Valid: true}

			// if column is not PK
			if col.UpdPosition > 0 {
				containers[col.UpdPosition] = sql.NullString{String: value, Valid: true}
			}
		} else if col.UpdPosition > 0 {
			// column not found and is not PK (null)
			containers[col.InsPosition].Valid = false
			containers[col.UpdPosition].Valid = false
		} else {
			// column not found is PK
			return nil, "", fmt.Errorf("error null primary key for column %s", colName)
		}
	}

	return pointers, fmt.Sprintf("%v", containers), nil
}

// clean queries from tabs, spaces  and returns
func clean(parameter string) string {
	replacer := strings.NewReplacer("\n", " ", "\t", "")
	return replacer.Replace(parameter)
}

// safe sanitizes a parameter
func safe(parameter string) string {
	replacer := strings.NewReplacer(";", "", ",", "")
	return replacer.Replace(parameter)
}
