package sqldb

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/lib/pq"
	"github.com/monax/bosmarmot/vent/sqldb/adapters"
	"github.com/monax/bosmarmot/vent/types"
)

// PostgreSQL specific error codes
const (
	errDupTable        = "42P07"
	errDupColumn       = "42701"
	errUndefinedTable  = "42P01"
	errUndefinedColumn = "42703"
	errInvalidType     = "42704"
)

type upsertQuery struct {
	query  string
	length int
	cols   map[string]upsertCols
}

type upsertCols struct {
	numeric bool
	posIns  int
	posUpd  int
}

// findDefaultSchema checks if the default schema exists in SQL database
func (db *SQLDB) findDefaultSchema() (bool, error) {
	var found bool

	query := db.DBAdapter.GetQueryFindSchema()

	db.Log.Debug("msg", "FIND SCHEMA", "query", adapters.Clean(query))
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

	query := db.DBAdapter.GetQueryCreateSchema()

	db.Log.Debug("msg", "CREATE SCHEMA", "query", adapters.Clean(query))
	_, err := db.DB.Exec(query)
	if err != nil {
		if db.DBAdapter.ErrorIsDupSchema(err) {
			db.Log.Warn("msg", "Duplicate schema", "value", db.Schema)
			return nil
		}
	}
	return err
}

// findTable checks if a table exists in the default schema
func (db *SQLDB) findTable(tableName string) (bool, error) {
	found := false
	safeTable := adapters.Safe(tableName)
	query := db.DBAdapter.GetQueryFindTable(safeTable)

	db.Log.Debug("msg", "FIND TABLE", "query", adapters.Clean(query), "value", safeTable)
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
func  (db *SQLDB) getLogTableDef() types.EventTables {
	tables := make(types.EventTables)
	logCol := make(map[string]types.SQLTableColumn)

	logCol["id"] = types.SQLTableColumn{
		Name:    "id",
		Type:    db.DBAdapter.SQLDataType(types.SQLColumnTypeSerial),
		Primary: true,
		Order:   1,
	}

	logCol["timestamp"] = types.SQLTableColumn{
		Name:    "timestamp",
		Type:    db.DBAdapter.SQLDataType(types.SQLColumnTypeDefaultTimeStamp),
		Primary: false,
		Order:   2,
	}

	logCol["registers"] = types.SQLTableColumn{
		Name:    "registers",
		Type:    db.DBAdapter.SQLDataType(types.SQLColumnTypeInt),
		Primary: false,
		Order:   3,
	}

	logCol["height"] = types.SQLTableColumn{
		Name:    "height",
		Type:    db.DBAdapter.SQLDataType(types.SQLColumnTypeVarchar100),
		Primary: false,
		Order:   4,
	}

	detCol := make(map[string]types.SQLTableColumn)

	detCol["id"] = types.SQLTableColumn{
		Name:    "id",
		Type:    db.DBAdapter.SQLDataType(types.SQLColumnTypeInt),
		Primary: true,
		Order:   1,
	}

	detCol["tableName"] = types.SQLTableColumn{
		Name:    "tblname",
		Type:    db.DBAdapter.SQLDataType(types.SQLColumnTypeVarchar100),
		Primary: true,
		Order:   2,
	}

	detCol["tableMap"] = types.SQLTableColumn{
		Name:    "tblmap",
		Type:    db.DBAdapter.SQLDataType(types.SQLColumnTypeVarchar100),
		Primary: true,
		Order:   3,
	}

	detCol["registers"] = types.SQLTableColumn{
		Name:    "registers",
		Type:    db.DBAdapter.SQLDataType(types.SQLColumnTypeInt),
		Primary: false,
		Order:   4,
	}

	log := types.SQLTable{
		Name:    "_bosmarmot_log",
		Columns: logCol,
	}

	det := types.SQLTable{
		Name:    "_bosmarmot_logdet",
		Columns: detCol,
	}

	tables["log"] = log
	tables["detail"] = det

	return tables
}


//-------------------------------------------------------------------------------------------------------------------
// getTableDef returns the structure of a given SQL table
func (db *SQLDB) getTableDef(tableName string) (types.SQLTable, error) {
	var table types.SQLTable

	safeTable:=adapters.Safe(tableName)

	found, err := db.findTable(safeTable)
	if err != nil {
		return table, err
	}

	if !found {
		db.Log.Debug("msg", "Error table not found", "value", safeTable)
		return table, errors.New("Error table not found " + safeTable)
	}

	table.Name = safeTable
	query := db.DBAdapter.GetQueryTableDefinition(safeTable)

	db.Log.Debug("msg", "QUERY STRUCTURE", "query", adapters.Clean(query), "value", safeTable)
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
		var columnSQLType string
		var columnIsPK bool
		var columnDescription string

		var column types.SQLTableColumn

		if err := rows.Scan(&columnName, &columnSQLType, &columnIsPK, &columnDescription); err != nil {
			db.Log.Debug("msg", "Error scanning table structure", "err", err)
			return table, err
		}

		if err := rows.Err(); err != nil {
			db.Log.Debug("msg", "Error scanning table structure", "err", err)
			return table, err
		}

		column.Order = i
		column.Name = columnName
		column.Primary = columnIsPK
		column.Type = columnSQLType

		//TODO: check for valid type
		//if db.DBAdapter.SQLDataType(columnSQLType){
		//	//
		//}


		columns[columnDescription] = column
	}

	table.Columns = columns
	return table, nil
}

// createTable creates a new table in the default schema
func (db *SQLDB) createTable(table types.SQLTable) error {
	db.Log.Info("msg", "Creating Table", "value", table.Name)

	safeTable := adapters.Safe(table.Name)

	// sort columns and create comments
	sortedColumns := make([]types.SQLTableColumn, len(table.Columns))
	comments := make([]string, len(table.Columns))

	for comment, tableColumn := range table.Columns {
		sortedColumns[tableColumn.Order-1] = tableColumn
		comments[tableColumn.Order-1] = fmt.Sprintf("COMMENT ON COLUMN %s.%s.%s IS '%s';", db.Schema, safeTable, adapters.Safe(tableColumn.Name), comment)
	}

	// build query
	columnsDef := ""
	primaryKey := ""

	for _, tableColumn := range sortedColumns {
		colName := adapters.Safe(tableColumn.Name)
		colType := adapters.Safe(tableColumn.Type)

		if columnsDef != "" {
			columnsDef += ", "
		}

		columnsDef += fmt.Sprintf("%s %s", colName, colType)

		if tableColumn.Primary {
			columnsDef += " NOT NULL"
			if primaryKey != "" {
				primaryKey += ", "
			}
			primaryKey += colName
		}
	}

	query := fmt.Sprintf("CREATE TABLE %s.%s (%s", db.Schema, safeTable, columnsDef)
	if primaryKey != "" {
		query += "," + fmt.Sprintf("CONSTRAINT %s_pkey PRIMARY KEY (%s)", safeTable, primaryKey)
	}
	query += ");"

	// create table
	db.Log.Debug("msg", "CREATE TABLE", "query", adapters.Clean(query))
	_, err := db.DB.Exec(query)
	if err != nil {
		if err, ok := err.(*pq.Error); ok {
			switch err.Code {
			case errDupTable:
				db.Log.Warn("msg", "Duplicate table", "value", safeTable)
				return nil

			case errInvalidType:
				db.Log.Debug("msg", "Error creating table, invalid datatype", "err", err)
				return err
			}
		}
		db.Log.Debug("msg", "Error creating table", "err", err)
		return err
	}

	// comment on table and columns
	for _, query := range comments {
		db.Log.Debug("msg", "COMMENT COLUMN", "query", adapters.Clean(query))
		_, err = db.DB.Exec(query)
		if err != nil {
			db.Log.Debug("msg", "Error commenting column", "err", err)
			return err
		}
	}

	return nil
}

// getBlockTables return all SQL tables that had been involved
// in a given batch transaction for a specific block id
func (db *SQLDB) getBlockTables(block string) (types.EventTables, error) {
	tables := make(types.EventTables)

	query := fmt.Sprintf(`
		SELECT
			tblname,
			tblmap
		FROM
			%s._bosmarmot_log l
			INNER JOIN %s._bosmarmot_logdet d ON l.id = d.id
		WHERE
			height = $1;
	`, db.Schema, db.Schema)

	db.Log.Debug("msg", "QUERY LOG", "query", adapters.Clean(query), "value", block)
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

// getTableQuery builds a select query for a specific SQL table
func getTableQuery(schema string, table types.SQLTable, height string) (string, error) {
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

	query := "SELECT " + fields + " FROM " + schema + "." + table.Name + " WHERE height='" + height + "';"
	return query, nil
}

// alterTable alters the structure of a SQL table
func (db *SQLDB) alterTable(newTable types.SQLTable) error {
	db.Log.Info("msg", "Altering table", "value", newTable.Name)

	safeTable := adapters.Safe(newTable.Name)

	// current table structure in PGSQL
	currentTable, err := db.getTableDef(safeTable)
	if err != nil {
		return err
	}

	// for each column in the new table structure
	for comment, newColumn := range newTable.Columns {
		found := false

		// check if exists in the current table structure
		for _, curretColum := range currentTable.Columns {
			if curretColum.Name == newColumn.Name {
				//if exists
				found = true
				break
			}
		}

		if !found {
			safeCol := adapters.Safe(newColumn.Name)
			query := fmt.Sprintf("ALTER TABLE %s.%s ADD COLUMN %s %s;", db.Schema, safeTable, safeCol, adapters.Safe(newColumn.Type))

			db.Log.Debug("msg", "ALTER TABLE", "query", adapters.Clean(query))
			_, err = db.DB.Exec(query)
			if err != nil {
				if err, ok := err.(*pq.Error); ok {
					if err.Code == errDupColumn {
						db.Log.Warn("msg", "Duplicate column", "value", safeCol)
					} else {
						db.Log.Debug("msg", "Error altering table", "err", err)
						return err
					}
				} else {
					db.Log.Debug("msg", "Error altering table", "err", err)
					return err
				}
			}

			query = fmt.Sprintf("COMMENT ON COLUMN %s.%s.%s IS '%s';", db.Schema, safeTable, safeCol, comment)
			db.Log.Debug("msg", "COMMENT COLUMN", "query", adapters.Clean(query))
			_, err = db.DB.Exec(query)
			if err != nil {
				db.Log.Debug("msg", "Error commenting column", "err", err)
				return err
			}
		}
	}
	return nil
}

// getUpsertQuery builds a query for upsert
func getUpsertQuery(schema string, table types.SQLTable) upsertQuery {
	columns := ""
	insValues := ""
	updValues := ""
	cols := len(table.Columns)
	nKeys := 0
	cKey := 0

	var uQuery upsertQuery
	uQuery.cols = make(map[string]upsertCols)

	i := 0

	for _, tableColumn := range table.Columns {
		isNum := adapters.IsNumeric(tableColumn.Type)
		safeCol := adapters.Safe(tableColumn.Name)
		cKey = 0
		i++

		// INSERT INTO TABLE (*columns).........
		if columns != "" {
			columns += ", "
			insValues += ", "
		}
		columns += safeCol
		insValues += "$" + fmt.Sprintf("%d", i)

		if !tableColumn.Primary {
			cKey = cols + nKeys
			nKeys++

			// INSERT........... ON CONFLICT......DO UPDATE (*updValues)
			if updValues != "" {
				updValues += ", "
			}
			updValues += safeCol + " = $" + fmt.Sprintf("%d", cKey+1)
		}

		uQuery.cols[safeCol] = upsertCols{
			numeric: isNum,
			posIns:  i - 1,
			posUpd:  cKey,
		}
	}
	uQuery.length = cols + nKeys

	safeTable := adapters.Safe(table.Name)
	query := fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES (%s) ", schema, safeTable, columns, insValues)

	if nKeys != 0 {
		query += fmt.Sprintf("ON CONFLICT ON CONSTRAINT %s_pkey DO UPDATE SET ", safeTable)
		query += updValues
	} else {
		query += fmt.Sprintf("ON CONFLICT ON CONSTRAINT %s_pkey DO NOTHING", safeTable)
	}
	query += ";"

	uQuery.query = query
	return uQuery
}

// getUpsertParams builds parameters in preparation for an upsert query
func getUpsertParams(uQuery upsertQuery, row types.EventDataRow) ([]interface{}, string, error) {
	pointers := make([]interface{}, uQuery.length)
	containers := make([]sql.NullString, uQuery.length)

	for colName, col := range uQuery.cols {
		// interface=data
		pointers[col.posIns] = &containers[col.posIns]
		if col.posUpd > 0 {
			pointers[col.posUpd] = &containers[col.posUpd]
		}

		// build parameter list
		if value, ok := row[colName]; ok {
			//column found (not null)
			containers[col.posIns] = sql.NullString{String: value, Valid: true}

			//if column is not PK
			if col.posUpd > 0 {
				containers[col.posUpd] = sql.NullString{String: value, Valid: true}
			}

		} else if col.posUpd > 0 {
			//column not found and is not PK (null)
			containers[col.posIns].Valid = false
			containers[col.posUpd].Valid = false

		} else {
			//column not found is PK
			return nil, "", errors.New("Error null primary key for column " + colName)
		}
	}

	return pointers, fmt.Sprintf("%v", containers), nil
}
