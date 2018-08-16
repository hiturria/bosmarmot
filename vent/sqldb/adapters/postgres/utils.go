package postgres

import (
	"database/sql"
	"errors"
	"fmt"
	"strconv"

	"github.com/lib/pq"
	"github.com/monax/bosmarmot/vent/sqldb/adapters"
	"github.com/monax/bosmarmot/vent/types"
)

// some PostgreSQL specific error codes
const (
	errDupTable        = "42P07"
	errDupColumn       = "42701"
	errDupSchema       = "42P06"
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
func (db *PostgresAdapter) findDefaultSchema() (bool, error) {
	var found bool

	var query = `
		SELECT
			EXISTS (
				SELECT
					1
				FROM
					pg_catalog.pg_namespace n
				WHERE
					n.nspname = $1
			)
	;`

	db.Log.Debug("msg", "FIND SCHEMA", "query", adapters.Clean(query), "value", db.Schema)
	err := db.DB.QueryRow(query, db.Schema).Scan(&found)
	if err == nil {
		if !found {
			db.Log.Warn("msg", "Schema not found", "value", db.Schema)
		}
	} else {
		db.Log.Debug("msg", "Error searching schema", "err", err)
	}

	return found, err
}

// createDefaultSchema creates the default schema in SQL database
func (db *PostgresAdapter) createDefaultSchema() error {
	db.Log.Info("msg", "Creating schema", "value", db.Schema)

	query := fmt.Sprintf("CREATE SCHEMA %s;", db.Schema)

	db.Log.Debug("msg", "CREATE SCHEMA", "query", adapters.Clean(query), "value", db.Schema)
	_, err := db.DB.Exec(query)
	if err != nil {
		if err, ok := err.(*pq.Error); ok {
			if err.Code == errDupSchema {
				db.Log.Warn("msg", "Duplicate schema", "value", db.Schema)
				return nil
			}
		}
	}
	return err
}

// findTable checks if a table exists in the default schema
func (db *PostgresAdapter) findTable(tableName string) (bool, error) {
	found := false

	query := `
		SELECT
			EXISTS (
				SELECT
					1
				FROM
					pg_catalog.pg_class c
					JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
				WHERE
					n.nspname = $1
					AND c.relname = $2
					AND c.relkind = 'r'
			)
	;`

	safeTable := adapters.Safe(tableName)

	db.Log.Debug("msg", "FIND TABLE", "query", adapters.Clean(query), "value", fmt.Sprintf("%s %s", db.Schema, safeTable))
	err := db.DB.QueryRow(query, db.Schema, safeTable).Scan(&found)

	if err == nil {
		if !found {
			db.Log.Warn("msg", "Table not found", "value", fmt.Sprintf("%s %s", db.Schema, safeTable))
		}
	} else {
		db.Log.Debug("msg", "Error finding table", "err", err)
	}

	return found, err
}

// createTable creates a new table in the default schema
func (db *PostgresAdapter) createTable(table types.SQLTable) error {
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

// getTableDef returns the structure of a given SQL table
func (db *PostgresAdapter) getTableDef(tableName string) (types.SQLTable, error) {
	var table types.SQLTable

	found, err := db.findTable(tableName)
	if err != nil {
		return table, err
	}

	if !found {
		db.Log.Debug("msg", "Error table not found", "value", tableName)
		return table, errors.New("Error table not found " + tableName)
	}

	table.Name = tableName
	//  "col_1";  "integer";          "int4";      0;  "NO";   "dsc1"
	//  "col_2";  "character varying";"varchar";  10;  "YES";  ""
	query := `
  WITH dsc AS (
		SELECT pgd.objsubid,st.schemaname,st.relname,pgd.description
		FROM pg_catalog.pg_statio_all_tables AS st
		INNER JOIN pg_catalog.pg_description pgd ON (pgd.objoid=st.relid)
	)
	SELECT
		c.column_name col,
		c.data_type dtype,
		c.udt_name dtype2,
		COALESCE(c.character_maximum_length, 0) length,
		is_nullable nullable,
		COALESCE(dsc.description, '')  description
	FROM
		information_schema.columns AS c
	LEFT OUTER JOIN
		dsc ON (c.ordinal_position = dsc.objsubid AND c.table_schema = dsc.schemaname AND c.table_name = dsc.relname)
	WHERE
		c.table_schema = $1
		AND c.table_name = $2
	;`

	db.Log.Debug("msg", "QUERY STRUCTURE", "query", adapters.Clean(query), "value", fmt.Sprintf("%s %s", db.Schema, tableName))
	rows, err := db.DB.Query(query, db.Schema, table.Name)
	if err != nil {
		db.Log.Debug("msg", "Error querying table structure", "err", err)
		return table, err
	}
	defer rows.Close()

	columns := make(map[string]types.SQLTableColumn)

	i := 0
	for rows.Next() {
		i++
		var col string
		var dtype string
		var dtype2 string
		var length int
		var nullable string
		var dsc string
		var column types.SQLTableColumn

		if err := rows.Scan(&col, &dtype, &dtype2, &length, &nullable, &dsc); err != nil {
			db.Log.Debug("msg", "Error scanning table structure", "err", err)
			return table, err
		}

		if err := rows.Err(); err != nil {
			db.Log.Debug("msg", "Error scanning table structure", "err", err)
			return table, err
		}

		column.Order = i
		column.Name = col

		if length == 0 {
			column.Type = dtype
		} else {
			column.Type = dtype2 + "(" + strconv.Itoa(length) + ")"
		}

		if nullable == "NO" {
			column.Primary = true
		} else {
			column.Primary = false
		}

		if dsc == "" {
			dsc = col
		}
		columns[dsc] = column
	}

	table.Columns = columns
	return table, nil
}

// getBlockTables return all SQL tables that had been involved
// in a given batch transaction for a specific block id
func (db *PostgresAdapter) getBlockTables(block string) (types.EventTables, error) {
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
		return "", errors.New("Error table does not contain any fields")
	}

	query := "SELECT " + fields + " FROM " + schema + "." + table.Name + " WHERE height='" + height + "';"
	return query, nil
}

// alterTable alters the structure of a SQL table
func (db *PostgresAdapter) alterTable(newTable types.SQLTable) error {
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

// getLogTableDef returns log structures
func getLogTableDef() types.EventTables {
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
		Type:    types.SQLColumnTypeTimeStamp + " DEFAULT CURRENT_TIMESTAMP",
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
		Type:    types.SQLColumnTypeVarchar100,
		Primary: false,
		Order:   4,
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
		Type:    types.SQLColumnTypeVarchar100,
		Primary: true,
		Order:   2,
	}

	detCol["tableMap"] = types.SQLTableColumn{
		Name:    "tblmap",
		Type:    types.SQLColumnTypeVarchar100,
		Primary: true,
		Order:   3,
	}

	detCol["registers"] = types.SQLTableColumn{
		Name:    "registers",
		Type:    types.SQLColumnTypeInt,
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
