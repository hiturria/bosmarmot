package types

// SQLTable contains the structure of a SQL table,
type SQLTable struct {
	Name    string
	Filter  string
	Columns map[string]SQLTableColumn
}

// SQLTableColumn contains the definition of a SQL table column,
// the Order is given to be able to sort the columns to be created
type SQLTableColumn struct {
	Name    string
	Type    SQLColumnType
	Length  int
	Primary bool
	Order   int
}

// UpsertQuery contains generic query to upsert row data
type UpsertQuery struct {
	Query   string
	Length  int
	Columns map[string]UpsertColumn
}

// UpsertColumn contains info about a specific column to be upserted
type UpsertColumn struct {
	IsNumeric   bool
	InsPosition int
	UpdPosition int
}

// SQL log & dictionary tables
const SQLLogTableName = "_vent_log"
const SQLDictionaryTableName = "_vent_dictionary"

// defined fixed sql column names in log table
const (
	//log
	SQLColumnNameId          = "_id"
	SQLColumnNameTimeStamp   = "_timestamp"
	SQLColumnNameEventName   = "_eventname"
	SQLColumnNameRowCount    = "_rowcount"
	SQLColumnNameEventFilter = "_eventfilter"
	SQLColumnNameHeight      = "_height"

	//common
	SQLColumnNameTableName = "_tablename"

	//dictionary
	SQLColumnNameColumnName   = "_columnname"
	SQLColumnNameColumnType   = "_columntype"
	SQLColumnNameColumnLength = "_columnlength"
	SQLColumnNamePrimaryKey   = "_primarykey"
	SQLColumnNameColumnOrder  = "_columnorder"

	//auxiliary not in DB
	SQLColumnNameIndex     = "_index"
	SQLColumnNameTxHash    = "_txhash"
	SQLColumnNameEventType = "_eventtype"
)
