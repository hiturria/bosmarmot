package types

// SQLTable contains the structure of a SQL table,
type SQLTable struct {
	Name    string
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
