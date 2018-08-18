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
	Type    string
	Length  int
	Primary bool
	Order   int
}
