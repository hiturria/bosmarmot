package adapters

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
