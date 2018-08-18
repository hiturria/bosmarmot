package adapters

type UpsertQuery struct {
	Query   string
	Length  int
	Columns map[string]UpsertColumn
}

type UpsertColumn struct {
	IsNumeric   bool
	InsPosition int
	UpdPosition int
}
