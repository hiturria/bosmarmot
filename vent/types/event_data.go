package types

// CRUDAction generic type
type CRUDAction int

const (
	ActionDelete CRUDAction = iota
	ActionUpsert
	ActionRead
)

// EventData contains data for each block of events
// already mapped to SQL columns & tables
// Tables map key is the table name
type EventData struct {
	Block  string
	Tables map[string]EventDataTable
}

// EventDataTable is an array of rows
type EventDataTable []EventDataRow

// EventDataRow contains each SQL column name and a corresponding value to upsert
// map key is the column name and map value is the given column value
// if Action == 'delete' then the row has to be deleted
type EventDataRow struct {
	Action  CRUDAction
	RowData map[string]interface{}
}
