package types

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
type EventDataRow map[string]interface{}
