package types

// SQLColumnType to store generic SQL column types
type SQLColumnType int

// generic SQL column types
const (
	SQLColumnTypeBool SQLColumnType = iota
	SQLColumnTypeByteA
	SQLColumnTypeInt
	SQLColumnTypeSerial
	SQLColumnTypeText
	SQLColumnTypeVarchar
	SQLColumnTypeTimeStamp
)
