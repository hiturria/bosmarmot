package types

// defined SQL column types
//**********************************************************************************************
//*                                                                                            *
//*     WARNING: THIS TYPES MUST NOT BE CHANGED BECAUSE ARE RETURNED AS VALUES IN SQL QUERYs   *
//*                                                                                            *
//**********************************************************************************************
const (
	SQLColumnTypeBool             = "BOOLEAN"
	SQLColumnTypeByteA            = "BYTEA"
	SQLColumnTypeInt              = "INTEGER"
	SQLColumnTypeText             = "TEXT"
	SQLColumnTypeSerial           = "SERIAL"
	SQLColumnTypeTimeStamp        = "TIMESTAMP"
	SQLColumnTypeDefaultTimeStamp = "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
	SQLColumnTypeVarchar100       = "VARCHAR(100)"
)
