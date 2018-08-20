package types

// SQL specific error codes
const (
	ErrDupSchema       = "DUP_SCH"
	ErrDupColumn       = "DUP_COL"
	ErrDupTable        = "DUP_TBL"
	ErrInvalidType     = "INV_TYP"
	ErrUndefinedTable  = "UND_TBL"
	ErrUndefinedColumn = "UND_COL"
	ErrGenericSQL      = "GENERIC"
)
