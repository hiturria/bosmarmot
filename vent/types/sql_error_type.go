package types

// SQLError to store generic SQL error types
type SQLError int

// generic SQL errors
const (
	ErrDuplicatedSchema SQLError = iota
	ErrDuplicatedColumn
	ErrDuplicatedTable
	ErrInvalidType
	ErrUndefinedTable
	ErrUndefinedColumn
	ErrGenericSQL
)
