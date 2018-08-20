package types

type SQLError int

const (
	ErrDuplicatedSchema SQLError = iota
	ErrDuplicatedColumn
	ErrDuplicatedTable
	ErrInvalidType
	ErrUndefinedTable
	ErrUndefinedColumn
	ErrGenericSQL
)
