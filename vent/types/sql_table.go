package types

// SQLTable contains the structure of a SQL table,
type SQLTable struct {
	Name    string
	Filter  string
	Columns map[string]SQLTableColumn
}

// SQLTableColumn contains the definition of a SQL table column,
// the Order is given to be able to sort the columns to be created
type SQLTableColumn struct {
	Name          string
	Type          SQLColumnType
	EVMType       string
	Length        int
	Primary       bool
	BytesToString bool
	Order         int
}

// UpsertDeleteQuery contains query and values to upsert or delete row data
type UpsertDeleteQuery struct {
	Query    string
	Values   string
	Pointers []interface{}
}

// SQL log & dictionary tables
const SQLLogTableName = "_vent_log"
const SQLDictionaryTableName = "_vent_dictionary"
const SQLBlockTableName = "_vent_block"
const SQLTxTableName = "_vent_tx"

// fixed sql column names in tables
const (
	// log
	SQLColumnLabelId          = "_id"
	SQLColumnLabelTimeStamp   = "_timestamp"
	SQLColumnLabelTableName   = "_tablename"
	SQLColumnLabelEventName   = "_eventname"
	SQLColumnLabelEventFilter = "_eventfilter"
	SQLColumnLabelHeight      = "_height"
	SQLColumnLabelTxHash      = "_txhash"
	SQLColumnLabelAction      = "_action"
	SQLColumnLabelDataRow     = "_datarow"
	SQLColumnLabelSqlStmt     = "_sqlstmt"
	SQLColumnLabelSqlValues   = "_sqlvalues"

	// dictionary
	SQLColumnLabelColumnName   = "_columnname"
	SQLColumnLabelColumnType   = "_columntype"
	SQLColumnLabelColumnLength = "_columnlength"
	SQLColumnLabelPrimaryKey   = "_primarykey"
	SQLColumnLabelColumnOrder  = "_columnorder"

	// context
	SQLColumnLabelIndex       = "_index"
	SQLColumnLabelEventType   = "_eventtype"
	SQLColumnLabelBlockHeader = "_blockheader"
	SQLColumnLabelTxType      = "_txtype"
	SQLColumnLabelEnvelope    = "_envelope"
	SQLColumnLabelEvents      = "_events"
	SQLColumnLabelResult      = "_result"
	SQLColumnLabelReceipt     = "_receipt"
	SQLColumnLabelException   = "_exception"
)

// labels for column mapping
const (
	// event related
	EventNameLabel = "eventName"
	EventTypeLabel = "eventType"

	// block related
	BlockHeightLabel = "height"
	BlockHeaderLabel = "blockHeader"
	BlockTxExecLabel = "txExecutions"

	// transaction related
	TxTxTypeLabel    = "txType"
	TxTxHashLabel    = "txHash"
	TxIndexLabel     = "index"
	TxEnvelopeLabel  = "envelope"
	TxEventsLabel    = "events"
	TxResultLabel    = "result"
	TxReceiptLabel   = "receipt"
	TxExceptionLabel = "exception"
)
