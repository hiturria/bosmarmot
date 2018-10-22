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
	SQLColumnLabelEventName   = "_eventname"
	SQLColumnLabelRowCount    = "_rowcount"
	SQLColumnLabelEventFilter = "_eventfilter"
	SQLColumnLabelHeight      = "_height"

	// common
	SQLColumnLabelTableName = "_tablename"

	// dictionary
	SQLColumnLabelColumnName   = "_columnname"
	SQLColumnLabelColumnType   = "_columntype"
	SQLColumnLabelColumnLength = "_columnlength"
	SQLColumnLabelPrimaryKey   = "_primarykey"
	SQLColumnLabelColumnOrder  = "_columnorder"

	// context
	SQLColumnLabelIndex       = "_index"
	SQLColumnLabelTxHash      = "_txhash"
	SQLColumnLabelEventType   = "_eventtype"
	SQLColumnLabelBlockHeader = "_blockheader"
	SQLColumnLabelTxType      = "_txtype"
	SQLColumnLabelTxExec      = "_txexecutions"
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
