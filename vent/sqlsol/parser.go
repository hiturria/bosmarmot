package sqlsol

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/hyperledger/burrow/execution/evm/abi"
	"github.com/monax/bosmarmot/vent/types"
	"github.com/pkg/errors"
)

// Parser contains EventTable, Event & Abi specifications
type Parser struct {
	Tables    types.EventTables
	EventSpec types.EventSpec
	AbiSpec   *abi.AbiSpec
}

// NewParser receives a sqlsol event configuration stream
// and returns a pointer to a filled parser structure
func NewParser(byteValue []byte) (*Parser, error) {
	tables, eventSpec, err := mapToTable(byteValue)
	if err != nil {
		return nil, err
	}

	abiSpecInput := []types.Event{}

	for _, spec := range eventSpec {
		abiSpecInput = append(abiSpecInput, spec.Event)
	}

	abiSpecInputBytes, err := json.Marshal(abiSpecInput)
	if err != nil {
		return nil, errors.Wrap(err, "Error generating abi spec input")
	}

	abiSpec, err := abi.ReadAbiSpec(abiSpecInputBytes)
	if err != nil {
		return nil, errors.Wrap(err, "Error creating abi spec")
	}

	return &Parser{
		Tables:    tables,
		EventSpec: eventSpec,
		AbiSpec:   abiSpec,
	}, nil
}

// GetAbiSpec returns the abi specification
func (p *Parser) GetAbiSpec() *abi.AbiSpec {
	return p.AbiSpec
}

// GetEventSpec returns the event specification
func (p *Parser) GetEventSpec() types.EventSpec {
	return p.EventSpec
}

// GetTables returns the event tables structures
func (p *Parser) GetTables() types.EventTables {
	return p.Tables
}

// GetTableName receives an eventName and returns the mapping tableName
func (p *Parser) GetTableName(eventName string) (string, error) {
	if table, ok := p.Tables[eventName]; ok {
		return table.Name, nil
	}

	return "", fmt.Errorf("GetTableName: eventName does not exists as a table in SQL table structure: %s ", eventName)
}

// GetColumnName receives an event Name and item and returns the mapping columnName
func (p *Parser) GetColumnName(eventName, eventItem string) (string, error) {
	if table, ok := p.Tables[eventName]; ok {
		if column, ok := table.Columns[eventItem]; ok {
			return column.Name, nil
		}
		return "", fmt.Errorf("GetColumnName: eventItem does not exists as a column in SQL table structure: %s ", eventItem)
	}

	return "", fmt.Errorf("GetColumnName: eventName does not exists as a table in SQL table structure: %s ", eventName)
}

// GetColumn receives an event Name and item and returns the mapping column with associated info
func (p *Parser) GetColumn(eventName, eventItem string) (types.SQLTableColumn, error) {
	column := types.SQLTableColumn{}

	if table, ok := p.Tables[eventName]; ok {
		if column, ok = table.Columns[eventItem]; ok {
			return column, nil
		}
		return column, fmt.Errorf("GetColumn: eventItem does not exists as a column in SQL table structure: %s ", eventItem)
	}

	return column, fmt.Errorf("GetColumn: eventName does not exists as a table in SQL table structure: %s ", eventName)
}

// SetTableName updates TableName element in structure
func (p *Parser) SetTableName(eventName, tableName string) error {
	if table, ok := p.Tables[eventName]; ok {
		table.Name = strings.ToLower(tableName)
		p.Tables[eventName] = table
		return nil
	}

	return fmt.Errorf("SetTableName: eventName does not exists as a table in SQL table structure: %s ", eventName)
}

// mapToTable gets a sqlsol specification stream,
// parses contents, maps event types to SQL column types
// and fills Event tables structures with table and columns info
func mapToTable(byteValue []byte) (types.EventTables, types.EventSpec, error) {
	tables := make(types.EventTables)
	eventSpec := types.EventSpec{}

	// parses json specification stream
	if err := json.Unmarshal(byteValue, &eventSpec); err != nil {
		return nil, nil, err
	}

	// obtain global SQL table columns to add to columns definition map
	globalColumns := getGlobalColumns()
	globalColumnsLength := len(globalColumns)

	for _, eventDef := range eventSpec {
		// validate json structure
		if err := eventDef.Validate(); err != nil {
			return nil, nil, err
		}

		// build columns mapping
		columns := make(map[string]types.SQLTableColumn)

		j := 0

		for _, eventInput := range eventDef.Event.Inputs {
			if col, ok := eventDef.Columns[eventInput.Name]; ok {

				sqlType, sqlTypeLength, err := getSQLType(eventInput.Type)
				if err != nil {
					return nil, nil, err
				}

				j++

				columns[eventInput.Name] = types.SQLTableColumn{
					Name:    strings.ToLower(col.Name),
					Type:    sqlType,
					Length:  sqlTypeLength,
					Primary: col.Primary,
					Order:   j + globalColumnsLength,
				}
			}
		}

		// add global columns to columns definition
		for k, v := range globalColumns {
			columns[k] = v
		}

		tables[eventDef.Event.Name] = types.SQLTable{
			Name:    strings.ToLower(eventDef.TableName),
			Filter:  eventDef.Filter,
			Columns: columns,
		}
	}

	// check if there are duplicated table names in structure or
	// duplicated column names (for a given table)
	tblName := make(map[string]int)
	colName := make(map[string]int)

	for _, tbls := range tables {
		tblName[tbls.Name]++
		if tblName[tbls.Name] > 1 {
			return nil, nil, fmt.Errorf("mapToTable: duplicated table name: %s ", tbls.Name)
		}

		for _, cols := range tbls.Columns {
			colName[tbls.Name+cols.Name]++
			if colName[tbls.Name+cols.Name] > 1 {
				return nil, nil, fmt.Errorf("mapToTable: duplicated column name: %s in table %s", cols.Name, tbls.Name)
			}
		}
	}

	return tables, eventSpec, nil
}

// getSQLType maps event input types with corresponding SQL column types
func getSQLType(eventInputType string) (types.SQLColumnType, int, error) {
	if strings.HasPrefix(strings.ToLower(eventInputType), types.EventInputTypeInt) ||
		strings.HasPrefix(strings.ToLower(eventInputType), types.EventInputTypeUInt) {
		return types.SQLColumnTypeNumeric, 0, nil
	}
	if strings.HasPrefix(strings.ToLower(eventInputType), types.EventInputTypeBytes) {
		return types.SQLColumnTypeVarchar, 100, nil
	}
	switch strings.ToLower(eventInputType) {
	case types.EventInputTypeAddress:
		return types.SQLColumnTypeByteA, 0, nil
	case types.EventInputTypeBool:
		return types.SQLColumnTypeBool, 0, nil
	case types.EventInputTypeString:
		return types.SQLColumnTypeText, 0, nil
	default:
		return -1, 0, fmt.Errorf("getSQLType: don't know how to map eventInputType: %s ", eventInputType)
	}
}

// getGlobalColumns returns global columns for event table structures,
// these columns will be part of every SQL event table to relate data with source events
func getGlobalColumns() map[string]types.SQLTableColumn {
	globalColumns := make(map[string]types.SQLTableColumn)

	globalColumns["height"] = types.SQLTableColumn{
		Name:    types.SQLColumnNameHeight,
		Type:    types.SQLColumnTypeVarchar,
		Length:  100,
		Primary: false,
		Order:   1,
	}

	globalColumns["txHash"] = types.SQLTableColumn{
		Name:    types.SQLColumnNameTxHash,
		Type:    types.SQLColumnTypeByteA,
		Primary: false,
		Order:   2,
	}

	globalColumns["index"] = types.SQLTableColumn{
		Name:    types.SQLColumnNameIndex,
		Type:    types.SQLColumnTypeNumeric,
		Length:  0,
		Primary: false,
		Order:   3,
	}

	globalColumns["eventType"] = types.SQLTableColumn{
		Name:    types.SQLColumnNameEventType,
		Type:    types.SQLColumnTypeVarchar,
		Length:  100,
		Primary: false,
		Order:   4,
	}

	globalColumns["eventName"] = types.SQLTableColumn{
		Name:    types.SQLColumnNameEventName,
		Type:    types.SQLColumnTypeVarchar,
		Length:  100,
		Primary: false,
		Order:   5,
	}

	return globalColumns
}
