package sqlsol

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/monax/bosmarmot/vent/types"
)

// Parser contains EventTable definition
type Parser struct {
	// maps event names to tables
	Tables types.EventTables
}

// NewParser receives a sqlsol event configuration stream
// and returns a pointer to a filled parser structure
func NewParser(byteValue []byte) (*Parser, error) {
	tables, err := mapToTable(byteValue)
	if err != nil {
		return nil, err
	}

	return &Parser{
		Tables: tables,
	}, nil
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

// mapToTable gets a sqlsol event configuration stream,
// parses contents, maps event types to SQL column types
// and fills Event table structure with table and columns info
func mapToTable(byteValue []byte) (map[string]types.SQLTable, error) {
	tables := make(map[string]types.SQLTable)
	eventsDefinition := []types.EventDefinition{}

	// parses json config stream
	if err := json.Unmarshal(byteValue, &eventsDefinition); err != nil {
		return nil, err
	}

	// obtain global SQL table columns to add to columns definition map
	globalColumns := getGlobalColumns()
	globalColumnsLength := len(globalColumns)

	for _, eventDef := range eventsDefinition {
		// validate json structure
		if err := eventDef.Validate(); err != nil {
			return nil, err
		}

		// if it is an event
		if eventDef.Event.Type == "event" {
			// build columns mapping
			columns := make(map[string]types.SQLTableColumn)

			for i, eventInput := range eventDef.Event.Inputs {
				if col, ok := eventDef.Columns[eventInput.Name]; ok {

					sqlType, sqlTypeLength, err := getSQLType(eventInput.Type)
					if err != nil {
						return nil, err
					}

					columns[eventInput.Name] = types.SQLTableColumn{
						Name:    col.Name,
						Type:    sqlType,
						Length:  sqlTypeLength,
						Primary: col.Primary,
						Order:   i + (globalColumnsLength + 1),
					}
				}
			}

			// add global columns to column definition
			for k, v := range globalColumns {
				columns[k] = v
			}

			tables[eventDef.Event.Name] = types.SQLTable{
				Name:    strings.ToLower(eventDef.TableName),
				Columns: columns,
			}
		}
	}

	return tables, nil
}

// getSQLType maps event input types with corresponding
// SQL column types
func getSQLType(eventInputType string) (types.SQLColumnType, int, error) {
	switch strings.ToLower(eventInputType) {
	case types.EventInputTypeInt, types.EventInputTypeUInt:
		return types.SQLColumnTypeInt, 0, nil
	case types.EventInputTypeAddress, types.EventInputTypeBytes:
		return types.SQLColumnTypeVarchar, 100, nil
	case types.EventInputTypeBool:
		return types.SQLColumnTypeBool, 0, nil
	case types.EventInputTypeString:
		return types.SQLColumnTypeText, 0, nil
	default:
		return 0, 0, fmt.Errorf("getSQLType: don't know how to map eventInputType: %s ", eventInputType)
	}
}

// getGlobalColumns returns global columns for event table structures
// these columns will be part of every SQL event table to relate data with source events
// TODO:
// check if all this is necessary, or what is really needed
// depending on how data is to be retrieved from event tables
// is the way we have to build tables, logs
// and relationship between them
func getGlobalColumns() map[string]types.SQLTableColumn {
	globalColumns := make(map[string]types.SQLTableColumn)

	globalColumns["height"] = types.SQLTableColumn{
		Name:    "height",
		Type:    types.SQLColumnTypeVarchar,
		Length:  100,
		Primary: false,
		Order:   1,
	}

	globalColumns["txHash"] = types.SQLTableColumn{
		Name:    "txhash",
		Type:    types.SQLColumnTypeByteA,
		Primary: false,
		Order:   2,
	}

	globalColumns["index"] = types.SQLTableColumn{
		Name:    "index",
		Type:    types.SQLColumnTypeInt,
		Primary: false,
		Order:   3,
	}

	globalColumns["eventType"] = types.SQLTableColumn{
		Name:    "eventtype",
		Type:    types.SQLColumnTypeVarchar,
		Length:  100,
		Primary: false,
		Order:   4,
	}

	globalColumns["eventName"] = types.SQLTableColumn{
		Name:    "eventname",
		Type:    types.SQLColumnTypeVarchar,
		Length:  100,
		Primary: false,
		Order:   5,
	}

	return globalColumns
}
