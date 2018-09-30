package sqlsol

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
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

// NewParserFromBytes creates a Parser from a stream of bytes
func NewParserFromBytes(bytes []byte) (*Parser, error) {
	eventSpec := types.EventSpec{}

	if err := json.Unmarshal(bytes, &eventSpec); err != nil {
		return nil, errors.Wrap(err, "Error unmarshalling eventSpec")
	}

	return NewParserFromEventSpec(eventSpec)
}

// NewParserFromFile creates a Parser from a file
func NewParserFromFile(file string) (*Parser, error) {
	bytes, err := readFile(file)
	if err != nil {
		return nil, errors.Wrap(err, "Error reading eventSpec file")
	}

	return NewParserFromBytes(bytes)
}

// NewParserFromFolder creates a Parser from a folder containing spec files
func NewParserFromFolder(folder string) (*Parser, error) {
	eventSpec := types.EventSpec{}

	err := filepath.Walk(folder, func(path string, _ os.FileInfo, err error) error {
		if err == nil && filepath.Ext(path) == ".json" {
			bytes, err := readFile(path)
			if err != nil {
				return errors.Wrap(err, "Error reading eventSpec file")
			}

			fileEventSpec := types.EventSpec{}

			if err := json.Unmarshal(bytes, &fileEventSpec); err != nil {
				return errors.Wrap(err, "Error unmarshalling eventSpec")
			}

			eventSpec = append(eventSpec, fileEventSpec...)
		}

		return nil
	})
	if err != nil {
		return nil, errors.Wrap(err, "Error reading eventSpec folder")
	}

	return NewParserFromEventSpec(eventSpec)
}

// NewParserFromEventSpec receives a sqlsol event specification
// and returns a pointer to a filled parser structure
// that contains event types mapped to SQL column types
// and Event tables structures with table and columns info
func NewParserFromEventSpec(eventSpec types.EventSpec) (*Parser, error) {
	// builds abi information from specification
	tables := make(types.EventTables)
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

	// obtain global SQL table columns to add to columns definition map
	globalColumns := getGlobalColumns()
	globalColumnsLength := len(globalColumns)

	for _, eventDef := range eventSpec {
		// validate json structure
		if err := eventDef.Validate(); err != nil {
			return nil, err
		}

		// build columns mapping
		columns := make(map[string]types.SQLTableColumn)
		j := 0

		if abiEvent, ok := abiSpec.Events[eventDef.Event.Name]; ok {
			for _, eventInput := range abiEvent.Inputs {
				if col, ok := eventDef.Columns[eventInput.Name]; ok {
					sqlType, sqlTypeLength, err := getSQLType(eventInput)
					if err != nil {
						return nil, err
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
	}

	// check if there are duplicated table names in structure or
	// duplicated column names (for a given table)
	tblName := make(map[string]int)
	colName := make(map[string]int)

	for _, tbls := range tables {
		tblName[tbls.Name]++
		if tblName[tbls.Name] > 1 {
			return nil, fmt.Errorf("Duplicated table name: %s ", tbls.Name)
		}

		for _, cols := range tbls.Columns {
			colName[tbls.Name+cols.Name]++
			if colName[tbls.Name+cols.Name] > 1 {
				return nil, fmt.Errorf("Duplicated column name: %s in table %s", cols.Name, tbls.Name)
			}
		}
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

// readFile opens a given file and reads it contents into a stream of bytes
func readFile(file string) ([]byte, error) {
	theFile, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer theFile.Close()

	byteValue, err := ioutil.ReadAll(theFile)
	if err != nil {
		return nil, err
	}

	return byteValue, nil
}

// getSQLType maps event input types with corresponding SQL column types
// takes into account related solidity types info and element indexed or hashed
func getSQLType(abiInputInfo abi.Argument) (types.SQLColumnType, int, error) {
	evmSignature := strings.ToLower(abiInputInfo.EVM.GetSignature())

	re := regexp.MustCompile("[0-9]+")
	typeSize, _ := strconv.Atoi(re.FindString(evmSignature))

	// solidity arrays => sql bytes
	if abiInputInfo.IsArray {
		return types.SQLColumnTypeByteA, 0, nil
	}

	switch {
	// solidity address => sql bytes
	case evmSignature == types.EventInputTypeAddress:
		return types.SQLColumnTypeByteA, 0, nil
		// solidity bool => sql bool
	case evmSignature == types.EventInputTypeBool:
		return types.SQLColumnTypeBool, 0, nil
		// solidity bytes => sql bytes
	case strings.HasPrefix(evmSignature, types.EventInputTypeBytes):
		return types.SQLColumnTypeByteA, 0, nil
		// solidity string => sql text
	case evmSignature == types.EventInputTypeString:
		return types.SQLColumnTypeText, 0, nil
		// solidity int <= 32 => sql int
		// solidity int > 32 => sql numeric
	case strings.HasPrefix(evmSignature, types.EventInputTypeInt):
		if typeSize <= 32 {
			return types.SQLColumnTypeInt, 0, nil
		} else {
			return types.SQLColumnTypeNumeric, 0, nil
		}
		// solidity uint <= 16 => sql int
		// solidity uint > 16 => sql numeric
	case strings.HasPrefix(evmSignature, types.EventInputTypeUInt):
		if typeSize <= 16 {
			return types.SQLColumnTypeInt, 0, nil
		} else {
			return types.SQLColumnTypeNumeric, 0, nil
		}
	default:
		return -1, 0, fmt.Errorf("Don't know how to map evmSignature: %s ", evmSignature)
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
