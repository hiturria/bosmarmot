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

	"github.com/monax/bosmarmot/vent/types"
	"github.com/pkg/errors"
)

// Parser contains EventTable, Event & Abi specifications
type Parser struct {
	Tables    types.EventTables
	EventSpec types.EventSpec
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
		for colName, col := range eventDef.Columns {
			sqlType, sqlTypeLength, err := getSQLType(strings.ToLower(col.Type), false, col.BytesToString)
			if err != nil {
				return nil, err
			}

			j++

			columns[colName] = types.SQLTableColumn{
				Name:          strings.ToLower(col.Name),
				Type:          sqlType,
				EVMType:       col.Type,
				Length:        sqlTypeLength,
				Primary:       col.Primary,
				BytesToString: col.BytesToString,
				Order:         j + globalColumnsLength,
			}
		}

		// add global columns to columns definition
		for k, v := range globalColumns {
			columns[k] = v
		}

		tables[eventDef.TableName] = types.SQLTable{
			Name:    strings.ToLower(eventDef.TableName),
			Filter:  eventDef.Filter,
			Columns: columns,
		}
	}

	// check if there are duplicated duplicated column names (for a given table)
	colName := make(map[string]int)

	for _, tbls := range tables {
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
	}, nil
}

// GetEventSpec returns the event specification
func (p *Parser) GetEventSpec() types.EventSpec {
	return p.EventSpec
}

// GetTables returns the event tables structures
func (p *Parser) GetTables() types.EventTables {
	return p.Tables
}

// GetColumn receives a table & column name and returns column info
func (p *Parser) GetColumn(tableName, columnName string) (types.SQLTableColumn, error) {
	column := types.SQLTableColumn{}

	if table, ok := p.Tables[tableName]; ok {
		if column, ok = table.Columns[columnName]; ok {
			return column, nil
		}
		return column, fmt.Errorf("GetColumn: columnName does not exists as a column in SQL table structure: %s ", columnName)
	}

	return column, fmt.Errorf("GetColumn: tableName does not exists as a table in SQL table structure: %s ", tableName)
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
func getSQLType(evmSignature string, isArray bool, bytesToString bool) (types.SQLColumnType, int, error) {

	re := regexp.MustCompile("[0-9]+")
	typeSize, _ := strconv.Atoi(re.FindString(evmSignature))

	switch {
	// solidity address => sql varchar
	case evmSignature == types.EventInputTypeAddress:
		return types.SQLColumnTypeVarchar, 40, nil
		// solidity bool => sql bool
	case evmSignature == types.EventInputTypeBool:
		return types.SQLColumnTypeBool, 0, nil
		// solidity bytes => sql bytes
		// bytesToString == true means there is a string in there so => sql varchar
	case strings.HasPrefix(evmSignature, types.EventInputTypeBytes):
		if bytesToString {
			return types.SQLColumnTypeVarchar, 40, nil
		} else {
			return types.SQLColumnTypeByteA, 0, nil
		}
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

	globalColumns[types.BlockHeightLabel] = types.SQLTableColumn{
		Name:    types.SQLColumnLabelHeight,
		Type:    types.SQLColumnTypeVarchar,
		Length:  100,
		Primary: false,
		Order:   1,
	}

	globalColumns[types.TxTxHashLabel] = types.SQLTableColumn{
		Name:    types.SQLColumnLabelTxHash,
		Type:    types.SQLColumnTypeVarchar,
		Length:  40,
		Primary: false,
		Order:   2,
	}

	globalColumns[types.EventTypeLabel] = types.SQLTableColumn{
		Name:    types.SQLColumnLabelEventType,
		Type:    types.SQLColumnTypeVarchar,
		Length:  100,
		Primary: false,
		Order:   3,
	}

	globalColumns[types.EventNameLabel] = types.SQLTableColumn{
		Name:    types.SQLColumnLabelEventName,
		Type:    types.SQLColumnTypeVarchar,
		Length:  100,
		Primary: false,
		Order:   4,
	}

	return globalColumns
}
