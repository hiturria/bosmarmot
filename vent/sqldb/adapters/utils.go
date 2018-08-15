package adapters

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/monax/bosmarmot/vent/types"
)

// safe sanitizes a parameter
func safe(parameter string) string {
	replacer := strings.NewReplacer(";", "", ",", "")
	return replacer.Replace(parameter)
}

// clean queries from tabs, spaces  and returns
func clean(parameter string) string {
	replacer := strings.NewReplacer("\n", " ", "\t", "")
	return replacer.Replace(parameter)
}

// isNumeric determines if a datatype is numeric
func isNumeric(dataType string) bool {
	cType := strings.ToUpper(dataType)
	return cType == types.SQLColumnTypeInt || cType == types.SQLColumnTypeSerial
}

// getUpsertParams builds parameters in preparation for an upsert query
func getUpsertParams(uQuery upsertQuery, row types.EventDataRow) ([]interface{}, string, error) {
	pointers := make([]interface{}, uQuery.length)
	containers := make([]sql.NullString, uQuery.length)

	for colName, col := range uQuery.cols {
		// interface=data
		pointers[col.posIns] = &containers[col.posIns]
		if col.posUpd > 0 {
			pointers[col.posUpd] = &containers[col.posUpd]
		}

		// build parameter list
		if value, ok := row[colName]; ok {
			//column found (not null)
			containers[col.posIns] = sql.NullString{String: value, Valid: true}

			//if column is not PK
			if col.posUpd > 0 {
				containers[col.posUpd] = sql.NullString{String: value, Valid: true}
			}

		} else if col.posUpd > 0 {
			//column not found and is not PK (null)
			containers[col.posIns].Valid = false
			containers[col.posUpd].Valid = false

		} else {
			//column not found is PK
			return nil, "", errors.New("Error null primary key for column " + colName)
		}
	}

	return pointers, fmt.Sprintf("%v", containers), nil
}
