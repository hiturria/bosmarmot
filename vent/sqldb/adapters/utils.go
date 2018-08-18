package adapters

import (
	"strings"

	"github.com/monax/bosmarmot/vent/types"
)

// Safe sanitizes a parameter
func Safe(parameter string) string {
	replacer := strings.NewReplacer(";", "", ",", "")
	return replacer.Replace(parameter)
}

// Clean queries from tabs, spaces  and returns
func Clean(parameter string) string {
	replacer := strings.NewReplacer("\n", " ", "\t", "")
	return replacer.Replace(parameter)
}

// IsNumeric determines if a datatype is numeric
func IsNumeric(dataType string) bool {
	cType := strings.ToUpper(dataType)
	return cType == types.SQLColumnTypeInt || cType == types.SQLColumnTypeSerial
}
