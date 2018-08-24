package test

import (
	"testing"
)

// GoodJSONConfFile sets a good json file to be used in parser tests
func GoodJSONConfFile(t *testing.T) string {
	t.Helper()

	goodJSONConfFile := `[
		{
			"TableName" : "UserAccounts",
			"Filter" : "LOG0 = 'UserAccounts'",
			"Event"  : {
				"anonymous": false,
				"inputs": [{
					"indexed": false,
					"name": "userName",
					"type": "string"
				}, {
					"indexed": false,
					"name": "userAddress",
					"type": "address"
				}, {
					"indexed": false,
					"name": "userBool",
					"type": "bool"
				}, {
					"indexed": false,
					"name": "userId",
					"type": "uint"
				}],
				"name": "UpdateUserAccount",
				"type": "event"
			},
			"Columns"  : {
				"userAddress" : {"name" : "address", "primary" : true},
				"userName": {"name" : "username", "primary" : false},
				"userId": {"name" : "userid", "primary" : false},
				"userBool": {"name" : "userbool", "primary" : false}
			}
		},
		{
		"TableName" : "TEST_TABLE",
		"Filter" : "Log1Text = 'EVENT_TEST'",
		"Event"  : {
			"anonymous": false,
			"inputs": [{
				"indexed": true,
				"name": "name",
				"type": "string"
			}, {
				"indexed": false,
				"name": "key",
				"type": "uint256"
			}, {
				"indexed": false,
				"name": "blocknum",
				"type": "uint256"
			}, {
				"indexed": false,
				"name": "somestr",
				"type": "string"
			}, {
				"indexed": false,
				"name": "this",
				"type": "address"
			}, {
				"indexed": false,
				"name": "instance",
				"type": "uint256"
			}],
			"name": "UpdateTable",
			"type": "event"
		},
		"Columns"  : {
			"key"		: {"name" : "Index",    "primary" : true},
			"blocknum"  : {"name" : "Block",    "primary" : false},
			"somestr"	: {"name" : "String",   "primary" : false},
			"instance" 	: {"name" : "Instance", "primary" : false}
		}
	}
	]`

	return goodJSONConfFile
}

// MissingFieldsJSONConfFile sets a json file with missing fields to be used in parser tests
func MissingFieldsJSONConfFile(t *testing.T) string {
	t.Helper()

	missingFieldsJSONConfFile := `[
		{
			"TableName" : "UserAccounts",
			"Event"  : {
				"anonymous": false,
				"inputs": [{
					"indexed": false,
					"name": "userName",
					"type": "string"
				}, {
					"indexed": false,
					"name": "userAddress",
					"type": "address"
				}, {
					"indexed": false,
					"name": "UnimportantInfo",
					"type": "uint"
				}],
				"type": "event"
			},
			"Columns"  : {
				"userAddress" : {"name" : "address", "primary" : true},
				"userName": {"name" : "username", "primary" : false}
			}
		}
	]`

	return missingFieldsJSONConfFile
}

// UnknownTypeJSONConfFile sets a json file with unknown column types to be used in parser tests
func UnknownTypeJSONConfFile(t *testing.T) string {
	t.Helper()

	unknownTypeJSONConfFile := `[
		{
			"TableName" : "UserAccounts",
			"Filter" : "LOG0 = 'UserAccounts'",
			"Event"  : {
				"anonymous": false,
				"inputs": [{
					"indexed": false,
					"name": "userName",
					"type": "typeunknown"
				}, {
					"indexed": false,
					"name": "userAddress",
					"type": "address"
				}, {
					"indexed": false,
					"name": "UnimportantInfo",
					"type": "uint"
				}],
				"name": "UpdateUserAccount",
				"type": "event"
			},
			"Columns"  : {
				"userAddress" : {"name" : "address", "primary" : true},
				"userName": {"name" : "username", "primary" : false}
			}
		},
		{
			"TableName" : "EventTest",
			"Filter" : "LOG0 = 'EventTest'",
			"Event"  : {
				"anonymous": false,
				"inputs": [{
					"indexed": false,
					"name": "name",
					"type": "typeunknown"
				}, {
					"indexed": false,
					"name": "description",
					"type": "string"
				}, {
					"indexed": false,
					"name": "UnimportantInfo",
					"type": "uint"
				}],
				"name": "TEST_EVENTS",
				"type": "event"
			},
			"Columns"  : {
				"name" : {"name" : "testname", "primary" : true},
				"description": {"name" : "testdescription", "primary" : false}
			}
		}
	]`

	return unknownTypeJSONConfFile
}

// BadJSONConfFile sets a malformed json file to be used in parser tests
func BadJSONConfFile(t *testing.T) string {
	t.Helper()

	badJSONConfFile := `[
		{
			"TableName" : "UserAccounts",
			"Event"  : {
				"anonymous": false,
				"inputs": [{
					"indexed": false,
					"name": "userName",
					"type": "string"
				}, {
					"indexed": false,
					"name": "userAddress",
					"type": "address"
				}, {
					"indexed": false,
					"name": "UnimportantInfo",
					"type": "uint"
				}],
				"name": "UpdateUserAccount",
			},
			"Columns"  : {
				"userAddress" : {"name" : "address", "primary" : true},
				"userName": {"name" : "username", "primary" : false}
	]`

	return badJSONConfFile
}

// DuplicatedTableNameJSONConfFile sets a good json file but with duplicated table names
func DuplicatedTableNameJSONConfFile(t *testing.T) string {
	t.Helper()

	duplicatedTableNameJSONConfFile := `[
		{
			"TableName" : "DUPLICATED",
			"Filter" : "LOG0 = 'UserAccounts'",
			"Event"  : {
				"anonymous": false,
				"inputs": [{
					"indexed": false,
					"name": "userName",
					"type": "string"
				}, {
					"indexed": false,
					"name": "userAddress",
					"type": "address"
				}, {
					"indexed": false,
					"name": "userBool",
					"type": "bool"
				}, {
					"indexed": false,
					"name": "userId",
					"type": "uint"
				}],
				"name": "UpdateUserAccount",
				"type": "event"
			},
			"Columns"  : {
				"userAddress" : {"name" : "address", "primary" : true},
				"userName": {"name" : "username", "primary" : false},
				"userId": {"name" : "userid", "primary" : false},
				"userBool": {"name" : "userbool", "primary" : false}
			}
		},
		{
		"TableName" : "DUPLICATED",
		"Filter" : "Log1Text = 'EVENT_TEST'",
		"Event"  : {
			"anonymous": false,
			"inputs": [{
				"indexed": true,
				"name": "name",
				"type": "string"
			}, {
				"indexed": false,
				"name": "key",
				"type": "uint256"
			}, {
				"indexed": false,
				"name": "blocknum",
				"type": "uint256"
			}, {
				"indexed": false,
				"name": "somestr",
				"type": "string"
			}, {
				"indexed": false,
				"name": "this",
				"type": "address"
			}, {
				"indexed": false,
				"name": "instance",
				"type": "uint256"
			}],
			"name": "UpdateTable",
			"type": "event"
		},
		"Columns"  : {
			"key"		: {"name" : "Index",    "primary" : true},
			"blocknum"  : {"name" : "Block",    "primary" : false},
			"somestr"	: {"name" : "String",   "primary" : false},
			"instance" 	: {"name" : "Instance", "primary" : false}
		}
	}
	]`

	return duplicatedTableNameJSONConfFile
}

// DuplicatedColNameJSONConfFile sets a good json file but with duplicated column names for a given table
func DuplicatedColNameJSONConfFile(t *testing.T) string {
	t.Helper()

	duplicatedColNameJSONConfFile := `[
		{
			"TableName" : "DUPLICATED_COLUMN",
			"Filter" : "LOG0 = 'UserAccounts'",
			"Event"  : {
				"anonymous": false,
				"inputs": [{
					"indexed": false,
					"name": "userName",
					"type": "string"
				}, {
					"indexed": false,
					"name": "userAddress",
					"type": "address"
				}, {
					"indexed": false,
					"name": "userBool",
					"type": "bool"
				}, {
					"indexed": false,
					"name": "userId",
					"type": "uint"
				}],
				"name": "UpdateUserAccount",
				"type": "event"
			},
			"Columns"  : {
				"userAddress" : {"name" : "address", "primary" : true},
				"userName": {"name" : "duplicated", "primary" : false},
				"userId": {"name" : "userid", "primary" : false},
				"userBool": {"name" : "duplicated", "primary" : false}
			}
	}
	]`

	return duplicatedColNameJSONConfFile
}
