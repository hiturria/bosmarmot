package adapters

import "github.com/monax/bosmarmot/vent/types"

// DBAdapter database acces interfase
type DBAdapter interface {
	Open() error
	Ping() error
	SynchronizeDB(eventTables types.EventTables) error
	SetBlock(eventTables types.EventTables, eventData types.EventData) error
	GetLastBlockID() (string, error)
	GetBlock(block string) (types.EventData, error)
	DestroySchema() error
	Close()
}
