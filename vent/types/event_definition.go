package types

import (
	"github.com/go-ozzo/ozzo-validation"
)

// EventSpec contains all event specifications
type EventSpec []EventDefinition

// EventDefinition struct (table name where to persist filtered events and it structure)
type EventDefinition struct {
	TableName string                 `json:"TableName"`
	Filter    string                 `json:"Filter"`
	Event     Event                  `json:"Event"`
	Columns   map[string]EventColumn `json:"Columns"`
}

// Validate checks the structure of an EventDefinition
func (evDef EventDefinition) Validate() error {
	return validation.ValidateStruct(&evDef,
		validation.Field(&evDef.TableName, validation.Required, validation.Length(1, 60)),
		validation.Field(&evDef.Event, validation.Required),
		validation.Field(&evDef.Columns, validation.Required, validation.Length(1, 0)),
	)
}

// Event struct (each given type of event)
type Event struct {
	Anonymous bool         `json:"anonymous"`
	Inputs    []EventInput `json:"inputs"`
	Name      string       `json:"name"`
	Type      string       `json:"type"`
}

// Validate checks the structure of an Event
func (ev Event) Validate() error {
	return validation.ValidateStruct(&ev,
		validation.Field(&ev.Inputs, validation.Required, validation.Length(1, 0)),
		validation.Field(&ev.Name, validation.Required, validation.Length(1, 0)),
	)
}

// EventInput struct (each event input)
type EventInput struct {
	Indexed bool   `json:"indexed"`
	Name    string `json:"name"`
	Type    string `json:"type"`
}

// Validate checks the structure of an EventInput
func (evInput EventInput) Validate() error {
	return validation.ValidateStruct(&evInput,
		validation.Field(&evInput.Name, validation.Required, validation.Length(1, 0)),
		validation.Field(&evInput.Type, validation.Required, validation.By(IsValidEventInputType)),
	)
}

// EventColumn struct (table column definition)
type EventColumn struct {
	Name    string `json:"name"`
	Primary bool   `json:"primary"`
}

// Validate checks the structure of an EventColumn
func (evColumn EventColumn) Validate() error {
	return validation.ValidateStruct(&evColumn,
		validation.Field(&evColumn.Name, validation.Required, validation.Length(1, 60)),
	)
}
