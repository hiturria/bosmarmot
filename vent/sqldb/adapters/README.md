# Vent Adapters

Vent adapters are relational dbms that can be used to store event & log data.

## Supported adapters:

+ PostgreSQL v9 (and above) is the first fully supported adapter for Vent.

## Considerations for adding new adapters:

Each adapter must be in a separate file with the name `<dbms>_adapter.go` and must implement given interface functions described in `db_adapter.go`.
