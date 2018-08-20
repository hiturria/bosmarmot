# Vent Adapters

Vent adapters are relational dbms that can be used to store event & log data.

## implement

Each adapter must be in a separate file with the name `<dbms>_adapter.go` and must implement given interface functions described in `db_adapter.go`.

## supported adapters

PostgreSQL v9 (and above) is the first fully supported adapter for Vent.
