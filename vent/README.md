# Vent component

Vent reads an event configuration file, parses its contents and maps column types to corresponding sql types to synchronize database structures.
Database structures are created or modified based on configuration file (just adding new columns is supported).
Then listens to burrow gRPC events based on given filters, parses, decodes log data and builds rows to be upserted in corresponding event tables.
Rows are upserted in blocks, where each block for a given event filter is one commit.
Block id and event filtering data is stored in Log tables to be able to resume pending blocks.

## Adapters:

Adapters are database implementations, Vent can store data in different rdbms.
In sqldb/adapters there's a list of supported adapters (there is a README in that directory that helps to understand how to implement a new one).

## Setup postgres database:

```bash
# Create postgres container (only once):
docker run --name postgres-local -e POSTGRES_USER=user -e POSTGRES_PASSWORD=pass -e POSTGRES_DB=vent -p 5432:5432 -d postgres:10.4-alpine

# Start postgres container:
docker start postgres-local

# Stop postgres container:
docker stop postgres-local

# Delete postgres container:
docker container rm postgres-local
```

## Run unit tests:

```bash
make test_integration_vent
```

## Vent command:

```bash
# Install vent command:
go install ./vent

# Print command help:
vent --help

# How to run vent command with postgres adapter:
vent --db-adapter="postgres" --db-url="postgres://user:pass@localhost:5432/vent?sslmode=disable" --db-schema="vent" --grpc-addr="localhost:10997" --log-level="debug" --cfg-file="<sqlsol conf file path>"

# How to run vent command with sqlite adapter:
vent --db-adapter="sqlite" --db-url="./vent" --grpc-addr="localhost:10997" --log-level="debug" --cfg-file="<sqlsol conf file path>"
```

`--db-adapter` = Database adapter, 'postgres' or 'sqlite' are fully supported
`--db-url` = PostgreSQL database URL or SQLite db file path
`--db-schema` = PostgreSQL database schema or empty for SQLite
`--grpc-addr` = Address to listen to gRPC Hyperledger Burrow server
`--log-level` = Logging level (error, warn, info, debug)
`--cfg-file` = SQLSol specification json file (full path)


