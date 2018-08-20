# Vent component

Vent reads an event configuration file, parses its contents and maps column types to corresponding PostgreSQL types to synchronize database structures.
Database structures are created or modified (just adding new columns is supported).
Then listens to burrow gRPC events, parses data and builds rows to be upserted in corresponding event tables.
Rows are upserted in blocks, where each block is one commit.
Block identification is stored in Log tables to be able to resume pending blocks.

## Setup postgres database:

```bash
# Create postgres container (only once):
docker run --name postgres-local -e POSTGRES_USER=user -e POSTGRES_PASSWORD=pass -e POSTGRES_DB=bosmarmot -p 5432:5432 -d postgres:10.4-alpine

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

# How to run vent command:
vent --db-adapter="postgres" --db-url="postgres://user:pass@localhost:5432/bosmarmot?sslmode=disable" --db-schema="bosmarmot" --grpc-addr="localhost:10997" --log-level="debug" --cfg-file="<sqlsol conf file path>"
```
