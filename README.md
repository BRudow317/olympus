# charon

A Python library for managing data from Oracle and Salesforce behind a single common interface.

---

## What it does

Charon wraps Oracle (oracledb) and Salesforce (REST + Bulk 2.0) behind a `DataSource` protocol.
Any code that works with one works with the other. You can describe a schema, describe a table,
run a raw query, or pull records -- all through the same four methods.

---

## Setup

Requires Python 3.11 or higher.

```shell
py -3.11 -m venv .venv
.venv\Scripts\pip install -e .
```

---

## Usage
```shell

# bash
python3.11 ./charon.py -v -l ./.logs \
    --exec ./src/app.py \
    --source-system salesforce \
    --source-environment TRAIL \
    --source-namespace TRAIL \
    --target-system oracle \
    --target-environment DWH \
    --target-namespace DWH \
    --action reset \
    --tables Asset Account

# Powershell
python ./charon.py -v -l ./.logs `
    --exec ./src/app.py `
    --source-system salesforce `
    --source-environment TRAIL `
    --source-namespace TRAIL `
    --target-system oracle `
    --target-environment DWH `
    --target-namespace DWH `
    --action reset `
    --tables *
```


## Configs


``` shell
env=$1
oracle_dbname_user=myuser
oracle_dbname_pass=examplepassword123
oracle_dbname_host=localhost
oracle_dbname_port=1521
oracle_dbname_service=exampledbservice

sf_orgname_consumer_key=exampleconsumerkey123
sf_orgname_consumer_secret=examplesecretkey123
sf_orgname_base_url=https://some-trailheadorg-8eqg8r-dev-ed.trailblaze.my.salesforce.com
```
---

### Other Important Configs
```jsonc
{
    "salesforce_api_version": "66.0",
    "salesforce_auth_uri": "/services/oauth2/token",
    "salesforce_callback_url": "http://localhost:1717/OauthRedirect",
}
```

## DataSource protocol

```python
# ./src/DataSource.py
@runtime_checkable
class DataSource(Protocol):
    def describe_schema(self, namespace: str | None = None) -> Schema: ...
    def describe_table(self, table: Table[Any]) -> Table: ...
    def mutate_table(self, table: Table[Any]) -> Table: ...
    def query(self, statement: str, **kwargs) -> Records: ...
    def get_records(self, table: Table, **kwargs) -> Records: ...
    def load_records(self, action: str, table: Table, records: Records, **kwargs) -> None: ...
```

`Schema`, `Table`, and `Records` are plain dataclasses defined in `src/models.py`.
`Column` carries type info (`python_type: PythonTypes`)

---

## Oracle specifics

Schema must be set explicitly -- it is not read from environment variables or session state.

```python
from src.Oracle import Oracle

db = Oracle("PROD", schema="MY_SCHEMA")
```

If `schema` is omitted, the connected user's default schema is used as a fallback.

---

## Salesforce specifics

Authentication uses the OAuth 2.0 client credentials flow -- no user interaction required.
Tokens are refreshed automatically on `INVALID_SESSION_ID` responses.

```python
from src.Salesforce import Salesforce

sf = Salesforce("PROD")
```

Objects that cannot be queried freely (feeds, history, shares, platform internals) are
excluded automatically by `describe_schema`.

---

## PythonTypes

All column types are normalized to a common vocabulary:

```
string    integer   float     boolean
date      datetime  time      byte
bytearray json
```

This makes the output of `describe_table` comparable across Oracle and Salesforce regardless
of the underlying raw type strings.

---

## Running tests

```
./tests/
```
