# olympus

A Python library for managing data from Oracle and Salesforce behind a single common interface.

---

## What it does

Olympus wraps Oracle (oracledb) and Salesforce (REST + Bulk 2.0) behind a `DataSource` protocol.
Any code that works with one works with the other. You can describe a schema, describe a table,
run a raw query, or pull records -- all through the same four methods.

---

## Setup

Requires Python 3.11.

```
py -3.11 -m venv .venv
.venv\Scripts\pip install -e .
```

---

## env file and usage

``` shell
ORAENV=$1
SFENV=$2
ORACLE_${ORAENV}_USER=myuser
ORACLE_${ORAENV}_PASS=examplepassword123
ORACLE_${ORAENV}_HOST=localhost
ORACLE_${ORAENV}_PORT=1521
ORACLE_${ORAENV}_SERVICE=exampledbservice

SF_${SFENV}_CONSUMER_KEY=exampleconsumerkey123
SF_${SFENV}_CONSUMER_SECRET=examplesecretkey123
SF_${SFENV}_BASE_URL=https://some-trailheadorg-8eqg8r-dev-ed.trailblaze.my.salesforce.com
SF_${SFENV}_API_VERSION=66.0
SF_${SFENV}_AUTH_URI=/services/oauth2/token
SF_${SFENV}_CALLBACK_URL=http://localhost:1717/OauthRedirect
```

---

## DataSource protocol

```python
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
