# olympus

A Python library for reading data from Oracle and Salesforce behind a single common interface.

---

## What it does

Olympus wraps Oracle (oracledb) and Salesforce (REST + Bulk 2.0) behind a `DataSource` protocol.
Any code that works with one works with the other. You can describe a schema, describe a table,
run a raw query, or pull records -- all through the same four methods.

---

## Project layout

```
src/
  models.py          - DataSource protocol, Column, Table, Schema, Records dataclasses
  PythonTypes.py     - System and PythonTypes enums (shared type vocabulary)

  Oracle.py          - Oracle DataSource implementation
  OracleClient.py    - thin oracledb connection wrapper
  OracleDialect.py   - SQL strings for schema/table introspection
  OracleTypeMap.py   - Oracle DB type -> PythonTypes mapping

  Salesforce.py      - Salesforce DataSource implementation
  SfClient.py        - httpx-based REST client, OAuth client credentials, token refresh
  SfRestEngine.py    - SOQL query engine, global describe, SOSL search, limits, APEX, SObject CRUD
  SfBulk2Engine.py   - Bulk API 2.0 ingest (insert, upsert, update, delete) and bulk query
  SfDialect.py       - SOQL value quoting, escaping, and formatting utilities
  SfTypeMap.py       - Salesforce field type -> PythonTypes mapping
  csv_utils.py       - CSV split, count, and conversion helpers for Bulk API 2.0 file prep

  app.py             - entry point: describe tables and fetch records

tests/               - pytest test suite
pyproject.toml       - project metadata and dependencies
```

---

## Setup

Requires Python 3.11.

```
py -3.11 -m venv .venv
.venv\Scripts\pip install -e .
```

---

## Environment variables

### Oracle

```
ORACLE_{ENV}_USER      database username
ORACLE_{ENV}_PASS      database password
ORACLE_{ENV}_HOST      host or IP
ORACLE_{ENV}_PORT      port (default: 1521)
ORACLE_{ENV}_SERVICE   service name
```

Replace `{ENV}` with your environment label, e.g. `ORACLE_PROD_USER`.

### Salesforce

```
SF_{ENV}_BASE_URL        org URL, e.g. https://myorg.my.salesforce.com
SF_{ENV}_CONSUMER_KEY    connected app client ID
SF_{ENV}_CONSUMER_SECRET connected app client secret
SF_{ENV}_API_VERSION     API version (default: 66.0)
SF_{ENV}_MAX_RETRIES     token refresh retries (default: 1)
```

Replace `{ENV}` with your environment label, e.g. `SF_PROD_BASE_URL`.

---

## DataSource protocol

Every data source exposes these four methods:

```python
class DataSource(Protocol):
    def describe_schema(self, namespace: str | None = None, environment: str | None = None) -> Schema: ...
    def describe_table(self, table: Table) -> Table: ...
    def query(self, statement: str, **kwargs) -> Records: ...
    def get_records(self, table: Table, **kwargs) -> Records: ...
```

`Schema`, `Table`, and `Records` are plain dataclasses defined in `src/models.py`.
`Column` carries type info (`python_type: PythonTypes`), key flags, FK mappings, and nullability.

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

## Fetch records via app.py

```python
from src.app import fetch
from src.models import System

fetch(
    system=System.SALESFORCE,
    environment="PROD",
    tables=["Account", "Contact"],
    limit=100,
)
```

Records are printed to stdout as newline-delimited JSON.

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
.venv\Scripts\pytest
```
