# python oracledb

## Links:
- [Oracle's Python Driver Documentation](https://python-oracledb.readthedocs.io/en/latest/)
- [OracleDataFrame API Reference](https://python-oracledb.readthedocs.io/en/latest/api/oracledb.html#oracledb.OracleDataFrame)
- [Arrow DataFrame Interchange Protocol](https://arrow.apache.org/docs/python/dataframe.html)
- [Polars from_arrow() Documentation](https://pola-rs.github.io/polars/py-polars/html/reference/api/polars.from_arrow.html) 
- [DataFrame API Protocol](https://data-apis.org/dataframe-protocol/latest/API.html)
- [Apache NanoArrow](https://arrow.apache.org/nanoarrow/latest/index.html)

## Overview
- `Connection.fetch_df_all()` and `Connection.fetch_df_batches()` return `OracleDataFrame` objects.
- `OracleDataFrame` exposes the Apache Arrow PyCapsule interface for zero-copy transfers.
- You can create PyArrow Tables using `pyarrow.Table.from_arrays(arrays=odf.column_arrays(), names=odf.column_names())`. Polars DataFrame is created via `polars.from_arrow(pyarrow_table)`.
- Data Mapping: Needs a quick summary of `fetch_decimals=True` for NUMBER -> DECIMAL128.
- API Methods: `odf.column_arrays()`, `odf.column_names()`, `odf.num_rows()`, `odf.num_columns()`, etc.

## DataFrame Core Protocol and Concepts
- **Buffer class**. A buffer is a contiguous block of memory - this is the only thing that actually maps to a 1-D array in a sense that it could be converted to NumPy, CuPy, et al.

- **Column class**. A column has a single dtype. It can consist of multiple chunks . A single chunk of a column (which may be the whole column if num_chunks == 1 ) is modeled as again a Column instance, and contains 1 data buffer and (optionally) one mask for missing data.

- **DataFrame class**. A data frame is an ordered collection of columns , which are identified with names that are unique strings. All the data frame’s rows are the same length. It can consist of multiple chunks . A single chunk of a data frame is modeled as again a DataFrame instance.

- **mask**. A mask of a single-chunk column is a buffer .

- **chunk** A chunk is a sub-dividing element that can be applied to a data frame or a column .

## Native Arrow DataFrame Capabilities
### Core Concept:
Starting in version 3.0.0, `python-oracledb` queries can fetch data directly into `OracleDataFrame` objects. These objects expose an **Apache Arrow PyCapsule Interface**, allowing for zero-copy data interchanges to PyArrow and Polars without ever serializing the data into Python dictionaries or tuples.

### 1. Ingress Methods (on the `Connection` object)
* **`Connection.fetch_df_all(statement, parameters, arraysize)`**: Fetches all rows into a single `OracleDataFrame` object.
* **`Connection.fetch_df_batches(statement, parameters, size)`**: Implements an iterator returning batches of rows as `OracleDataFrame` objects. **Crucial for low-memory (OOM-safe) streaming.**

### 2. The `OracleDataFrame` API
The `OracleDataFrame` object implements the standard Python DataFrame Interchange Protocol. 

**Key Methods:**
* `odf.column_arrays()`: Returns a list of `OracleArrowArray` objects (this is what PyArrow consumes).
* `odf.column_names()`: Returns a list of the column names.
* `odf.num_rows()` / `odf.num_columns()`: Returns the row/column count.
* `odf.get_column_by_name(name)`: Returns an `OracleColumn` object.

```python
import polars
import pyarrow

# Get an OracleDataFrame
# Adjust arraysize to tune the query fetch performance
sql = "select id from SampleQueryTab order by id"
odf = connection.fetch_df_all(statement=sql, arraysize=100)

# Convert to a Polars DataFrame
pyarrow_table = pyarrow.Table.from_arrays(
    odf.column_arrays(), names=odf.column_names()
)
df = polars.from_arrow(pyarrow_table)

# Perform various Polars operations on the DataFrame
r, c = df.shape
print(f"{r} rows, {c} columns")
print(p.sum())
```

### 3. Zero-Copy Conversion to PyArrow & Polars
Do not iterate rows manually. Use the PyCapsule interface to pass the arrays directly to PyArrow, and then into Polars.

```python
import pyarrow as pa
import polars as pl

# 1. Fetch the OracleDataFrame
odf = connection.fetch_df_all(statement=sql, arraysize=1000)

# 2. Convert to PyArrow Table (Zero-Copy via PyCapsule)
pyarrow_table = pa.Table.from_arrays(
    arrays=odf.column_arrays(), 
    names=odf.column_names()
)

# 3. Convert to Polars DataFrame (Zero-Copy)
df = pl.from_arrow(pyarrow_table)
```

### 4. Critical Data Type Mappings (Oracle -> Arrow)
Output type handlers cannot be used to map data types when using DataFrames.
* **Strings:** `VARCHAR`, `CHAR`, `NVARCHAR`, `NCHAR` → `STRING`
* **Dates:** `DATE`, `TIMESTAMP` → `TIMESTAMP` (Dates default to "seconds" unit; Timestamps scale from ms to ns based on precision). Arrow timestamps lack timezone data.
* **Numbers:** `NUMBER` -> `DOUBLE` (by default). 
    * *CRITICAL FIX:* If `defaults.fetch_decimals` is set to `True`, `NUMBER` columns are fetched safely as `DECIMAL128`. If precision <= 18 and scale is 0, it maps to `INT64`.
* **Booleans:** `DB_TYPE_BOOLEAN` → `BOOLEAN`

### 5. Streaming Pattern (Low Memory Wrapper Example)
For containerized constraints (e.g., 4GB RAM), use this pattern to yield PyArrow RecordBatches:

```python
import pyarrow as pa

def stream_oracle_to_batches(connection, sql: str, batch_size: int = 50000):
    """Yields PyArrow RecordBatches directly from Oracle without RAM bloat."""
    
    # Iterate through chunks natively
    for odf in connection.fetch_df_batches(statement=sql, size=batch_size):
        
        # Assemble the PyArrow Table from the Arrow Arrays
        pa_table = pa.Table.from_arrays(
            arrays=odf.column_arrays(), 
            names=odf.column_names()
        )
        
        # Yield as RecordBatches to satisfy a RecordBatchReader pipeline
        for batch in pa_table.to_batches():
            yield batch
```

## Writing Apache Parquet Files
To write output in Apache Parquet file format, you can use data frames as an efficient intermediary. Use the Connection.fetch_df_batches() iterator and convert to a PyArrow Table that can be written by the PyArrow library.
[More parquet samples](https://github.com/oracle/python-oracledb/blob/main/samples/dataframe_parquet_write.py)
```python
import pyarrow
import pyarrow.parquet as pq

FILE_NAME = "sample.parquet"

# Tune the fetch batch size for your query
BATCH_SIZE = 10000

sql = "select * from mytable"
pqwriter = None
for odf in connection.fetch_df_batches(statement=sql, size=BATCH_SIZE):

    # Get a PyArrow table from the query results
    pyarrow_table = pyarrow.Table.from_arrays(
        arrays=odf.column_arrays(), names=odf.column_names()
    )

    if not pqwriter:
        pqwriter = pq.ParquetWriter(FILE_NAME, pyarrow_table.schema)

    pqwriter.write_table(pyarrow_table)

pqwriter.close()
```
