from typing import Any, Literal
import oracledb

def python_to_oracle_ddl(column: Column) -> str:
    """Translates a universal FieldModel into an Oracle-specific DDL type string.
       e.g., returns "VARCHAR2(255 CHAR)" or "NUMBER(1, 0)"
    """
    ptype = column.properties.get("python_type", None)
    if ptype is None: raise ValueError(f"Column {column.name} is missing 'python_type' in properties.")
    if ptype == "string":
        length = column.max_length or 255
        if length > 4000:
            return "CLOB"
        return f"VARCHAR2({length} CHAR)"
    if ptype == "integer": return "NUMBER"
    if ptype == "float":
        precision = column.precision
        scale = column.scale
        if precision is not None and scale is not None: return f"NUMBER({precision}, {scale})"
        else: return "NUMBER"
    if ptype == "boolean":return "NUMBER(1, 0)"
    if ptype == "datetime": return "TIMESTAMP WITH TIME ZONE" if column.timezone else "TIMESTAMP"
    if ptype == "date": return "DATE"  
    if ptype == "time": return "VARCHAR2(15 CHAR)"
    if ptype == "binary": return "BLOB"
    if ptype == "json": return "JSON"
    return "VARCHAR2(255 CHAR)"

def map_python_to_oracledb_input_size(column: Column) -> Any:
    """Returns the appropriate type hint for oracledb.cursor.setinputsizes().
       This is critical for performance and preventing data truncation during inserts.
    """
    ptype = column.properties.get("python_type", None)
    if ptype is None: raise ValueError(f"Column {column.name} is missing 'python_type' in properties.")
    if ptype == "string":
        length = column.max_length or 4000
        if length > 4000: return oracledb.DB_TYPE_CLOB
        else: return length
    if ptype in ("integer", "float", "boolean"): return oracledb.DB_TYPE_NUMBER
    if ptype == "datetime": return getattr(oracledb, "DB_TYPE_TIMESTAMP_TZ", oracledb.DB_TYPE_TIMESTAMP)
    if ptype == "date": return oracledb.DB_TYPE_DATE
    if ptype == "binary": return oracledb.DB_TYPE_BLOB
    if ptype == "json":return getattr(oracledb, "DB_TYPE_JSON", oracledb.DB_TYPE_CLOB)
    return None


# Legacy mapping.
PythonTypes = Literal[
    "string",
    "integer",
    "float",
    "boolean",
    "datetime", # datetime.datetime # timezone format
    "date",     # datetime.date
    "time",     # datetime.time
    "byte",
    "bytearray",
    "json",     # dict or list
]
def oracle_to_python(raw_type: str, scale: int | None = None) -> PythonTypes:
    raw_upper = raw_type.upper()
    if raw_upper in ("VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR", "CLOB", "NCLOB", "ROWID", "UROWID"): return "string"
    if raw_upper == "NUMBER":
        if scale == 0: return "integer" 
        else: return "float"
    if raw_upper in ("FLOAT", "BINARY_FLOAT", "BINARY_DOUBLE"): return "float"
    if "TIMESTAMP" in raw_upper: return "datetime"
    if raw_upper == "DATE": return "datetime"
    if raw_upper in ("BLOB", "RAW", "LONG RAW", "BFILE"): return "byte"
    if raw_upper == "JSON": return "json"
    return "string"
