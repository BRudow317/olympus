"""OracleTypeMap.py"""
from __future__ import annotations
from typing import Any #, Literal
import oracledb
from src.models import Column, PythonTypes

def python_to_oracle(column: Column) -> str:
    ptype = column.python_type or None
    if ptype is None: raise ValueError(f"Column {column.name} is missing 'python_type' in properties.")
    if ptype == PythonTypes.STRING:

        length = column.max_length or 255
        if length > 4000:
            return "CLOB"
        return f"VARCHAR2({length} CHAR)"
    if ptype == PythonTypes.INTEGER: return "NUMBER"
    if ptype == PythonTypes.FLOAT:
        precision = column.precision
        scale = column.scale
        if precision is not None and scale is not None: return f"NUMBER({precision}, {scale})"
        else: return "NUMBER"
    if ptype == PythonTypes.BOOLEAN:return "NUMBER(1, 0)"
    if ptype == PythonTypes.DATETIME: return "TIMESTAMP WITH TIME ZONE" if column.timezone else "TIMESTAMP"
    if ptype == PythonTypes.DATE: return "DATE"  
    if ptype == PythonTypes.TIME: return "VARCHAR2(15 CHAR)"
    if ptype == PythonTypes.BYTE: return "BLOB"
    if ptype == PythonTypes.JSON: return "JSON"
    return "VARCHAR2(255 CHAR)"

def python_to_oracledb_input_size(column: Column) -> Any:
    ptype = column.python_type or None
    if ptype is None: raise ValueError(f"Column {column.name} is missing 'python_type' in properties.")
    if ptype == PythonTypes.STRING:
        length = column.max_length or 4000
        if length > 4000: return oracledb.DB_TYPE_CLOB
        else: return length
    if ptype in (PythonTypes.INTEGER, PythonTypes.FLOAT, PythonTypes.BOOLEAN): return oracledb.DB_TYPE_NUMBER
    if ptype == PythonTypes.DATETIME: return getattr(oracledb, "DB_TYPE_TIMESTAMP_TZ", oracledb.DB_TYPE_TIMESTAMP)
    if ptype == PythonTypes.DATE: return oracledb.DB_TYPE_DATE
    if ptype == PythonTypes.TIME: return oracledb.DB_TYPE_VARCHAR
    if ptype == PythonTypes.BYTE: return oracledb.DB_TYPE_BLOB
    if ptype == PythonTypes.JSON:return getattr(oracledb, "DB_TYPE_JSON", oracledb.DB_TYPE_CLOB)
    return None

def oracle_to_python(raw_type: str, scale: int | None = None) -> PythonTypes:
    raw_upper = raw_type.upper()
    if raw_upper in ("VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR", "CLOB", "NCLOB", "ROWID", "UROWID"): return PythonTypes.STRING
    if raw_upper == "NUMBER":
        if scale == 0: return PythonTypes.INTEGER
        else: return PythonTypes.FLOAT
    if raw_upper in ("FLOAT", "BINARY_FLOAT", "BINARY_DOUBLE"): return PythonTypes.FLOAT
    if "TIMESTAMP" in raw_upper: return PythonTypes.DATETIME
    if raw_upper == "DATE": return PythonTypes.DATETIME
    if raw_upper in ("BLOB", "RAW", "LONG RAW", "BFILE"): return PythonTypes.BYTE
    if raw_upper == "JSON": return PythonTypes.JSON
    return PythonTypes.STRING
