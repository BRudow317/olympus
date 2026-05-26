"""OracleTypeMap.py"""
from __future__ import annotations
from typing import Any #, Literal
import oracledb
from src.models import Column, PythonTypes


def python_to_oracle(column: Column) -> str:
    ptype = column.python_type or None
    if ptype is None: raise ValueError(f"Column {column.name} is missing 'python_type' in properties.")
    if ptype == PythonTypes.string:
        length = column.max_length
        if length is None or length > 4000:
            return "CLOB"
        return f"VARCHAR2({length} CHAR)"
    if ptype == PythonTypes.integer: return "NUMBER"
    if ptype == PythonTypes.float:
        precision = column.precision
        scale = column.scale
        if precision is not None and scale is not None: return f"NUMBER({precision}, {scale})"
        else: return "NUMBER"
    if ptype == PythonTypes.boolean: return "VARCHAR2(1 CHAR)"
    if ptype == PythonTypes.datetime: return "TIMESTAMP WITH TIME ZONE" if column.timezone else "TIMESTAMP"
    if ptype == PythonTypes.date: return "DATE"  
    if ptype == PythonTypes.time: return "VARCHAR2(15 CHAR)"
    if ptype == PythonTypes.byte: return "BLOB"
    if ptype == PythonTypes.json: return "JSON"
    return "VARCHAR2(255 CHAR)"


def oracle_to_python(raw_type: str, scale: int | None = None, max_length: int | None = None) -> PythonTypes:
    raw_upper = raw_type.upper()
    if raw_upper in ("CLOB", "NCLOB", "ROWID", "UROWID"): return PythonTypes.string
    if raw_upper in ("VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR"):
        #TODO
        # This needs expanded to ensure only boolean values are converted to PythonTypes.boolean, and not all single-character strings. This is a placeholder until wiring in check constraints for a max of 2 accepted values.
        # if max_length == 1: return PythonTypes.boolean
        return PythonTypes.string
    if raw_upper == "NUMBER":
        if scale == 0: return PythonTypes.integer
        else: return PythonTypes.float
    if raw_upper in ("FLOAT", "BINARY_FLOAT", "BINARY_DOUBLE"): return PythonTypes.float
    if "TIMESTAMP" in raw_upper: return PythonTypes.datetime
    if raw_upper == "DATE": return PythonTypes.date
    if raw_upper in ("BLOB", "RAW", "LONG RAW", "BFILE"): return PythonTypes.byte
    if raw_upper == "JSON": return PythonTypes.json
    return PythonTypes.string


def python_bool_to_oracle(value: bool | None) -> str | None:
    if value is None: return None
    return "Y" if value else "N"


def oracle_bool_to_python(value: str | int | None = None) -> bool | None:
    if value is None: return None
    return str(value).strip().upper() == "Y"

# def raw_type_to_oracledb_input_size(raw_type: str, char_length: int | None = None, max_length: int | None = None) -> Any:
#     rt = raw_type.upper()
#     if rt in ("VARCHAR2", "NVARCHAR2", "CHAR"):
#         return int(char_length or max_length or 4000)
#     if rt in ("NUMBER", "FLOAT", "BINARY_FLOAT", "BINARY_DOUBLE"):
#         return oracledb.DB_TYPE_NUMBER
#     if rt == "DATE":
#         return oracledb.DB_TYPE_DATE
#     if rt.startswith("TIMESTAMP"):
#         return getattr(oracledb, "DB_TYPE_TIMESTAMP_TZ", oracledb.DB_TYPE_TIMESTAMP)
#     if rt in ("CLOB", "NCLOB"):
#         return oracledb.DB_TYPE_CLOB
#     if rt in ("BLOB", "BFILE"):
#         return oracledb.DB_TYPE_BLOB
#     if rt in ("RAW", "LONG RAW"):
#         return oracledb.DB_TYPE_RAW
#     if rt == "JSON":
#         return getattr(oracledb, "DB_TYPE_JSON", oracledb.DB_TYPE_CLOB)
#     return None




# def python_to_oracledb_input_size(column: Column) -> Any:
#     ptype = column.python_type or None
#     if ptype is None: raise ValueError(f"Column {column.name} is missing 'python_type' in properties.")
#     if ptype == PythonTypes.string:
#         length = column.max_length or 4000
#         if length > 4000: return oracledb.DB_TYPE_CLOB
#         else: return length
#     if ptype in (PythonTypes.integer, PythonTypes.float): return oracledb.DB_TYPE_NUMBER
#     if ptype == PythonTypes.boolean: return 1
#     if ptype == PythonTypes.datetime: return getattr(oracledb, "DB_TYPE_TIMESTAMP_TZ", oracledb.DB_TYPE_TIMESTAMP)
#     if ptype == PythonTypes.date: return oracledb.DB_TYPE_DATE
#     if ptype == PythonTypes.time: return oracledb.DB_TYPE_VARCHAR
#     if ptype == PythonTypes.byte: return oracledb.DB_TYPE_BLOB
#     if ptype == PythonTypes.json:return getattr(oracledb, "DB_TYPE_JSON", oracledb.DB_TYPE_CLOB)
#     return None
