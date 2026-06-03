"""OracleTypeMap.py"""
from __future__ import annotations
from typing import Any #, Literal
from datetime import datetime, time, date
from decimal import Decimal, InvalidOperation
from dataclasses import asdict

import oracledb

from src.models import Column, PythonTypes, Table
from src.oracle.OracleModels import ( 
    OracleTable, 
    OracleColumn, 
    COMMA_RE, 
    TZ_OFFSET_RE, 
    TIMESTAMP_FMTS, 
    NULL_BYTE_RE, 
    DATE_FMT
)

def oracle_to_python(raw_type: str, scale: int | None = None, max_length: int | None = None) -> PythonTypes:
    raw_upper = raw_type.upper()
    
    if raw_upper in ("CLOB", "NCLOB", "ROWID", "UROWID"):
        return PythonTypes.string
        
    if raw_upper in ("VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR"):
        # TODO 
        # This needs expanded to ensure only boolean values are converted to 
        # PythonTypes.boolean, and not all single-character strings. This is a 
        # placeholder until wiring in check constraints for a max of 2 accepted values. 
        # if max_length == 1: 
        #     return PythonTypes.boolean 
        return PythonTypes.string
        
    if raw_upper == "NUMBER":
        if scale == 0:
            return PythonTypes.integer
        else:
            return PythonTypes.float
            
    if raw_upper in ("FLOAT", "BINARY_FLOAT", "BINARY_DOUBLE"):
        return PythonTypes.float
        
    if "TIMESTAMP" in raw_upper:
        return PythonTypes.datetime
        
    if raw_upper == "DATE":
        return PythonTypes.date
        
    if raw_upper in ("BLOB", "RAW", "LONG RAW", "BFILE"):
        return PythonTypes.byte
        
    if raw_upper == "JSON":
        return PythonTypes.json
        
    return PythonTypes.string

def python_to_oracle(column: Column) -> str:
    ptype = column.python_type or None
    
    if ptype is None:
        raise ValueError(f"Column {column.name} is missing 'python_type' in properties.")
        
    if ptype == PythonTypes.string:
        length = column.max_length
        if length is None or length > 4000:
            return "CLOB"
        return f"VARCHAR2({length} CHAR)"
        
    if ptype == PythonTypes.integer:
        return "NUMBER"
        
    if ptype == PythonTypes.float:
        precision = column.precision
        scale = column.scale
        if precision is not None and scale is not None:
            return f"NUMBER({precision}, {scale})"
        else:
            return "NUMBER"
            
    if ptype == PythonTypes.boolean:
        return "VARCHAR2(1 CHAR)"
        
    if ptype == PythonTypes.datetime:
        return "TIMESTAMP WITH TIME ZONE" if column.timezone else "TIMESTAMP"
        
    if ptype == PythonTypes.date:
        return "DATE"
        
    if ptype == PythonTypes.time:
        return "VARCHAR2(15 CHAR)"
        
    if ptype == PythonTypes.byte:
        return "BLOB"
        
    if ptype == PythonTypes.json:
        return "JSON"
        
    return "VARCHAR2(255 CHAR)"

def to_oracle_table(table: Table | OracleTable, enforce_constraints: bool = True) -> OracleTable:
    if isinstance(table, Table) and not isinstance(table, OracleTable):
        cols: list[OracleColumn] = []
        for c in table.columns:
            col: Column = c
            ora_raw = (
                python_to_oracle(col).split("(")[0].strip()
                if col.python_type is not None
                else col.raw_type
            )
            col_data = asdict(col)
            col_data["raw_type"] = ora_raw
            col_data["enforce_constraints"] = enforce_constraints
            cols.append(OracleColumn(**col_data))

        return OracleTable(
            name=table.name,
            system=table.system,
            environment=table.environment,
            alias=table.alias,
            namespace=table.namespace,
            prefix=table.prefix,
            columns=cols,
            properties=table.properties.copy(),
        )

    # Already an OracleTable: apply the enforcement policy to its columns too.
    for col in table.columns:
        col.enforce_constraints = enforce_constraints
    return table

def oracle_bool_to_python(value: str | int | None = None) -> bool | None:
    if value is None:
        return None
    return str(value).strip().upper() == "Y"

from datetime import datetime, time, date
from decimal import Decimal, InvalidOperation
from typing import Any

def python_bool_to_oracle(value: Any) -> str | None:
    if value is None:
        return None
    return "Y" if bool(value) else "N"

def to_decimal(value: Any) -> Decimal | None:
    if isinstance(value, (int, float, Decimal)):
        return Decimal(str(value))
    try:
        cleaned: str = COMMA_RE.sub('', str(value).strip())
        return Decimal(cleaned)
    except (InvalidOperation, ValueError, AttributeError):
        return None

def to_date(value: Any) -> date | str | None:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        stripped: str = value.strip()
        if not stripped:
            return None
        try:
            return datetime.strptime(stripped[:10], DATE_FMT).date()
        except ValueError:
            return stripped
    return value

def to_datetime(value: Any) -> datetime | str | None:
    # 1. Promote pre-parsed objects immediately
    if isinstance(value, time):
        return datetime.combine(datetime.today(), value)
    if isinstance(value, date) and not isinstance(value, datetime):
        return datetime.combine(value, time.min)
    if isinstance(value, datetime):
        return value
        
    # 2. Parse raw strings if needed
    if isinstance(value, str):
        stripped: str = value.strip()
        if not stripped:
            return None
        cleaned: str = TZ_OFFSET_RE.sub('', stripped)
        for fmt in TIMESTAMP_FMTS:
            try:
                return datetime.strptime(cleaned, fmt)
            except ValueError:
                continue
        return stripped
        
    return value

def normalize_cell(raw_type: str, raw: Any, python_type: PythonTypes | None = None) -> Any:
    if raw is None:
        return None
        
    if isinstance(raw, bool) or python_type == PythonTypes.boolean:
        return python_bool_to_oracle(raw)
        
    # 1. DEFENSIVE FIX: Intercept raw date/time objects by their native Python type first
    # This prevents un-mapped columns from sliding down to string operations
    if isinstance(raw, time):
        return to_datetime(raw) # Instantly promotes to datetime, DPY-3002
    elif isinstance(raw, datetime):
        return raw
    elif isinstance(raw, date):
        # Check if the cell needs full hour/minute/second padding
        if str(raw_type).upper() == 'TIMESTAMP':
            return to_datetime(raw)
        return to_date(raw)
        
    # 2. Handle numbers and basic types
    if isinstance(raw, (Decimal, int, float)):
        value = raw
    else:
        value = NULL_BYTE_RE.sub('', str(raw))
        if not value.strip():
            return None
            
    # 3. Route explicitly if the schema matched perfectly
    raw_type_upper = str(raw_type).upper()
    if raw_type_upper == 'NUMBER':
        return to_decimal(value)
    elif raw_type_upper == 'DATE':
        return to_date(value)
    elif 'TIMESTAMP' in raw_type_upper:
        return to_datetime(value)
    elif raw_type_upper in ('CLOB', 'BLOB'):
        return value
    else:
        return str(value).strip()