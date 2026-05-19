"""dto.py"""
from __future__ import annotations
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Protocol, runtime_checkable 
from collections.abc import Iterator

class System(StrEnum):
    ORACLE = 'oracle'
    SALESFORCE = 'salesforce'

class PythonTypes(StrEnum):
     STRING = "string"
     INTEGER = "integer"
     FLOAT = "float"
     BOOLEAN = "boolean"
     DATETIME = "datetime" # datetime.datetime # timezone format
     DATE = "date"     # datetime.date
     TIME = "time"     # datetime.time
     BYTE = "byte"
     BYTEARRAY = "bytearray"
     JSON = "json"     # dict or list