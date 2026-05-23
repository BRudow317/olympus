"""dto.py"""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable
from enum import StrEnum 
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

@dataclass
class Column:
    name: str
    alias: str | None = None
    raw_type: str | None = None
    python_type: PythonTypes | None = None
    is_primary_key: bool = False
    is_unique: bool = False
    is_nullable: bool = True
    is_read_only: bool = False
    is_compound_key: bool = False
    is_foreign_key: bool = False
    foreign_key_mapping: dict[str, str] = field(default_factory=dict)  # {target_table: target_column}
    is_foreign_key_enforced: bool = False
    max_length: int | None = None
    precision: int | None = None
    scale: int | None = None
    serialized_null_value: str | None = None
    default_value: Any = None
    enum_values: list[Any] = field(default_factory=list)
    timezone: str | None = None
    properties: dict[str, Any] = field(default_factory=dict)
    ordinal_position: int | None = None
    is_computed: bool = False
    is_deprecated: bool = False
    is_hidden: bool = False
    is_indexed: bool = False
    description: str | None = None

@dataclass
class Table:
    name: str
    system: System
    environment: str | None = None
    alias: str | None = None
    namespace: str | None = None
    prefix: str | None = None
    columns: list[Column] = field(default_factory=list)
    properties: dict[str, Any] = field(default_factory=dict)
    @property
    def primary_key_columns(self) -> list[Column]:
        return [f for f in self.columns if f.is_primary_key]
    @property
    def column_map(self) -> dict[str, Column]:
        return {f.name: f for f in self.columns}
    @property
    def qualified_name(self) -> str:
        if self.namespace:
            return ".".join([self.namespace, self.name])
        return self.name

@dataclass
class Schema:
    namespace: str | None = None
    environment: str | None = None
    system: System | None = None
    tables: list[Table] = field(default_factory=list)
    code: int = 200
    message: str = 'ok'

@dataclass
class Records:
    data: Iterator[dict[str, Any]] = field(default_factory=lambda: iter([]))
    code: int = 200
    message: str = 'ok'

@runtime_checkable
class DataSource(Protocol):
    def describe_schema(self, namespace: str | None = None, environment: str | None = None) -> Schema: ...
    def describe_table(self, table: Table) -> Table: ...
    def query(self, statement: str, **kwargs) -> Records: ...
    def get_records(self, table: Table, **kwargs) -> Records: ...
