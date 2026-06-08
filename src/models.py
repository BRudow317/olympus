"""models.py"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable, Generic, TypeVar
from collections.abc import Iterator
from enum import StrEnum

class System(StrEnum):
    oracle = 'oracle'
    salesforce = 'salesforce'

class SystemPrefix(StrEnum):
    oracle = 'ora'
    salesforce = 'sf'

class PythonTypes(StrEnum):
    string = "string"
    integer = "integer"
    float = "float"
    boolean = "boolean"
    datetime = "datetime" # datetime.datetime + timezone format
    date = "date"         # datetime.date
    time = "time"         # datetime.time
    byte = "byte"
    bytearray = "bytearray"
    json = "json"         # dict or list

C = TypeVar("C", bound="Column")
TABLE = TypeVar("TABLE", bound="Table")

@dataclass(kw_only=True)
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
    foreign_key_mapping: dict[str, str] = field(default_factory=dict) # {target_table: target_column}
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
    formula: str | None = None # TODO: add this to the describe_table functions and implement in both sf and oracle
    is_deprecated: bool = False
    is_hidden: bool = False
    is_indexed: bool = False
    description: str | None = None

@dataclass(kw_only=True)
class Table(Generic[C]):
    name: str
    system: System
    environment: str | None = None
    alias: str | None = None
    namespace: str | None = None
    prefix: str | None = None
    columns: list[C] = field(default_factory=list)
    record_count: int | None = None
    properties: dict[str, Any] = field(default_factory=dict)
    
    @property
    def primary_key_columns(self) -> list[C]:
        return [f for f in self.columns if f.is_primary_key]
        
    @property
    def column_map(self) -> dict[str, C]:
        return {f.name: f for f in self.columns}

@dataclass
class Schema:
    namespace: str | None = None
    environment: str | None = None
    system: System | None = None
    tables: list[Table] = field(default_factory=list)

@dataclass
class Records:
    data: Iterator[dict[str, Any]] = field(default_factory=lambda: iter([]))
    columns: list[Column] = field(default_factory=lambda: [])
    code: int = 200
    message: str = 'ok'

@runtime_checkable
class DataSource(Protocol):
    environment: str | None
    namespace: str | None
    def is_healthy(self) -> bool: ...
    def describe_schema(self, namespace: str | None = None) -> Schema: ...
    def describe_table(self, table: Table[Any]) -> Table: ...
    def mutate_table(self, table: Table[Any], source_system: System | None = None) -> Table: ...
    def query(self, statement: str, **kwargs) -> Records: ...
    def get_records(self, table: Table, **kwargs) -> Records: ...
    def load_records(self, action: str, table: Table, records: Records, **kwargs) -> None: ...
