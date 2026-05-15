"""dto.py"""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Generic, Literal, TypeVar, TypeAlias,Protocol, runtime_checkable
from collections.abc import Mapping, Iterable

# import polars as pl
# Records: TypeAlias = Iterable[dict[str, Any]] | dict[str, Any] | list[dict[str, Any]]  # aka json

type JSONScalar = str | int | float | bool | None
type JSON = dict[str, JSONScalar | JSON] | list[JSONScalar | JSON]
Records: TypeAlias = JSON | Iterable[dict[str, JSON]]

T = TypeVar('T')
# | pl.DataFrame | pl.LazyFrame

# Legacy mapping.
PYTHON_TYPES = Literal[
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

@dataclass
class Column:
    name: str
    alias: str | None = None
    raw_type: str | None = None
    python_type: PYTHON_TYPES | None = None
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
class Catalog(Generic[T]):
    name: str | None = None
    alias: str | None = None
    label: str | None = None
    description: str | None = None
    entities: list[Table] = field(default_factory=list)
    ok: bool = False
    records: T | Records | None = None
    code: int = 500
    message: str = ''
    properties: dict[str, Any] = field(default_factory=dict)

@runtime_checkable
class DTO(Protocol):
    def describe_catalog(self, namespace: str | None = None) -> Catalog: ...
    def describe_table(self, table: Table) -> Catalog: ...
    def describe_column(self, table: Table, column: Column) -> Catalog: ...

    def query(self, sql: str, binds: dict[str, Any] | None = None) -> Catalog: ...
    def get_records(
        self,
        table: Table,
        columns: list[str] | None = None,
        filters: dict[str, Any] | None = None,
    ) -> Catalog: ...
    def insert_records(self, table: Table, records: Records) -> Catalog: ...
    def update_records(self, table: Table, records: Records) -> Catalog: ...
    def merge_records(
        self,
        table: Table,
        records: Records,
        key_columns: list[str],
    ) -> Catalog: ...
    def delete_records(self, table: Table, records: Records) -> Catalog: ...