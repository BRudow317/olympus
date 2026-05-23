"""OracleModels.py"""
from __future__ import annotations
from dataclasses import dataclass #, field
from typing import Any, Literal

from src.models import Table, Column #, PythonTypes
from src.oracle.OracleDialect import ORACLE_MAX_VARCHAR2_CHAR, varchar2_growth_buffer

@dataclass(kw_only=True)
class OracleColumn(Column):
    oracle_name: str | None = None
    char_length: int | None = None
    char_used: str | None = None
    is_new: bool = False
    serialized_null_value: str | None = "NULL"

    @property
    def bind_name(self) -> str:
        return self.oracle_name or self.alias or self.name.upper()

    @property
    def effective_max_varchar2(self) -> int:
        observed: int = max((self.char_length or 0), self.max_length or 0)
        if observed > ORACLE_MAX_VARCHAR2_CHAR:
            raise ValueError(
                f"observed_char_len {observed} exceeds Oracle max {ORACLE_MAX_VARCHAR2_CHAR}"
            )
        buffered: int = observed + varchar2_growth_buffer
        return min(buffered, ORACLE_MAX_VARCHAR2_CHAR)

    def column_definition(self) -> str:
        rt: str | None = self.raw_type
        if rt == "VARCHAR2":
            type_clause: str = f"VARCHAR2({self.effective_max_varchar2} CHAR)"
        elif rt == "NUMBER":
            type_clause = "NUMBER"
        elif rt == "DATE":
            type_clause = "DATE"
        elif "TIMESTAMP" in str(rt):
            type_clause = "TIMESTAMP"
        elif rt == "CLOB":
            type_clause = "CLOB"
        else:
            raise ValueError(
                f"Unrecognized raw_type '{rt}' on column '{self.bind_name}'"
            )

        null_clause = " NULL" if self.is_nullable else " NOT NULL"
        return f"{self.bind_name} {type_clause}{null_clause}"

@dataclass(kw_only=True)
class OracleTable(Table[OracleColumn]):
    fetched_columns: list[dict[str, Any]] | None = None
    input_sizes_cache: dict[str, object] | None = None
    active_plan_cache: list[tuple] | None = None

    @property
    def qualified_name(self) -> str:
        if self.namespace:
            return f"{self.namespace}.{self.name}"
        return self.name

    @property
    def column_map(self) -> dict[str, OracleColumn]:
        return {c.name: c for c in self.columns}

    @property
    def primary_key_columns(self) -> list[OracleColumn]:
        return [c for c in self.columns if c.is_primary_key]

    def clear_caches(self) -> None:
        self.fetched_columns = None
        self.input_sizes_cache = None
        self.active_plan_cache = None
        