"""app.py"""
from __future__ import annotations
import logging
import json
from typing import Any

from sf.Salesforce import Salesforce
from oracle.Oracle import Oracle
from src.models import DataSource, System, Table

logger = logging.getLogger(__name__)


def _make_client(system: System, environment: str) -> DataSource:
    if system == System.ORACLE:
        return Oracle(environment)
    return Salesforce(environment)


def _serialize(record: dict[str, Any]) -> str:
    return json.dumps(record, default=str)


def fetch(
    system: System,
    environment: str,
    namespace: str | None = None,
    tables: list[str] = [],
    limit: int = 200,
) -> None:
    source = _make_client(system, environment)

    for table_name in tables:
        stub = Table(name=table_name, system=system, namespace=namespace, environment=environment)

        logger.info(f"Describing {system}.{table_name} ...")
        described = source.describe_table(stub)
        logger.info(f"  {len(described.columns)} columns: {[c.name for c in described.columns]}")

        logger.info(f"Fetching records from {table_name} (limit={limit}) ...")
        result = source.get_records(described, limit=limit)

        if result.code != 200:
            logger.error(f"  Error fetching {table_name}: {result.message}")
            continue

        count = 0
        for record in result.data:
            print(_serialize(record))
            count += 1

        logger.info(f"  {count} record(s) from {table_name}")


def app(system: System, environment: str, tables: list[str], limit: int = 200) -> int:

    fetch(
        system=system,
        environment=environment,
        tables=tables,
        limit=limit
    )
    return 0
