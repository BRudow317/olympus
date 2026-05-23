"""seeding.py"""
from __future__ import annotations
import logging
logger: logging.Logger = logging.getLogger(__name__)
import json

from src.sf.Salesforce import Salesforce
from src.oracle.Oracle import Oracle
from src.models import DataSource, System, Table

from typing import TYPE_CHECKING, Any
if TYPE_CHECKING:
    from src.models import Records

def make_client(system: System, environment: str, namespace: str | None) -> DataSource:
    if system == System.ORACLE:
        return Oracle(environment, namespace)
    return Salesforce(environment, namespace)

def fetch_test(
    system: System,
    environment: str,
    namespace: str | None = None,
    tables: list[str] = []
) -> None:
    """usage
        fetch_test(
            system=source_system,
            environment=source_environment,
            namespace=source_namespace,
            tables=tables
        )
    """
    client: DataSource = make_client(system, environment, namespace)

    for table_name in tables:
        stub: Table[Any] = Table(
            name=table_name,
            system=system,
            namespace=namespace,
            environment=environment
        )
        described_table: Table[Any] = client.describe_table(stub)

        result: Records = client.get_records(described_table)

        if result.code != 200:
            raise RuntimeError(f"Unable to fetch Table: {described_table.name} from {described_table.environment}")

        count = 0
        for record in result.data:
            logger.debug(json.dumps(record, default=str, indent=2))
            count += 1

        logger.info(f"Fetched {count} record(s) from {table_name}")

def seeding_test(
    source_system: System,
    source_environment: str,
    source_namespace: str | None,
    target_system: System,
    target_environment: str,
    target_namespace: str | None,
    tables: list[str]
) -> int:

    source: DataSource = make_client(source_system, source_environment, source_namespace)
    target: DataSource = make_client(target_system, target_environment, target_namespace)

    for table_name in tables:
        stub: Table[Any] = Table(
            name=table_name,
            system=source_system,
            environment=source_environment,
            namespace=source_namespace,
        )
        described_source_table: Table[Any] = source.describe_table(stub)
        target.mutate_table(described_source_table)
        records: Records = source.get_records(described_source_table)
        target.load_records(action="insert",
                            table=described_source_table,
                            records=records,
                            )
    logger.info("Seeding.py Success!")
    return 0