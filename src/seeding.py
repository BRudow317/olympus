"""seeding.py"""
from __future__ import annotations
import logging
logger: logging.Logger = logging.getLogger(__name__)
import json

from src.sf.Salesforce import Salesforce
from src.oracle.Oracle import Oracle
from src.oracle.OracleDialect import to_oracle_snake
from src.models import DataSource, System, SystemPrefix, Table

from typing import TYPE_CHECKING, Any
if TYPE_CHECKING:
    from src.models import Records


def make_client(system: System, environment: str, namespace: str | None) -> DataSource:
    if system == System.ORACLE:
        return Oracle(environment, namespace)
    return Salesforce(environment, namespace)


def resolve_cross_system_name(
    source_name: str,
    source_system: System,
    target_system: System,
) -> str:
    """
    Resolve the table/object name to use on the target system.

    SF → Oracle: prepends 'SF_' and applies oracle snake-case → SF_CONTACT
    Oracle → SF: prepends 'ora_', lowercases, appends '__c' → ora_contact__c

    Round-trip detection: if the source name already carries the target system's
    own prefix, it's returning home — strip the prefix instead of double-prefixing.
      SF_CONTACT (Oracle) → SF  : strip 'SF_' → CONTACT
      ora_contact__c (SF) → Oracle: strip 'ora_' + '__c' → CONTACT
    """
    source_system = System(source_system)
    target_system = System(target_system)

    if source_system == target_system:
        return source_name

    target_prefix = str(SystemPrefix[target_system.name])  # 'ora' or 'sf'

    if target_system == System.ORACLE:
        # Round-trip: SF object was originally from Oracle (name starts with 'ora_')
        rtrip = f"{target_prefix}_"  # "ora_"
        if source_name.lower().startswith(rtrip):
            stripped = source_name[len(rtrip):]
            if stripped.lower().endswith("__c"):
                stripped = stripped[:-3]
            return to_oracle_snake(stripped)

        # Normal SF → Oracle: SF_CONTACT
        src_prefix = str(SystemPrefix[source_system.name]).upper()  # "SF"
        return f"{src_prefix}_{to_oracle_snake(source_name)}"

    else:  # target_system == System.SALESFORCE
        # Round-trip: Oracle table was originally from SF (name starts with 'SF_')
        rtrip = f"{target_prefix}_".upper()  # "SF_"
        if source_name.upper().startswith(rtrip):
            return source_name[len(rtrip):]  # SF is case-insensitive; bare name works

        # Normal Oracle → SF: ora_contact__c
        src_prefix = str(SystemPrefix[source_system.name])  # "ora"
        return f"{src_prefix}_{source_name.lower()}__c"


def fetch_test(
    system: System,
    environment: str,
    namespace: str | None = None,
    tables: list[str] = []
) -> None:
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
    source_system = System(source_system)
    target_system = System(target_system)

    source: DataSource = make_client(source_system, source_environment, source_namespace)
    target: DataSource = make_client(target_system, target_environment, target_namespace)

    for table_name in tables:
        source_stub: Table[Any] = Table(
            name=table_name,
            system=source_system,
            environment=source_environment,
            namespace=source_namespace,
        )
        described_source_table: Table[Any] = source.describe_table(source_stub)

        target_name: str = resolve_cross_system_name(table_name, source_system, target_system)
        target_stub: Table[Any] = Table(
            name=target_name,
            system=source_system,
            environment=target_environment,
            namespace=target_namespace,
            columns=described_source_table.columns,
        )

        target_table: Table[Any] = target.mutate_table(target_stub)
        records: Records = source.get_records(described_source_table)
        target.load_records(action="reset", table=target_table, records=records)

    logger.info("Seeding.py Success!")
    return 0
