"""seeding.py

Orchestrates cross-system migrations using the system-agnostic DataSource
protocol. The same flow handles all four directions (sf->oracle, oracle->sf,
oracle->oracle, sf->sf):

    describe source table -> mutate (create/align) target table ->
    stream source records -> rename keys to target column names -> load.
"""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any
from collections.abc import Iterator

from src.sf.Salesforce import Salesforce
from src.oracle.Oracle import Oracle
from src.models import DataSource, System, Table, Schema, Records
from src.oracle.OracleModels import to_oracle_snake
from src.sf.SfClient import SalesforceRequestError
from src.redaction import Redactor, RedactionScope
from src.settings.rules import apply_structural_rules, redaction_fields
from src.settings.guard import DestructiveGuard, DEFAULT_DESTRUCTIVE_MODE
from src import settings

if TYPE_CHECKING:
    from src.models import Column

logger: logging.Logger = logging.getLogger(__name__)

def get_datasource(system: System, environment: str, namespace: str | None) -> DataSource:
    """Instantiates the specialized concrete driver implementing the DataSource protocol."""
    if System(system) == System.oracle:
        return Oracle(environment, namespace)
    return Salesforce(environment, namespace)


def resolve_cross_system_name(
    source_name: str,
    source_system: System,
    target_system: System,
) -> str:
    """Resolve the table/object name to use on the target system.

    SF -> Oracle: prepends 'SF_' ex) Contact -> SF_CONTACT
    Oracle -> SF: prepends 'ora_', lowercases, appends '__c' ex) DEMOGRAPHIC_TABLE -> ora_demographic_table__c

    Round-trip detection: if the name already carries the target system's prefix
    it is returning home, so the prefix is stripped instead of double-prefixed.
    """
    source_system = System(source_system)
    target_system = System(target_system)

    if source_system == target_system:
        return source_name

    if source_system == System.salesforce and target_system == System.oracle:
        if source_name.lower().startswith("ora_"):
            stripped = source_name[4:]
            if stripped.lower().endswith("__c"):
                stripped = stripped[:-3]
            return stripped
        return f"SF_{source_name}"

    if source_system == System.oracle and target_system == System.salesforce:
        if source_name.upper().startswith("SF_"):
            return source_name[3:]
        return f"ora_{source_name.lower()}__c"

    return source_name

def _build_rename_map(
    source_columns: list[Column], target_columns: list[Column]
) -> dict[str, str]:
    """Correlates columns across boundaries using canonical Oracle snake-case signatures."""
    target_by_canon: dict[str, str] = {
        to_oracle_snake(c.name): c.name for c in target_columns
    }
    rename: dict[str, str] = {}
    for col in source_columns:
        target_name = target_by_canon.get(to_oracle_snake(col.name))
        if target_name is not None:
            rename[col.name] = target_name
    return rename


def _rename_records(records: Records, rename: dict[str, str]) -> Records:
    """Lazily rewrite record keys from source names to target column names."""
    if not rename:
        return records

    source_data = records.data

    def renamed() -> Iterator[dict[str, Any]]:
        for row in source_data:
            yield {rename.get(k, k): v for k, v in row.items()}

    return Records(
        data=renamed(),
        columns=records.columns,
        code=records.code,
        message=records.message,
    )


def _redact_records(
    records: Records, plan: dict[str, RedactionScope], redactor: Redactor
) -> Records:
    """Lazily apply a redaction plan to records before they leave the source.

    The plan is keyed by SOURCE field name, so this must run before key renaming.
    Field/scan scope handling lives behind the boundary in Redactor.redact_record;
    here we just stream. A no-op when the redactor is disabled or the plan empty.
    """
    if not redactor.enabled or not plan:
        return records

    source_data = records.data

    def redacted() -> Iterator[dict[str, Any]]:
        for row in source_data:
            yield redactor.redact_record(row, plan)

    return Records(
        data=redacted(),
        columns=records.columns,
        code=records.code,
        message=records.message,
    )


def seeding(
    source_system: System,
    source_environment: str,
    source_namespace: str | None,
    target_system: System,
    target_environment: str,
    target_namespace: str | None,
    tables: list[str],
    action: str,
    external_id_field: str | None = None,
    redaction: str = "none",
    destructive: str = DEFAULT_DESTRUCTIVE_MODE,
) -> DataSource:
    """Orchestrates schema compilation, extraction loops, re-keying maps, and loader targets."""
    # Publish the program's tuning knobs into the environment before any connector
    # work, so the connectors' use-time reads observe settings (explicit env wins).
    settings.apply()

    source_system = System(source_system)
    target_system = System(target_system)

    source: DataSource = get_datasource(source_system, source_environment, source_namespace)
    target: DataSource = get_datasource(target_system, target_environment, target_namespace)
    redactor = Redactor(redaction)

    # The guard gates destructive ops on whichever side performs them (always the
    # target today, but attach to both so a future read-side op is covered too).
    guard = DestructiveGuard(destructive)
    source.guard = guard
    target.guard = guard

    skipped: list[str] = []
    
    if tables and tables[0] == "*" and len(tables) == 1:
        schema: Schema = source.describe_schema(namespace=source.namespace)
        tables = [t.name for t in schema.tables]

    for table_name in tables:
        try:
            source_stub: Table[Any] = Table(
                name=table_name,
                system=source_system,
                environment=source_environment,
                namespace=source_namespace,
            )
            described_source_table: Table[Any] = source.describe_table(source_stub)

            # Force-type overrides (e.g. Case.Description -> CLOB) are stamped onto
            # the source columns so they flow through the target's type translation.
            apply_structural_rules(source_system, table_name, described_source_table.columns)

            target_name: str = resolve_cross_system_name(table_name, source_system, target_system)
            target_stub: Table[Any] = Table(
                name=target_name,
                system=target_system,
                environment=target_environment,
                namespace=target_namespace,
                columns=described_source_table.columns,
            )

            target_table: Table[Any] = target.mutate_table(
                target_stub, source_system=source_system, action=action
            )

            records: Records = source.get_records(described_source_table)
            if records.code != 200:
                raise RuntimeError(
                    f"Failed to read records from {described_source_table.name}: {records.message}"
                )

            # Redact sensitive fields while records are still keyed by their
            # source names, before they are renamed to target column names.
            plan = redaction_fields(source_system, table_name, described_source_table.columns)
            records = _redact_records(records, plan, redactor)

            rename = _build_rename_map(described_source_table.columns, target_table.columns)
            records = _rename_records(records, rename)

            # Note: source records stream lazily, so a Salesforce access denial on
            # the underlying SOQL query surfaces here, during load, not above.
            target.load_records(
                action=action,
                table=target_table,
                records=records,
                external_id_field=external_id_field,
            )
            logger.info("Migrated %s -> %s (action=%s)", table_name, target_name, action)
        
        except SalesforceRequestError as e:
            if not e.is_access_error:
                raise
            # The authenticated user can't read this object; skip it and keep
            # going so one inaccessible table doesn't abort the whole migration.
            logger.warning(
                "Skipping '%s': Salesforce access denied (%s).",
                table_name,
                ", ".join(e.error_codes) or e.status_code,
            )
            skipped.append(table_name)

    migrated_count = len(tables) - len(skipped)
    
    if skipped:
        logger.info(
            "Seeding complete: %d migrated, %d skipped (access denied): %s",
            migrated_count, len(skipped), ", ".join(skipped),
        )
    else:
        logger.info("Seeding complete: %d table(s).", migrated_count)
    
    return target
