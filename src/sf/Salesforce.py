"""Salesforce.py"""
from __future__ import annotations

import logging
import csv
import io
from collections.abc import Iterator
from typing import Any
from urllib.parse import quote

from src.models import DataSource, Column, Records, Schema, System, Table
from src.sf.SfBulk2 import Bulk2
from src.sf.SfClient import SfClient, SKIP_NAMES, SKIP_SUFFIXES, HttpMethod as http, SalesforceRequestError
from src.sf.SfTypeMap import sf_type_to_python

logger: logging.Logger = logging.getLogger(__name__)

# ==============================================================================
# 1. OPTIMIZATION BUDGETS & EXTRACTION THRESHOLDS
# ==============================================================================

# A SOQL SELECT with many columns is sent on the GET /query request line, which
# Salesforce's front-end caps (HTTP 431). Above this encoded-length budget we
# route the query through the Bulk API 2.0 (SOQL travels in the POST body).
_SOQL_GET_MAX_ENCODED: int = 6000

# Objects with more than this many records are extracted via the Bulk API 2.0
# (one REST page is 2000 rows; beyond that, Bulk streams far more efficiently).
# All Bulk CSV stays in memory -- it is never written to disk.
_BULK_RECORD_THRESHOLD: int = 2000


# ==============================================================================
# 2. CORE SALESFORCE METADATA ENGINE
# ==============================================================================
class Salesforce(DataSource):
    client: SfClient
    bulk2_client: Bulk2
    max_retries = 1
    environment: str | None
    namespace: str | None

    def __init__(self, environment: str, namespace: str | None = None) -> None:
        """Establishes environment variables, instantiates REST drivers, and maps Bulk clients."""
        self.environment = environment
        self.namespace = namespace
        self.client = SfClient.client_constructor(environment)
        self.bulk2_client = Bulk2(self.client)

    def _request_records(self, soql: str, force_bulk: bool = False) -> Iterator[dict[str, Any]]:
        """Stream records for a SOQL query, choosing REST vs Bulk.

        Bulk API 2.0 is used when the caller forces it (e.g. high row count) or
        when the SELECT is too long for the GET /query request line (HTTP 431).
        As a safety net, a 431 from the REST path also falls back to Bulk. The
        431 can only occur on the initial request (pagination uses a short
        locator URL), so the fallback never re-yields already-emitted rows.
        """
        if force_bulk or len(quote(soql)) > _SOQL_GET_MAX_ENCODED:
            logger.info("Using Bulk API 2.0 for query (force_bulk=%s).", force_bulk)
            yield from self._bulk_query_records(soql)
            return

        try:
            yield from self._rest_query_records(soql)
        except SalesforceRequestError as e:
            if e.status_code != 431:
                raise
            logger.warning("REST query returned HTTP 431; retrying via Bulk API 2.0.")
            yield from self._bulk_query_records(soql)

    def _rest_query_records(self, soql: str) -> Iterator[dict[str, Any]]:
        """Paginates records sequentially from Salesforce using the standard synchronous REST pathway."""
        resp: Any = self.client.request(http.get, f"{self.client.services_url}/query/", params={"q": soql}).json()

        while True:
            for record in resp.get("records", []):
                record.pop("attributes", None)
                yield record

            next_path: Any = resp.get("nextRecordsUrl")
            if not next_path:
                break

            resp = self.client.request(http.get, f"{next_path}").json()

    def _bulk_query_records(self, soql: str) -> Iterator[dict[str, Any]]:
        """Run a SOQL query via Bulk API 2.0, yielding one dict per CSV row.

        Each page is a self-contained CSV block (with header). Values arrive as
        strings; callers apply the same cast_record() as the REST path, so empty
        strings, booleans, and numbers normalize identically.
        """
        for page in self.bulk2_client.query(soql):
            if not page:
                continue
            reader = csv.DictReader(io.StringIO(page.decode("utf-8")))
            for row in reader:
                yield dict(row)

    @staticmethod
    def is_migratable(obj: dict[str, Any]) -> bool:
        """Determines if a target SObject qualifies for business migration based on query rules."""
        name: Any = obj.get("name", "")
        
        if not obj.get("queryable", False):
            return False
            
        if name in SKIP_NAMES:
            return False
            
        return not any(name.endswith(sfx) for sfx in SKIP_SUFFIXES)

    def describe_schema(self, namespace: str | None = None, environment: str | None = None) -> Schema:
        """Queries the global org state to build a complete catalog listing of all migratable entities."""
        resp: list[dict[str, Any]] = self.client.describe()
        logger.debug(resp)
        tables: list[Table] = []
        
        for obj in resp:
            if not self.is_migratable(obj):
                continue
                
            tables.append(
                Table(
                    name=obj["name"],
                    system=System.salesforce,
                    alias=obj.get("label"),
                    namespace=namespace,
                    environment=environment,
                    properties={
                        "labelPlural": obj.get("labelPlural"),
                        "keyPrefix": obj.get("keyPrefix"),
                        "custom": obj.get("custom"),
                        "triggerable": obj.get("triggerable"),
                    },
                )
            )
            
        tables.sort(key=lambda t: t.name)
        return Schema(
            system=System.salesforce,
            namespace=namespace,
            environment=environment,
            tables=tables,
        )

    def describe_table(self, table: Table) -> Table:
        """Extracts complete structural field mappings, rules, and tracking definitions for an active table."""
        resp: Any = self.client.request(http.get, f"{self.client.services_url}/sobjects/{table.name}/describe").json()
        _SKIP_FIELD_TYPES = frozenset({"address", "location", "complexvalue", "base64"})
        
        columns: list[Column] = [
            self.describe_column(f, i)
            for i, f in enumerate(resp.get("fields", []))
            if f.get("type", "") not in _SKIP_FIELD_TYPES
        ]
        
        return Table(
            name=table.name,
            system=System.salesforce,
            alias=resp.get("label"),
            namespace=table.namespace,
            environment=table.environment,
            columns=columns,
            properties={
                "labelPlural": resp.get("labelPlural"),
                "keyPrefix": resp.get("keyPrefix"),
                "custom": resp.get("custom"),
                "triggerable": resp.get("triggerable"),
                "queryable": resp.get("queryable"),
                "searchable": resp.get("searchable"),
            },
        )

    def describe_column(self, field: dict[str, Any], position: int) -> Column:
        """Maps an individual native Salesforce field descriptor into an abstract core Column construct."""
        sf_type: Any = field.get("type", "")
        ref_to: Any | list[Any] = field.get("referenceTo") or []
        fk_map: dict[Any, str] = {ref: "Id" for ref in ref_to} if ref_to else {}
        
        enum_values: list[Any] = [
            pv["value"]
            for pv in (field.get("picklistValues") or [])
            if pv.get("active")
        ]
        
        return Column(
            name=field["name"],
            alias=field.get("label"),
            raw_type=sf_type,
            python_type=sf_type_to_python(sf_type),
            is_primary_key=field["name"] == "Id",
            is_unique=field.get("unique", False),
            is_nullable=field.get("nillable", True),
            is_read_only=not field.get("updateable", True),
            is_computed=field.get("calculated", False),
            is_deprecated=field.get("deprecatedAndHidden", False),
            is_hidden=field.get("deprecatedAndHidden", False),
            is_indexed=field.get("idLookup", False) or field.get("externalId", False),
            is_foreign_key=bool(ref_to),
            foreign_key_mapping=fk_map,
            max_length=field.get("length") or None,
            precision=field.get("precision") or None,
            scale=field.get("scale") or None,
            default_value=field.get("defaultValue"),
            enum_values=enum_values,
            ordinal_position=position,
            description=field.get("inlineHelpText"),
            properties={
                "soapType": field.get("soapType"),
                "filterable": field.get("filterable"),
                "sortable": field.get("sortable"),
                "groupable": field.get("groupable"),
                "externalId": field.get("externalId"),
            },
        )

# ==============================================================================
# 3. STREAM DATA LOADING & QUERY HANDLING IMPLEMENTATION
# ==============================================================================

    def query(self, statement: str, **_kwargs) -> Records:
        """Executes a direct raw SOQL query string statement, returning an encapsulated stream wrapper."""
        try:
            data: Iterator[dict[str, Any]] = self._request_records(statement)
            return Records(data=data, code=200, message="ok")
        except Exception as e:
            logger.error(f"Error in query: {e}")
            return Records(data=iter([]), code=500, message=str(e))

    def _exceeds_bulk_threshold(self, object_name: str) -> bool:
        """True if the object has more rows than the Bulk-routing threshold.
        Uses a cheap SELECT COUNT() probe. Any failure (e.g. the object is
        access-restricted) is swallowed and routes to REST, so the real error
        still surfaces lazily on the main query and the caller can skip the table
        rather than aborting the whole migration here.
        """
        try:
            return self.client.record_count(object_name) > _BULK_RECORD_THRESHOLD
        except Exception as e:
            logger.debug("Row-count probe failed for %s (%s); routing via REST.", object_name, e)
            return False

    def get_records(self, table: Table, **_kwargs) -> Records:
        """Fetches data blocks from the active entity layout with auto-calculating threshold routing hooks."""
        from src.sf.SfTypeMap import cast_record
        try:
            col_str = ", ".join(c.name for c in table.columns) if table.columns else "FIELDS(ALL)"
            soql: str = f"SELECT {col_str} FROM {table.name}"
            field_types: dict[str, str] = {
                c.name: c.raw_type
                for c in table.columns
                if c.raw_type
            }
            force_bulk: bool = self._exceeds_bulk_threshold(table.name)
            raw: Iterator[dict[str, Any]] = self._request_records(soql, force_bulk=force_bulk)
            data: Iterator[dict[str, Any]] = (
                cast_record(r, field_types) for r in raw
            ) if field_types else raw
            return Records(data=data, code=200, message="ok")
        except Exception as e:
            logger.error(f"Error in get_records: {e}")
            return Records(data=iter([]), code=500, message=str(e))

    def is_healthy(self) -> bool:
        """Return True if the Salesforce org is reachable and authenticated."""
        try:
            return self.client.is_healthy()
        except Exception as e:
            logger.error("Salesforce health check failed for %s: %s", self.environment, e)
            return False

    def mutate_table(self, table: Table, source_system: System | None = None, **kwargs) -> Table:
        """Discovery pass mapping checking for targeted structural presence in the source org context."""
        try:
            return self.describe_table(table)
        except Exception as e:
            raise RuntimeError(
                f"Salesforce object '{table.name}' could not be described. "
                f"Ensure the object exists in the org. Error: {e}"
            ) from e

    def load_records(self, action: str, table: Table, records: Records, **kwargs) -> None:
        """Pushes data blocks to Salesforce via Bulk API 2.0 based on structural field validation states."""
        from src.sf.SfTypeMap import prepare_record
        
        writable: set[str] = {
            c.name for c in table.columns
            if not c.is_read_only and not c.is_computed
        }
        
        raw_rows = list(records.data)
        if not raw_rows:
            logger.info("load_records: no records to load into %s", table.name)
            return
            
        sobj = getattr(self.bulk2_client, table.name)
        
        def _prep(row: dict, exclude_id: bool = False, keep: set[str] | None = None) -> dict:
            allowed = writable | (keep or set())
            return prepare_record({
                k: v for k, v in row.items()
                if k in allowed and not (exclude_id and k == "Id")
            })
            
        if action == "insert":
            results = sobj.insert([_prep(r, exclude_id=True) for r in raw_rows])
        elif action in ("reset", "full_load"):
            # Salesforce sobjects can't be dropped, so a full_load is the closest
            # equivalent: delete every existing record, then insert fresh.
            existing = list(self._request_records(f"SELECT Id FROM {table.name}"))
            if existing:
                sobj.delete([{"Id": r["Id"]} for r in existing])
            results = sobj.insert([_prep(r, exclude_id=True) for r in raw_rows])
        elif action == "upsert":
            # Prefer an explicit field, then a column flagged externalId, else Id.
            external_id_field: str | None = kwargs.get("external_id_field")
            if not external_id_field:
                ext_cols = [c for c in table.columns if c.properties.get("externalId")]
                external_id_field = ext_cols[0].name if ext_cols else "Id"
                
            # The external id must be written even when it is read-only (e.g. Id).
            results = sobj.upsert(
                [_prep(r, keep={external_id_field}) for r in raw_rows],
                external_id_field=external_id_field,
            )
        elif action == "update":
            results = sobj.update([_prep(r, keep={"Id"}) for r in raw_rows])
        else:
            raise ValueError(f"Unknown load action '{action}' for Salesforce target")
            
        total: int = sum(r.get("numberRecordsProcessed", 0) for r in results)
        failed: int = sum(r.get("numberRecordsFailed", 0) for r in results)
        
        logger.info(
            "load_records: %s %d records into %s (%d failed)",
            action, total, table.name, failed,
        )
        
        if failed:
            logger.warning("load_records: %d records failed in %s", failed, table.name)