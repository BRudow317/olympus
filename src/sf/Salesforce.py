"""Salesforce.py"""
from __future__ import annotations
import logging
logger: logging.Logger = logging.getLogger(__name__)
import os
from collections.abc import Iterator
from typing import Any

from src.models import DataSource
from src.sf.SfBulk2 import Bulk2
from src.sf.SfRest import Rest
from src.models import Column, Records, Schema, System, Table
from src.sf.SfModels import SKIP_NAMES, SKIP_SUFFIXES
from src.sf.SfClient import SfClient
from src.sf.SfTypeMap import sf_type_to_python

def build_column(field: dict[str, Any], position: int) -> Column:
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

def _is_migratable(obj: dict[str, Any]) -> bool:
    name: Any = obj.get("name", "")
    if not obj.get("queryable", False):
        return False
    if name in SKIP_NAMES:
        return False
    return not any(name.endswith(sfx) for sfx in SKIP_SUFFIXES)

class Salesforce(DataSource):
    _client: SfClient
    _rest_client: Rest
    _bulk2_client: Bulk2
    max_retries = 1

    def __init__(self, environment: str, namespace: str | None = None) -> None:
        _base_url: str = os.getenv(f"SF_{environment}_BASE_URL", "")
        _consumer_key: str = os.getenv(f"SF_{environment}_CONSUMER_KEY", "")
        _consumer_secret: str = os.getenv(f"SF_{environment}_CONSUMER_SECRET", "")
        _api_version: str = os.getenv(f"SF_{environment}_API_VERSION", "66.0")

        if not all([_base_url, _consumer_key, _consumer_secret]):
            raise ValueError(f"Missing Salesforce connection info for environment '{environment}'. Required: SF_{environment}_BASE_URL, SF_{environment}_CONSUMER_KEY, SF_{environment}_CONSUMER_SECRET")

        self._client = SfClient(
            base_url=_base_url,
            consumer_key=_consumer_key,
            consumer_secret=_consumer_secret,
            access_token=None,
            api_version=_api_version,
            max_retries=self.max_retries,
        )

        self._rest_client = Rest(self._client)
        self._bulk2_client = Bulk2(self._client)

    def request_records(self, soql: str) -> Iterator[dict[str, Any]]:
        resp: Any = self._client.request("GET", "query/", params={"q": soql}).json()
        while True:
            for record in resp.get("records", []):
                record.pop("attributes", None)
                yield record
            next_path: Any = resp.get("nextRecordsUrl")
            if not next_path:
                break
            resp: Any = self._client.request("GET", f"{next_path}").json()

    def describe_schema(self, namespace: str | None = None, environment: str | None = None) -> Schema:
        resp: Any = self._client.request("GET", "sobjects/").json()
        print(resp)

        tables: list[Table] = []
        for obj in resp.get("sobjects", []):
            if not _is_migratable(obj):
                continue
            tables.append(
                Table(
                    name=obj["name"],
                    system=System.SALESFORCE,
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

        tables.sort(key = lambda t: t.name)
        return Schema(
            system=System.SALESFORCE,
            namespace=namespace,
            environment=environment,
            tables=tables,
        )

    def describe_table(self, table: Table) -> Table:
        resp: Any = self._client.request("GET", f"sobjects/{table.name}/describe/").json()
        columns: list[Column] = [build_column(f, i) for i, f in enumerate(resp.get("fields", []))]
        return Table(
            name=table.name,
            system=System.SALESFORCE,
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

    def query(self, statement: str, **kwargs) -> Records:
        try:
            data: Iterator[dict[str, Any]] = self.request_records(statement)
            return Records(data=data, code=200, message="ok")
        except Exception as e:
            logger.error(f"Error in query: {e}")
            return Records(data=iter([]), code=500, message=str(e))

    def get_records(self, table: Table, **kwargs) -> Records:
        try:
            col_str = ", ".join(c.name for c in table.columns) if table.columns else "FIELDS(ALL)"
            soql: str = f"SELECT {col_str} FROM {table.name}"
            data: Iterator[dict[str, Any]] = self.request_records(soql)
            return Records(data=data, code=200, message="ok")
        except Exception as e:
            logger.error(f"Error in get_records: {e}")
            return Records(data=iter([]), code=500, message=str(e))

    def mutate_table(self, table: Table) -> Table:
        raise NotImplementedError("mutate_table is not implemented for Salesforce")

    def load_records(self, action: str, table: Table, records: Records, **kwargs) -> int:
        return 1