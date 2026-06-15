"""SfBulk2Engine.py
https://salesforce.com
https://salesforce.com
"""
from __future__ import annotations
import logging
logger = logging.getLogger(__name__)
import csv
import io
import json
import time
import datetime
import math
from collections.abc import Iterator
from enum import StrEnum
from typing import Any, TYPE_CHECKING, TypedDict, AnyStr

# HttpMethod is needed at runtime (http.post/get/... below), not just for typing.
from src.sf.SfClient import HttpMethod as http

if TYPE_CHECKING:
    from src.sf.SfClient import SfClient

# ==============================================================================
# 1. BULK API LIMITATION CONSTANTS
# ==============================================================================
MAX_INGEST_JOB_FILE_SIZE = 150 * 1024 * 1024   # 150 MB per job
MAX_INGEST_JOB_PARALLELISM = 15
DEFAULT_QUERY_PAGE_SIZE = 50_000


# ==============================================================================
# 2. ENUMERATION DESCRIPTIONS
# ==============================================================================
class Operation(StrEnum):
    insert = "insert"
    upsert = "upsert"
    update = "update"
    delete = "delete"
    hard_delete = "hardDelete"
    query = "query"
    query_all = "queryAll"

class JobState(StrEnum):
    open = "Open"
    aborted = "Aborted"
    failed = "Failed"
    upload_complete = "UploadComplete"
    in_progress = "InProgress"
    job_complete = "JobComplete"

class ColumnDelimiter(StrEnum):
    BACKQUOTE = "BACKQUOTE"  # (`)
    CARET = "CARET"          # (^)
    COMMA = "COMMA"          # (,)
    PIPE = "PIPE"            # (|)
    SEMICOLON = "SEMICOLON"  # (;)
    TAB = "TAB"              # (\t)

class LineEnding(StrEnum):
    LF = "LF"
    CRLF = "CRLF"

class ResultsType(StrEnum):
    failed = "failedResults"
    successful = "successfulResults"
    unprocessed = "unprocessedRecords"


# ==============================================================================
# 3. BULK API 2.0 TYPE DICTIONARIES
# ==============================================================================
class QueryParameters(TypedDict, total=False):
    maxRecords: int
    locator: str

class QueryRecordsResult(TypedDict):
    locator: str
    number_of_records: int
    records: str

QueryResult = QueryRecordsResult

class QueryBytesResult(TypedDict):
    locator: str
    number_of_records: int
    data: bytes


# ==============================================================================
# 4. BULK 2 ORCHESTRATION CLIENT
# ==============================================================================
class Bulk2:
    _http: SfClient
    bulk2_url: str

    def __init__(self, http_client: SfClient) -> None:
        """Mounts connection context references and binds bulk tracking paths."""
        self._http = http_client
        self.bulk2_url = self._http.bulk2_url

    def __getattr__(self, name: str) -> Bulk2SObject:
        """Dynamic lookup mapping targeting contextual Bulk2 SObject managers."""
        if name.startswith("__"):
            return super().__getattribute__(name)
        return Bulk2SObject(object_name=name, bulk2_url=self.bulk2_url, http_client=self._http)

    def query(self, soql: str, **kwargs: Any) -> Iterator[bytes]:
        """Pipes bulk selection data via an encapsulated dynamic object dispatcher query."""
        yield from Bulk2SObject("_query", self.bulk2_url, self._http).query(soql, **kwargs)

    def query_all(self, soql: str, **kwargs: Any) -> Iterator[bytes]:
        """Pipes all data streams including soft-deleted items using queryAll actions."""
        yield from Bulk2SObject("_query", self.bulk2_url, self._http).query_all(soql, **kwargs)

class Bulk2SObject:
    """Bulk 2.0 interface for a specific SObject."""
    object_name: str
    bulk2_url: str
    _http: SfClient
    _client: _Bulk2Client

    def __init__(self, object_name: str, bulk2_url: str, http_client: SfClient) -> None:
        self.object_name = object_name
        self.bulk2_url = bulk2_url
        self._http = http_client
        self._client = _Bulk2Client(object_name, bulk2_url, http_client)

    @staticmethod
    def _records_to_csv_bytes(
        records: list[dict[str, Any]],
        line_ending: LineEnding = LineEnding.LF,
        include_header: bool = True,
    ) -> bytes:
        """Serialize a list of dicts to CSV bytes using Python's csv module."""
        if not records:
            return b""
        # Use the union of keys across all records (order-preserving). prepare_record
        # omits null fields, so rows can have differing key sets; a header built from
        # only the first row would silently drop or misalign columns.
        fieldnames = list(dict.fromkeys(k for record in records for k in record))
        buf = io.StringIO()
        writer = csv.DictWriter(
            buf,
            fieldnames=fieldnames,
            lineterminator="\n",
            extrasaction="ignore",
        )
        if include_header:
            writer.writeheader()
        writer.writerows(records)
        data = buf.getvalue().encode("utf-8")
        if line_ending == LineEnding.CRLF:
            data = data.replace(b"\n", b"\r\n")
        return data

    @staticmethod
    def _split_records(records: list[dict[str, Any]], chunk_size: int | None) -> list[list[dict[str, Any]]]:
        size = chunk_size or (MAX_INGEST_JOB_FILE_SIZE // 500)
        return [records[i:i + size] for i in range(0, len(records), size)]

    def _upload_chunk(
        self,
        operation: Operation,
        data: bytes,
        record_count: int,
        column_delimiter: ColumnDelimiter = ColumnDelimiter.COMMA,
        line_ending: LineEnding = LineEnding.LF,
        external_id_field: str | None = None,
        wait: int = 5,
    ) -> dict[str, int]:
        """Upload a single in-memory CSV bytes buffer as one Bulk 2.0 job."""
        res = self._client.create_job(
            operation,
            column_delimiter=column_delimiter,
            line_ending=line_ending,
            external_id_field=external_id_field,
        )
        job_id = res.get("id") or res.get("jobId") or res.get("job_id")
        if not job_id:
            raise Exception(f"Failed to create job for chunk upload: {res}")

        try:
            if res.get("state", "") != JobState.open.value:
                raise Exception(f"Job {job_id} created in unexpected state: {res.get('state', '')}")

            self._client.upload_job_data(job_id, data)
            self._client.close_job(job_id)
            self._client.wait_for_job(job_id, is_query=False, wait=wait)
            res = self._client.get_job(job_id, is_query=False)

            return {
                "numberRecordsFailed":    int(res.get("numberRecordsFailed", 0)),
                "numberRecordsProcessed": int(res.get("numberRecordsProcessed", 0)),
                "numberRecordsTotal":     record_count,
                "job_id":                 job_id,
            }

        except Exception:
            try:
                current = self._client.get_job(job_id, is_query=False)
                if current.get("state", "") in (
                    JobState.upload_complete.value,
                    JobState.in_progress.value,
                    JobState.open.value,
                ):
                    self._client.abort_job(job_id, is_query=False)
            except Exception as abort_err:
                logger.warning(f"Failed to abort job {job_id} during cleanup: {abort_err}")
            raise

    def _upload_table(
        self,
        operation: Operation,
        records: list[dict[str, Any]],
        chunk_size: int | None = None,
        column_delimiter: ColumnDelimiter = ColumnDelimiter.COMMA,
        line_ending: LineEnding = LineEnding.LF,
        external_id_field: str | None = None,
        wait: int = 5,
    ) -> list[dict[str, int]]:
        """Serialize each chunk of records to CSV bytes and upload as Bulk 2.0 jobs."""
        chunks = self._split_records(records, chunk_size)
        results: list[dict[str, int]] = []
        for chunk in chunks:
            data = self._records_to_csv_bytes(chunk, line_ending)
            result = self._upload_chunk(
                operation, data, len(chunk),
                column_delimiter, line_ending, external_id_field, wait,
            )
            results.append(result)
        return results

    def insert(
        self,
        records: list[dict[str, Any]],
        chunk_size: int | None = None,
        line_ending: LineEnding = LineEnding.LF,
        wait: int = 5,
    ) -> list[dict[str, int]]:
        return self._upload_table(Operation.insert, records, chunk_size=chunk_size, line_ending=line_ending, wait=wait)

    def upsert(
        self,
        records: list[dict[str, Any]],
        external_id_field: str,
        chunk_size: int | None = None,
        line_ending: LineEnding = LineEnding.LF,
        wait: int = 5,
    ) -> list[dict[str, int]]:
        return self._upload_table(
            Operation.upsert, records,
            chunk_size=chunk_size, line_ending=line_ending,
            external_id_field=external_id_field, wait=wait,
        )

    def update(
        self,
        records: list[dict[str, Any]],
        chunk_size: int | None = None,
        line_ending: LineEnding = LineEnding.LF,
        wait: int = 5,
    ) -> list[dict[str, int]]:
        return self._upload_table(Operation.update, records, chunk_size=chunk_size, line_ending=line_ending, wait=wait)

    def delete(
        self,
        records: list[dict[str, Any]],
        line_ending: LineEnding = LineEnding.LF,
        wait: int = 5,
    ) -> list[dict[str, int]]:
        self._constrain_id_only(records)
        return self._upload_table(Operation.delete, records, line_ending=line_ending, wait=wait)

    def hard_delete(
        self,
        records: list[dict[str, Any]],
        line_ending: LineEnding = LineEnding.LF,
        wait: int = 5,
    ) -> list[dict[str, int]]:
        self._constrain_id_only(records)
        return self._upload_table(Operation.hard_delete, records, line_ending=line_ending, wait=wait)

    @staticmethod
    def _constrain_id_only(records: list[dict[str, Any]]) -> None:
        if not records:
            return
        keys = [k.lower() for k in records[0].keys()]
        if keys != ["id"]:
            raise Exception(
                f"Delete operations require records with only an 'Id' key. Got: {list(records[0].keys())}"
            )

    def query(
        self,
        query: str,
        max_records: int = DEFAULT_QUERY_PAGE_SIZE,
        line_ending: LineEnding = LineEnding.LF,
        wait: int = 5,
    ) -> Iterator[bytes]:
        """Generator - yields raw CSV bytes per page."""
        res = self._client.create_job(
            Operation.query, query=query, line_ending=line_ending
        )
        job_id = res["id"]
        self._client.wait_for_job(job_id, is_query=True, wait=wait)

        locator = ""
        first = True
        while first or locator:
            first = False
            result = self._client.get_query_results(job_id, locator, max_records)
            locator = result["locator"]
            yield result["data"]

    def query_all(
        self,
        query: str,
        max_records: int = DEFAULT_QUERY_PAGE_SIZE,
        line_ending: LineEnding = LineEnding.LF,
        wait: int = 5,
    ) -> Iterator[bytes]:
        """Includes soft-deleted records. Same yield contract as query()."""
        res = self._client.create_job(
            Operation.query_all, query=query, line_ending=line_ending
        )
        job_id = res["id"]
        self._client.wait_for_job(job_id, is_query=True, wait=wait)

        locator = ""
        first = True
        while first or locator:
            first = False
            result = self._client.get_query_results(job_id, locator, max_records)
            locator = result["locator"]
            yield result["data"]

    def get_successful_records(self, job_id: str) -> bytes:
        return self._client.get_ingest_results(job_id, ResultsType.successful.value)

    def get_failed_records(self, job_id: str) -> bytes:
        return self._client.get_ingest_results(job_id, ResultsType.failed.value)

    def get_unprocessed_records(self, job_id: str) -> bytes:
        return self._client.get_ingest_results(job_id, ResultsType.unprocessed.value)

    def get_all_ingest_results(self, job_id: str) -> dict[str, bytes]:
        return {
            "successfulRecords":  self.get_successful_records(job_id),
            "failedRecords":      self.get_failed_records(job_id),
            "unprocessedRecords": self.get_unprocessed_records(job_id),
        }

class _Bulk2Client:
    """Low-level Bulk 2.0 HTTP operations. 
    Not for direct use outside Bulk2SObject.
    Do Not instantiate directly"""
    JSON_CONTENT_TYPE = "application/json"
    CSV_CONTENT_TYPE = "text/csv; charset=UTF-8"
    DEFAULT_WAIT_TIMEOUT_SECONDS = 7800
    MAX_CHECK_INTERVAL_SECONDS = 60.0

    def __init__(self, object_name: str, bulk2_url: str, http_client: SfClient) -> None:
        self.object_name = object_name
        self.bulk2_url = bulk2_url
        self._http = http_client

    def _headers(self, content_type: str | None = None, accept: str | None = None) -> dict[str, str]:
        return {
            "Content-Type": content_type or self.JSON_CONTENT_TYPE,
            "Accept": accept or self.JSON_CONTENT_TYPE,
        }

    def _url(self, job_id: str | None, is_query: bool) -> str:
        base = self.bulk2_url + ("/query" if is_query else "/ingest")
        return f"{base}/{job_id}" if job_id else base

    def create_job(
        self,
        operation: Operation,
        query: str | None = None,
        column_delimiter: ColumnDelimiter = ColumnDelimiter.COMMA,
        line_ending: LineEnding = LineEnding.LF,
        external_id_field: str | None = None,
    ) -> dict[str, Any]:
        is_query = operation in (Operation.query, Operation.query_all)
        payload: dict[str, Any] = {
            "operation":       operation.value,
            "columnDelimiter": column_delimiter.value,
            "lineEnding":      line_ending.value,
        }

        if is_query:
            if not query:
                raise Exception("query string is required for query jobs")
            payload["query"] = query
            headers = self._headers(self.JSON_CONTENT_TYPE, self.CSV_CONTENT_TYPE)
        else:
            payload["object"]      = self.object_name
            payload["contentType"] = "CSV"
            if external_id_field:
                payload["externalIdFieldName"] = external_id_field
            headers = self._headers()

        response = self._http.request(
            http.post, self._url(None, is_query),
            headers=headers,
            content=json.dumps(payload, allow_nan=False).encode(),
        )
        return response.json()

    def wait_for_job(
        self,
        job_id: str,
        is_query: bool,
        wait: float = 0.5,
    ) -> None:
        """Exponential backoff poll until JobComplete, Aborted, or Failed."""
        deadline  = datetime.datetime.now() + datetime.timedelta(seconds=self.DEFAULT_WAIT_TIMEOUT_SECONDS)
        delay     = 0.0
        delay_cnt = 0

        time.sleep(wait)

        while datetime.datetime.now() < deadline:
            info  = self.get_job(job_id, is_query)
            state = info["state"]
            if state == JobState.job_complete.value:
                return
            if state in (JobState.aborted.value, JobState.failed.value):
                raise Exception(
                    f"Job {job_id} ended with state '{state}': "
                    f"{info.get('errorMessage', info)}"
                )
            if delay < self.MAX_CHECK_INTERVAL_SECONDS:
                delay = wait + math.exp(delay_cnt) / 1000.0
                delay_cnt += 1
            time.sleep(delay)

        raise Exception(f"Job {job_id} timed out after {self.DEFAULT_WAIT_TIMEOUT_SECONDS}s")

    def get_job(self, job_id: str, is_query: bool) -> dict[str, Any]:
        response = self._http.request(http.get, self._url(job_id, is_query))
        return response.json()

    def close_job(self, job_id: str) -> dict[str, Any]:
        return self._set_state(job_id, is_query=False, state=JobState.upload_complete.value)

    def abort_job(self, job_id: str, is_query: bool) -> dict[str, Any]:
        return self._set_state(job_id, is_query=is_query, state=JobState.aborted.value)

    def delete_job(self, job_id: str, is_query: bool) -> dict[str, Any]:
        response = self._http.request(http.delete, self._url(job_id, is_query))
        return response.json()

    def _set_state(self, job_id: str, is_query: bool, state: str) -> dict[str, Any]:
        response = self._http.request(
            http.patch, self._url(job_id, is_query),
            content=json.dumps({"state": state}, allow_nan=False).encode(),
        )
        return response.json()

    def upload_job_data(self, job_id: str, data: bytes) -> None:
        if not data:
            raise Exception("data is required for ingest jobs")
        if len(data) > MAX_INGEST_JOB_FILE_SIZE:
            raise Exception(
                f"Chunk is {len(data)} bytes - exceeds the {MAX_INGEST_JOB_FILE_SIZE} byte "
                "Bulk 2.0 limit. Reduce chunk_size on the upload call."
            )
        response = self._http.request(
            http.put,
            self._url(job_id, is_query=False) + "/batches",
            headers=self._headers(self.CSV_CONTENT_TYPE, self.JSON_CONTENT_TYPE),
            content=data,
        )
        if response.status_code != 201:
            raise Exception(f"Upload failed. HTTP {response.status_code}: {response.text}")

    def get_query_results(
        self,
        job_id: str,
        locator: str = "",
        max_records: int = DEFAULT_QUERY_PAGE_SIZE,
    ) -> QueryBytesResult:
        params: dict[str, Any] = {"maxRecords": max_records}
        if locator and locator != "null":
            params["locator"] = locator

        response = self._http.request(
            http.get,
            self._url(job_id, is_query=True) + "/results",
            headers=self._headers(self.JSON_CONTENT_TYPE, self.CSV_CONTENT_TYPE),
            params=params,
        )

        next_locator = response.headers.get("Sforce-Locator", "")
        if next_locator == "null":
            next_locator = ""

        return {
            "locator":           next_locator,
            "number_of_records": int(response.headers["Sforce-NumberOfRecords"]),
            "data":              _filter_null_bytes(response.content),
        }

    def get_ingest_results(self, job_id: str, results_type: str) -> bytes:
        response = self._http.request(
            http.get,
            self._url(job_id, is_query=False) + f"/{results_type}",
            headers=self._headers(self.JSON_CONTENT_TYPE, self.CSV_CONTENT_TYPE),
        )
        return _filter_null_bytes(response.content)

def _filter_null_bytes(b: AnyStr) -> AnyStr:
    """https://github.com/airbytehq/airbyte/issues/8300"""
    if isinstance(b, str):
        return b.replace("\x00", "")
    if isinstance(b, bytes):
        return b.replace(b"\x00", b"")
    raise TypeError("Expected str or bytes")
