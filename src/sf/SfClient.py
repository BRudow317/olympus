"""SfClient.py"""
from __future__ import annotations

import logging
logger = logging.getLogger(__name__)

import re
import os
import base64
import json
# from collections.abc import MutableMapping
from datetime import datetime
from pathlib import Path
from typing import Any, Iterator
from urllib.parse import quote_plus

import httpx

from src.sf.SfModels import (
    SKIP_NAMES,
    SKIP_SUFFIXES,
    HttpMethod,
    HttpMethod as http,
    SalesforceRequestError,
)

class SfClient:
    httpx_client: httpx.Client
    base_url: str
    auth_url: str
    services_url: str
    api_version: str
    environment: str
    api_usage: str | dict[str, Any]
    _access_token: str
    _max_retries: int

    def __init__(
        self,
        environment: str,
        base_url: str,
        auth_url: str = '/services/oauth2/token',
        consumer_key: str | None = None,
        consumer_secret: str | None = None,
        access_token: str | None = None,
        api_version: str = '66.0',
        max_retries: int = 1,
    ) -> None:
        self.base_url = str(self.resolve_url(base_url=base_url))
        logger.debug(f'"SF_{environment}_BASE_URL" : "{self.base_url}"')
        
        self.auth_url = str(self.resolve_url(auth_url))
        logger.debug(f'"SF_{environment}_AUTH_URL" : "{self.auth_url}"')
        
        _ck, _cs, _url = consumer_key, consumer_secret, self.auth_url
        self._get_token = lambda: self._auth_callout(_ck, _cs, _url)
        
        if not access_token:
            self._access_token = self._get_token()
        else:
            self._access_token = access_token
            
        self.httpx_client = httpx.Client(
            headers=self._auth_headers(self._access_token),
            timeout=httpx.Timeout(30.0, connect=10.0),
        )
        
        self.api_version = api_version
        self.services_url = str(self.resolve_url(f'/services/data/v{self.api_version}/', self.base_url))
        logger.debug(f'"services_url" : "{self.services_url}"')
        
        self.api_usage = {}
        self._max_retries = max_retries

    @classmethod
    def client_constructor(
        cls, 
        environment: str, 
        max_retries: int = 1, 
        access_token: str | None = None
    ) -> SfClient:
        environment = environment.upper()
        
        base_url: str = os.getenv(f"SF_{environment}_BASE_URL", "")
        consumer_key: str = os.getenv(f"SF_{environment}_CONSUMER_KEY", "")
        consumer_secret: str = os.getenv(f"SF_{environment}_CONSUMER_SECRET", "")
        api_version: str = os.getenv(f"SF_{environment}_API_VERSION", "66.0")
        # auth_url: str = os.getenv(f"SF_{environment}_AUTH_URL", "")
        
        if not all([base_url, consumer_key, consumer_secret]):
            raise ValueError(
                f"Missing Salesforce connection info for environment '{environment}'. "
                f"Required variables: SF_{environment}_BASE_URL, SF_{environment}_CONSUMER_KEY, "
                f"SF_{environment}_CONSUMER_SECRET, SF_{environment}_AUTH_URL"
            )
            
        return cls(
            environment=environment,
            base_url=base_url,
            # auth_url=auth_url,
            consumer_key=consumer_key,
            consumer_secret=consumer_secret,
            access_token=access_token,
            api_version=api_version,
            max_retries=max_retries,
        )
    
    @staticmethod
    def _auth_callout(
        consumer_key: str | None = None,
        consumer_secret: str | None = None,
        auth_url: str | None = None,
    ) -> str:
        """Fetch an OAuth access token using the Client Credentials flow.
        Submits request as form-encoded data (application/x-www-form-urlencoded).
        """
        if not all([consumer_key, consumer_secret, auth_url]):
            env_debug = {
                k: ("*" * len(v) if v else "[EMPTY STRING]")
                for k, v in os.environ.items()
                if k.startswith("SF_")
            }
            raise RuntimeError(f"Missing required environment variables for authentication: {env_debug}")
            
        logger.info(f"\nInitiating Salesforce OAuth callout to: {auth_url}\n...")
        
        # Build transport with retry logic for network-level resilience
        transport = httpx.HTTPTransport(retries=3)
        try:
            with httpx.Client(transport=transport, timeout=15.0) as client:
                response = client.post(
                    url=str(auth_url),
                    data={
                        "grant_type": "client_credentials",
                        "client_id": consumer_key,
                        "client_secret": consumer_secret,
                    },
                )
        except httpx.RequestError as exc:
            logger.error(f"Network transport error during authentication: {exc}")
            raise RuntimeError(f"Failed to connect to Salesforce auth endpoint: {exc}") from exc
            
        try:
            payload = response.json()
        except Exception as exc:
            raise RuntimeError(f"Non-JSON response received ({response.status_code}): {response.text}") from exc
            
        if response.status_code != 200:
            error_type = payload.get("error", "unknown_error")
            error_desc = payload.get("error_description", "No description provided by Salesforce.")
            
            # Actionable feedback logs for External Client App troubleshooting
            logger.error(f"Salesforce OAuth Rejected [{response.status_code}]: {error_type} - {error_desc}")
            pretty_json = json.dumps(payload, indent=4)
            logger.error(f"\n\n{pretty_json}\n\n")
            
            if error_type == "invalid_grant":
                logger.error("Check if the External Client App 'Enable Client Credentials Flow' policy is checked and a 'Run As' execution user is set.")
            elif error_type == "invalid_client":
                logger.error("Verify that your Client ID (Consumer Key) and Client Secret match the values in Salesforce exactly.")
                
            raise RuntimeError(f"Salesforce Auth Failure ({error_type}): {error_desc}")
            
        logger.info("Salesforce OAuth authentication successful.")
        return str(payload.get("access_token", ""))
    
    @staticmethod
    def _auth_headers(token: str) -> dict[str, str]:
        return {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        }

    def resolve_url(self, url_part: str | None = None, base_url: str | None = None) -> httpx.URL:
        quick_check = httpx.URL(str(url_part or ""))
        if quick_check.is_absolute_url:
            return quick_check
            
        target_base = base_url or self.base_url
        url = httpx.URL(target_base).join(str(url_part or ""))
        
        if not url.is_absolute_url:
            raise ValueError("SfClient.resolve_url failed, no base_url given or found.")
            
        logger.debug(f"Resolved URL: {url}")
        return url

    def update_headers(self, headers: dict) -> None:
        self.httpx_client.headers.update(headers)

    def close(self) -> None:
        self.httpx_client.close()

    def __enter__(self) -> SfClient:
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

    def request(
        self, method: HttpMethod, url: httpx.URL | str, **kwargs
    ) -> httpx.Response:
        url_str = str(url)
        
        if not url_str.startswith("http://") and not url_str.startswith("https://"):
            if not url_str.startswith("/services"):
                url = httpx.URL(self.services_url).join(url_str.lstrip("/"))
            else:
                url = self.resolve_url(url_str)
        else:
            url = httpx.URL(url_str)
            
        response: httpx.Response = self.httpx_client.request(method=method, url=url, **kwargs)
        
        if response.status_code == 401:
            self._handle_401(response)
            response = self.httpx_client.request(method, url, **kwargs)
            
        if response.status_code >= 300:
            raise SalesforceRequestError(response.status_code, method, url, response.text)
            
        return response

    def is_healthy(self) -> bool:
        url = self.resolve_url("/services/oauth2/userinfo")
        user_info_response = self.httpx_client.get(url)
        user_info_response.raise_for_status()
        
        logger.debug(f"Connection is healthy. Org ID: {str(user_info_response.json().get('organization_id'))}")
        return True

    def __getattr__(self, name: str) -> "SObject":
        if name.startswith("__"):
            return super().__getattribute__(name)
        return SObject(object_name=name, http_client=self)
    
    def describe(self, **kwargs: Any) -> list[dict[str, Any]]:
        """Global describe filtered to business-data objects only."""
        all_objects: list[dict[str, Any]] = self.describe_all(**kwargs).get("sobjects", [])
        
        def is_migratable(obj: dict[str, Any]) -> bool:
            if not obj.get("queryable") or not obj.get("retrieveable"):
                return False
            if not obj.get("layoutable") and not obj.get("searchable"):
                return False
            name = obj.get("name", "")
            if any(name.endswith(s) for s in SKIP_SUFFIXES):
                return False
            if name in SKIP_NAMES:
                return False
            return True
            
        filtered_objects = [obj for obj in all_objects if is_migratable(obj)]
        logger.debug(filtered_objects)
        return filtered_objects

    def describe_all(self, **kwargs: Any) -> dict[str, Any]:
        """Global describe - all available SObjects."""
        response = self.request(http.get, "sobjects", **kwargs)
        logger.debug(response.json())
        return response.json()

    def search(self, search_str: str) -> dict[str, Any]:
        """Execute a SOSL search."""
        response = self.request(http.get, "search/", params={"q": search_str})
        return response.json() or {}

    def quick_search(self, search_str: str) -> dict[str, Any]:
        """Wraps search string in FIND {...}."""
        return self.search(f"FIND {{{search_str}}}")

    def query(
        self, query_str: str, include_deleted: bool = False, **kwargs: Any,
    ) -> dict[str, Any]:
        """Execute a SOQL query. Returns first page."""
        endpoint = "queryAll/" if include_deleted else "query/"
        response = self.request(
            http.get, endpoint, params={"q": query_str}, **kwargs
        )
        return response.json()

    def record_count(self, sobject: str, include_deleted: bool = False) -> int:
        """Return the row count of an SObject via a lightweight SELECT COUNT()."""
        resp = self.query(f"SELECT COUNT() FROM {sobject}", include_deleted=include_deleted)
        return int(resp.get("totalSize", 0))

    def query_more(
        self,
        next_records_identifier: str,
        identifier_is_url: bool = False,
        include_deleted: bool = False,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Fetch the next page of SOQL results via nextRecordsUrl."""
        if identifier_is_url:
            endpoint = next_records_identifier
        else:
            base = "queryAll" if include_deleted else "query"
            endpoint = f"{base}/{next_records_identifier}"
            
        response = self.request(http.get, endpoint, **kwargs)
        return response.json()

    def lazy_query(
        self, query_str: str, include_deleted: bool = False, **kwargs: Any,
    ) -> Iterator[Any]:
        """Generator - lazily yields individual records across all pages."""
        result = self.query(query_str, include_deleted=include_deleted, **kwargs)
        
        while True:
            for record in result["records"]:
                yield record
            if result["done"]:
                return
            result = self.query_more(
                result["nextRecordsUrl"], identifier_is_url=True, **kwargs
            )

    def query_all(
        self, query_str: str, include_deleted: bool = False, **kwargs: Any,
    ) -> dict[str, Any]:
        """Eagerly fetch all records across all pages into memory."""
        records = list(self.lazy_query(query_str, include_deleted=include_deleted, **kwargs))
        return {"records": records, "totalSize": len(records), "done": True}
    
    def apex_execute(
        self, 
        action: str | None = None, 
        method: HttpMethod = http.get, 
        data: dict[str, Any] | None = None, 
        **kwargs: Any
    ) -> Any:
        """Makes an HTTP request to an APEX REST endpoint."""
        json_data = json.dumps(data) if data is not None else None
        
        result = self.request(
            method=method, 
            url="/services/apexrest", 
            data=json_data, 
            **kwargs
        )
        
        try:
            return result.json()
        except Exception:
            return result.text

    def is_sandbox(self) -> bool:
        """Return whether the org is a sandbox."""
        result = self.query_all("SELECT IsSandbox FROM Organization LIMIT 1")
        records = result.get("records", [])
        
        if not records:
            return False
            
        return records[0].get("IsSandbox", False)

    def limits(self, **kwargs: Any) -> dict[str, Any]:
        """Org REST API limits."""
        response = self.request(http.get, "limits/", **kwargs)
        limit_info = response.headers.get("Sforce-Limit-Info")
        
        if limit_info:
            api_usage = re.match(r"[^-]?api-usage=(?P<used>\d+)/(?P<tot>\d+)", limit_info)
            pau = re.match(
                r".+per-app-api-usage=(?P<u>\d+)/(?P<t>\d+)\(appName=(?P<n>.+)\)", 
                limit_info,
            )
            
        return response.json()

    def _handle_401(self, response: httpx.Response) -> None:
        """Refresh the token on INVALID_SESSION_ID."""
        try:
            error_code = response.json().get("errorCode")
        except Exception:
            return
            
        if error_code != "INVALID_SESSION_ID":
            return
            
        if getattr(self, "_get_token", None) is None:
            raise Exception("Session expired and no credentials are available to refresh the token.")
            
        logger.info("Session expired. Refreshing token...")
        
        for attempt in range(1, self._max_retries + 1):
            new_token = self._get_token()
            if new_token and new_token != self._access_token:
                self._access_token = new_token
                headers = self._auth_headers(self._access_token)
                self.update_headers(headers)
                return
                
            logger.warning(f"Token refresh attempt {attempt} returned same or empty token.")
            
        raise Exception("Max retries exceeded: could not refresh Salesforce token.")
    





class SObject:
    """Interface to a specific Salesforce SObject."""
    object_name: str
    client: SfClient
    base_endpoint: str

    def __init__(self, object_name: str, http_client: SfClient) -> None:
        self.object_name = object_name
        self.client = http_client
        self.base_endpoint = f"sobjects/{object_name}"

    def metadata(self, **kwargs: Any) -> dict[str, Any]:
        response = self.client.request(http.get, self.base_endpoint, **kwargs)
        return response.json()

    def describe(self, **kwargs: Any) -> dict[str, Any]:
        response = self.client.request(
            http.get, f"{self.base_endpoint}/describe", **kwargs
        )
        return response.json()

    def describe_layout(self, record_id: str, **kwargs: Any) -> dict[str, Any]:
        response = self.client.request(
            http.get, f"{self.base_endpoint}/describe/layouts/{record_id}", **kwargs
        )
        return response.json()

    def get(self, record_id: str, **kwargs: Any) -> dict[str, Any]:
        response = self.client.request(
            http.get, f"{self.base_endpoint}/{record_id}", **kwargs
        )
        return response.json()

    def get_by_custom_id(
        self, custom_id_field: str, custom_id: str, **kwargs: Any
    ) -> dict[str, Any]:
        endpoint = f"{self.base_endpoint}/{custom_id_field}/{quote_plus(custom_id)}"
        response = self.client.request(http.get, endpoint, **kwargs)
        return response.json()

    def create(self, data: dict[str, Any], **kwargs: Any) -> dict[str, Any]:
        response = self.client.request(
            http.post, self.base_endpoint, json=data, **kwargs
        )
        return response.json()

    def upsert(
        self,
        external_id_field: str,
        external_id_value: str,
        data: dict[str, Any],
        raw_response: bool = False,
        **kwargs: Any,
    ) -> int | httpx.Response:
        endpoint = f"{self.base_endpoint}/{external_id_field}/{quote_plus(external_id_value)}"
        response = self.client.request(
            http.patch, endpoint, json=data, **kwargs
        )
        return self._raw_response(response, raw_response)

    def update(
        self,
        record_id: str,
        data: dict[str, Any],
        raw_response: bool = False,
        **kwargs: Any,
    ) -> int | httpx.Response:
        response = self.client.request(
            http.patch, f"{self.base_endpoint}/{record_id}", json=data, **kwargs
        )
        return self._raw_response(response, raw_response)

    def delete(
        self,
        record_id: str,
        raw_response: bool = False,
        **kwargs: Any,
    ) -> int | httpx.Response:
        response = self.client.request(
            http.delete, f"{self.base_endpoint}/{record_id}", **kwargs
        )
        return self._raw_response(response, raw_response)

    def deleted(
        self, start: datetime, end: datetime, **kwargs: Any
    ) -> dict[str, Any]:
        endpoint = (
            f"{self.base_endpoint}/deleted/"
            f"?start={start.isoformat()}&end={end.isoformat()}"
        )
        response = self.client.request(http.get, endpoint, **kwargs)
        return response.json()

    def updated(
        self, start: datetime, end: datetime, **kwargs: Any
    ) -> dict[str, Any]:
        endpoint = (
            f"{self.base_endpoint}/updated/"
            f"?start={start.isoformat()}&end={end.isoformat()}"
        )
        response = self.client.request(http.get, endpoint, **kwargs)
        return response.json()

    def upload_base64(
        self,
        file_path: str,
        base64_field: str = "Body",
        **kwargs: Any,
    ) -> httpx.Response:
        body = base64.b64encode(Path(file_path).read_bytes()).decode()
        return self.client.request(
            http.post, self.base_endpoint, json={base64_field: body}, **kwargs
        )
    
    def update_base64(
        self,
        record_id: str,
        file_path: str,
        base64_field: str = "Body",
        raw_response: bool = False,
        **kwargs: Any,
    ) -> int | httpx.Response:
        body = base64.b64encode(Path(file_path).read_bytes()).decode()
        response = self.client.request(
            http.patch,
            f"{self.base_endpoint}/{record_id}",
            json={base64_field: body},
            **kwargs,
        )
        return self._raw_response(response, raw_response)

    def get_base64(
        self,
        record_id: str,
        base64_field: str = "Body",
        **kwargs: Any,
    ) -> bytes:
        response = self.client.request(
            http.get,
            f"{self.base_endpoint}/{record_id}/{base64_field}",
            **kwargs,
        )
        return response.content

    @staticmethod
    def _raw_response(response: httpx.Response, return_raw: bool) -> int | httpx.Response:
        return response if return_raw else response.status_code