"""SfClient.py

https://python-httpx.org
"""
from __future__ import annotations

import logging
import re
import os
import base64
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Iterator
from urllib.parse import quote_plus
from enum import StrEnum

import httpx

logger = logging.getLogger(__name__)

# ==============================================================================
# 1. ENUMS & ENVIRONMENT CONFIGURATION
# ==============================================================================
class HttpMethod(StrEnum):
    delete = 'DELETE'
    get = 'GET'
    head = 'HEAD'
    options = 'OPTIONS'
    patch = 'PATCH'
    post = 'POST'
    put = 'PUT'
    request = 'REQUEST'

# Type alias matching method assignments
http = HttpMethod

# Base SSL and CA Certificate paths across runtime platforms
_DEVAPPOEL24_CHECKPOINT_CERT = '/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem'
_MUTABLE_CERT: str | bool = _DEVAPPOEL24_CHECKPOINT_CERT

if sys.platform == "win32":
    _MUTABLE_CERT = True


# ==============================================================================
# 2. CORE SALESFORCE CLIENT CORE
# ==============================================================================
class SfClient:
    httpx_client: httpx.Client
    environment: str

    # Dedicated Salesforce REST endpoint URLs
    api_version: str
    base_url: str
    auth_url: str
    services_url: str
    oauth2_url: str
    apex_url: str
    bulk_url: str
    bulk2_url: str
    metadata_url: str
    tooling_url: str
    max_retries: int
    
    # Private session auth token reference
    __access_token: str
    
    def __init__(
        self,
        environment: str,
        base_url: str,
        consumer_key: str | None = None,
        consumer_secret: str | None = None,
        access_token: str | None = None,
    ) -> None:
        """Initializes Endpoint URLs, sets up token handlers, and mounts a persistent httpx Client."""
        self.environment = environment

        # Dynamic Endpoint layout assignments mapping the targeted API version
        self.api_version = '66.0'
        self.base_url =     str(self.resolve_url(base_url=base_url))
        self.services_url = str(self.resolve_url(f'{self.base_url}/services/data/v{self.api_version}'))
        self.oauth2_url =   str(self.resolve_url(f'{self.base_url}/services/oauth2/'))
        self.auth_url =     str(self.resolve_url(f'{self.oauth2_url}/token'))
        self.apex_url =     str(self.resolve_url(f'{self.base_url}/services/apexrest'))
        self.bulk_url =     str(self.resolve_url(f'{self.services_url}/async/{self.api_version}'))
        self.bulk2_url =    str(f'{self.services_url}/jobs')
        self.metadata_url = str(self.resolve_url(f'{self.base_url}/services/Soap/m/{self.api_version}'))
        self.tooling_url =  str(self.resolve_url(f'{self.services_url}/tooling'))
        self.max_retries = 2
        
        # Token extraction lambda mapping OAuth configuration endpoints
        _ck, _cs, _url = consumer_key, consumer_secret, self.auth_url
        self._get_token = lambda: self._fetch_token(_ck, _cs, _url)
        
        if not access_token:
            self.__access_token = self._get_token()
        else:
            self.__access_token = access_token

        # Instantiate explicit synchronous http pipeline network transport
        self.httpx_client = httpx.Client(
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.__access_token}"
            },
            timeout=httpx.Timeout(30.0, connect=10.0),
            verify=_MUTABLE_CERT
        )
        
        logger.info(f'"services_url" : "{self.services_url}"')

    @classmethod
    def client_constructor(
        cls, 
        environment: str,
        access_token: str | None = None
    ) -> SfClient:
        """Constructs an instance using automated OS environment parameter lookups."""
        environment = environment.upper()
        
        base_url:        str = os.getenv(f"SF_AUTO_{environment}_BASE_URL", "")
        consumer_key:    str = os.getenv(f"SF_AUTO_{environment}_CONSUMER_KEY", "")
        consumer_secret: str = os.getenv(f"SF_AUTO_{environment}_CONSUMER_SECRET", "")

        if not all([base_url, consumer_key, consumer_secret]):
            raise ValueError(
                f"Missing Salesforce connection info for environment '{environment}'. "
                f"Required variables: SF_AUTO_{environment}_BASE_URL : {base_url} \n"
                f"SF_AUTO_{environment}_CONSUMER_KEY, "
                f"SF_AUTO_{environment}_CONSUMER_SECRET, SF_AUTO_{environment}_AUTH_URL"
            )
            
        return cls(
            environment=environment,
            base_url=base_url,
            consumer_key=consumer_key,
            consumer_secret=consumer_secret,
            access_token=access_token,
        )

    @staticmethod
    def _fetch_token(
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
                for k, v in os.environ.items() if k.startswith("SF_")
            }
            raise RuntimeError(f"Missing required environment variables for authentication: {env_debug}")
            
        logger.info(f"Initiating Salesforce OAuth callout to: {auth_url}")
        
        try:
            with httpx.Client(timeout=15.0, verify=_MUTABLE_CERT) as client:
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
        except (ValueError, json.JSONDecodeError):
            payload = {}
            logger.warning(f"Non-JSON response received ({response.status_code}): {response.text}")

        if response.status_code != 200:
            error_type = payload.get("error", "unknown_error")
            error_desc = payload.get("error_description", "No description provided by Salesforce.")
            
            logger.error(f"Salesforce OAuth Rejected [{response.status_code}]: {error_type} - {error_desc}")
            logger.error(f"Full Error Response:\n{json.dumps(payload, indent=4)}")
            
            if error_type == "invalid_grant":
                logger.error("Troubleshooting: Check if the External Client App 'Enable Client Credentials Flow' policy is checked and a 'Run As' execution user is set.")
            elif error_type == "invalid_client":
                logger.error("Troubleshooting: Verify that your Client ID (Consumer Key) and Client Secret match the Salesforce values exactly.")
                
            raise RuntimeError(f"Salesforce Auth Failure ({error_type}): {error_desc}")
            
        logger.info("Salesforce OAuth authentication successful.")
        return str(payload.get("access_token", ""))

    def resolve_url(self, url_part: str | None = None, base_url: str | None = None) -> httpx.URL:
        """Resolves target components into absolute URL parameters safely."""
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
        """Updates persistence engine parameters across runtime operations."""
        self.httpx_client.headers.update(headers)

    def close(self) -> None:
        """Releases active system connection resources safely."""
        self.httpx_client.close()

    def __enter__(self) -> SfClient:
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

    def request(
        self, method: HttpMethod, url: httpx.URL | str, **kwargs
    ) -> httpx.Response:
        """Performs connection operations with automated token lifecycle handling hooks."""
        url_str = str(url)
        
        if not url_str.startswith("http://") and not url_str.startswith("https://"):
            if not url_str.startswith("/services"):
                url = httpx.URL(self.services_url).join(url_str.lstrip("/"))
            else:
                url = self.resolve_url(url_str)
        else:
            url = httpx.URL(url_str)    
            
        print(str(url)) 
        response: httpx.Response = self.httpx_client.request(method=method, url=url, **kwargs)
        
        if response.status_code == 401:
            self._handle_401(response)
            response = self.httpx_client.request(method, url, **kwargs)
            
        if response.status_code >= 300:
            raise SalesforceRequestError(response.status_code, method, url, response.text)
            
        return response

    def is_healthy(self) -> bool:
        """Verifies session freshness via an explicit oauth identity endpoint call."""
        url = self.resolve_url(f"{self.oauth2_url}/userinfo")
        user_info_response = self.httpx_client.get(url)
        user_info_response.raise_for_status()
        logger.debug(f"Connection is healthy. Org ID: {str(user_info_response.json().get('organization_id'))}")
        return True

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
        endpoint = f"{self.services_url}/queryAll" if include_deleted else f"{self.services_url}/query"
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
            base = f"{self.services_url}/queryAll" if include_deleted else f"{self.services_url}/query"
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
            url=self.resolve_url(f'{self.apex_url}/{action}'),
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
            api_usage = re.match(r"[^-]?api-usage=(\d+)/(\d+)", limit_info)
            pau = re.match(
                r".+per-app-api-usage=(\d+)/(\d+)\(appName=(.+)\)",
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
        
        for attempt in range(1, self.max_retries + 1):
            new_token = self._get_token()
            if new_token and new_token != self.__access_token:
                self.__access_token = new_token
                headers = {
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {self.__access_token}"
                }
                self.update_headers(headers)
                return
                
        logger.warning(f"Token refresh attempt {attempt} returned same or empty token.")
        raise Exception("Max retries exceeded: could not refresh Salesforce token.")

    def __getattr__(self, name: str) -> "SObject":
        if name.startswith("__"):
            return super().__getattribute__(name)
        return SObject(object_name=name, http_client=self)


# ==============================================================================
# 3. INDIVIDUAL SOBJECT TARGET MANAGER
# ==============================================================================
class SObject:
    """Interface to a specific Salesforce SObject.
    Usage:
        client = SfClient(...)
        described_contact = client.Contact.describe()
    """
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


# ==============================================================================
# 4. MODULE ARCHIVE DATA FILTER CONSTANTS
# ==============================================================================
SKIP_NAMES = {
    # Feeds
    'AccountFeed', 'ContactFeed', 'CaseFeed', 'LeadFeed',
    'OpportunityFeed', 'UserFeed', 'CollaborationGroupFeed',
    # History
    'AccountHistory', 'ContactHistory', 'CaseHistory', 'LeadHistory',
    'OpportunityHistory', 'OpportunityFieldHistory',
    # Shares
    'AccountShare', 'CaseShare', 'LeadShare', 'OpportunityShare',
    # Apex / Dev
    'ApexClass', 'ApexTrigger', 'ApexLog', 'ApexTestResult',
    'AsyncApexJob', 'CronTrigger', 'CronJobDetail',
    # Content (binary blobs - break bulk migrations)
    'ContentVersion', 'ContentDocument', 'ContentDocumentLink',
    # Restricted query syntax -- require specific WHERE filters; can't be queried freely
    'ContentFolderItem', 'IdeaComment',
    # Metadata / Definitions
    'EntityDefinition', 'FieldDefinition', 'FieldPermissions',
    # Auth / Sessions
    'OauthToken', 'AuthSession', 'SessionPermSetActivation',
    'TwoFactorInfo', 'VerificationHistory', 'LoginHistory', 'LoginGeo',
    # Platform
    'StaticResource', 'AuraDefinition', 'AuraDefinitionBundle',
    'FlowDefinitionView', 'FlowInterview', 'PlatformEventChannel',
    'PlatformEventChannelMember', 'DataStatistics', 'BackgroundOperation',
    'SetupAuditTrail',
    # Permissions
    'PermissionSet', 'PermissionSetAssignment', 'GroupMember',
    'UserRole', 'UserLicense',
}

SKIP_SUFFIXES = (
    '__History',
    '__Feed',
    '__Share',
    '__Tag',
    '__ChangeEvent',
    '__e',
    '__mdt',
    '__b',
)

SF_ACCESS_ERROR_CODES = frozenset({
    "INSUFFICIENT_ACCESS",
    "INSUFFICIENT_ACCESS_OR_READONLY",
    "INSUFFICIENT_ACCESS_ON_CROSS_REFERENCE_ENTITY",
    "MALFORMED_QUERY"
})


# ==============================================================================
# 5. CUSTOM EXCEPTION INTEGRATIONS
# ==============================================================================
class SalesforceRequestError(Exception):
    """Raised when the Salesforce REST API returns a non-2xx response"""
    
    def __init__(self, status_code: int, method: Any, url: Any, body: str) -> None:
        self.status_code = status_code
        self.method = str(method)
        self.url = str(url)
        self.body = body
        self.error_codes = self._parse_error_codes(body)
        super().__init__(f"HTTP {status_code} {self.method} {self.url}: {body}")

    @staticmethod
    def _parse_error_codes(body: str) -> tuple[str, ...]:
        try:
            payload = json.loads(body)
        except (ValueError, TypeError):
            return ()
            
        if isinstance(payload, dict):
            payload = [payload]
            
        if not isinstance(payload, list):
            return ()
            
        return tuple(
            str(item["errorCode"])
            for item in payload
            if isinstance(item, dict) and item.get("errorCode")
        )

    @property
    def is_access_error(self) -> bool:
        """True when the failure is an access-rights denial (skippable)."""
        return any(code in SF_ACCESS_ERROR_CODES for code in self.error_codes)