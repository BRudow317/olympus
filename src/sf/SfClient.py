"""SfClient.py"""
from __future__ import annotations
import logging
logger = logging.getLogger(__name__)
import re, os
from collections.abc import Mapping, MutableMapping
from enum import Enum
from typing import Any, NamedTuple, TypedDict

import httpx



CLIENT_NAME: str | None = 'Salesforce'
CLIENT_ALIAS: str | None = 'sf'
CLIENT_NAMESPACE: str | None = 'salesforce'

API_VERSION: str = os.getenv('SF_API_VERSION', '66.0')
SF_EXTERNAL_CLIENT_APP_NAME: str = os.getenv('SF_EXTERNAL_CLIENT_APP_NAME', 'automation')
SF_BASE_URL: str = os.getenv('SF_BASE_URL') or f"https://{os.getenv('SF_BASE_DOMAIN')}.salesforce.com"
SF_CALLBACK_URL: str = os.getenv('SF_CALLBACK_URL', 'http://localhost:1717/OauthRedirect')
SF_AUTH_URI: str = os.getenv('SF_AUTH_URI', '/services/oauth2/token')

# --- Common Types ---
Headers = MutableMapping[str, str]
BulkDataAny = list[Mapping[str, any]]
BulkDataStr = list[Mapping[str, str]]

# --- http methods ---
class HttpMethod(str, Enum):
    delete = 'DELETE'
    get = 'GET'
    head = 'HEAD'
    options = 'OPTIONS'
    patch = 'PATCH'
    post = 'POST'
    put = 'PUT'
    request = 'REQUEST'

# --- REST Models ---
class Usage(NamedTuple):
    """Usage information for a Salesforce org"""
    used: int
    total: int

class PerAppUsage(NamedTuple):
    """Per App Usage information for a Salesforce org"""
    used: int
    total: int
    name: str

# --- Bulk API 2.0 Enums ---
class Operation(str, Enum):
    insert = "insert"
    upsert = "upsert"
    update = "update"
    delete = "delete"
    hard_delete = "hardDelete"
    query = "query"
    query_all = "queryAll"

class JobState(str, Enum):
    open = "Open"
    aborted = "Aborted"
    failed = "Failed"
    upload_complete = "UploadComplete"
    in_progress = "InProgress"
    job_complete = "JobComplete"

class ColumnDelimiter(str, Enum):
    BACKQUOTE = "BACKQUOTE"  # (`)
    CARET = "CARET"          # (^)
    COMMA = "COMMA"          # (,)
    PIPE = "PIPE"            # (|)
    SEMICOLON = "SEMICOLON"  # (;)
    TAB = "TAB"              # (\t)

class LineEnding(str, Enum):
    LF = "LF"
    CRLF = "CRLF"

class ResultsType(str, Enum):
    failed = "failedResults"
    successful = "successfulResults"
    unprocessed = "unprocessedRecords"

# --- Bulk API 2.0 Types ---
class QueryParameters(TypedDict, total=False):
    maxRecords: int
    locator: str

class QueryRecordsResult(TypedDict):
    locator: str
    number_of_records: int
    records: str

QueryResult = QueryRecordsResult

# Objects excluded from migratable describe
SKIP_SUFFIXES = (
    '__History', '__Feed', '__Share', '__Tag',
    '__ChangeEvent', '__e', '__mdt', '__b',
)

SKIP_NAMES = {
    # Feeds
    'AccountFeed', 'ContactFeed', 'CaseFeed', 'LeadFeed',
    'OpportunityFeed', 'UserFeed', 'CollaborationGroupFeed',
    # History
    'AccountHistory', 'ContactHistory', 'CaseHistory',
    'LeadHistory', 'OpportunityHistory', 'OpportunityFieldHistory',
    # Shares
    'AccountShare', 'CaseShare', 'LeadShare', 'OpportunityShare',
    # Apex / Dev
    'ApexClass', 'ApexTrigger', 'ApexLog', 'ApexTestResult',
    'AsyncApexJob', 'CronTrigger', 'CronJobDetail',
    # Content (binary blobs - break bulk migrations)
    'ContentVersion', 'ContentDocument', 'ContentDocumentLink',
    # Restricted query syntax — require specific WHERE filters; can't be queried freely
    'ContentFolderItem', 'IdeaComment',
    # Metadata / Definitions
    'EntityDefinition', 'FieldDefinition', 'FieldPermissions',
    # Auth / Sessions
    'OauthToken', 'AuthSession', 'SessionPermSetActivation',
    'TwoFactorInfo', 'VerificationHistory', 'LoginHistory', 'LoginGeo',
    # Platform
    'StaticResource', 'AuraDefinition', 'AuraDefinitionBundle',
    'FlowDefinitionView', 'FlowInterview',
    'PlatformEventChannel', 'PlatformEventChannelMember',
    'DataStatistics', 'BackgroundOperation', 'SetupAuditTrail',
    # Permissions
    'PermissionSet', 'PermissionSetAssignment',
    'GroupMember', 'UserRole', 'UserLicense',
}

def fetch_client_credentials(
    consumer_key: str | None = None,
    consumer_secret: str | None = None,
    base_url: str | None = None,
    access_token: str | None = None
) -> str:
    """Fetch an OAuth access token using the Client Credentials flow.
        Returns: access token string
        Raises: RuntimeError on failure
    """
    if access_token: return access_token
    if consumer_key is None and consumer_secret is None:
        consumer_key = os.getenv('SF_CONSUMER_KEY', None)
        consumer_secret = os.getenv('SF_CONSUMER_SECRET', None)
    if base_url is None:
        base_url = os.getenv('SF_BASE_URL', None)
    try:
        if not all([consumer_key, consumer_secret, base_url]):
            env_debug = {
                k: ("*" * len(v) if v else "[EMPTY STRING]")
                for k, v in os.environ.items()
                if k.startswith("SF_")
            }
            print(f"DEBUG SF Vars: {env_debug}")
            raise RuntimeError("Missing required environment variables for authentication.")
        with httpx.Client() as client:
            response = client.post(
                f"{base_url}{SF_AUTH_URI}",
                data={
                    "grant_type": "client_credentials",
                    "client_id": consumer_key,
                    "client_secret": consumer_secret,
                },
            )
        payload = response.json()
        if response.status_code != 200:
            raise RuntimeError(f"{payload.get('error')}: {payload.get('error_description')}")
        return str(payload.get('access_token', ''))
    except RuntimeError: raise
    except Exception as exc:
        raise RuntimeError("An unexpected error occurred while fetching client credentials.") from exc

class SfClient:
    """Sync HTTP client for Salesforce REST and Bulk 2.0 APIs."""
    base_url: str
    services_url: str
    access_token: str
    api_version: str
    api_usage: MutableMapping[str, Usage | PerAppUsage]
    _session: httpx.Client
    _max_retries: int

    def __init__(
        self,
        base_url: str | None = None,
        consumer_key: str | None = None,
        consumer_secret: str | None = None,
        access_token: str | None = None,
        api_version: str = API_VERSION,
        max_retries: int = 1,
    ) -> None:
        resolved_url = base_url or SF_BASE_URL
        if not resolved_url:
            raise RuntimeError("base_url or SF_BASE_URL environment variable is required.")
        if access_token is None:
            access_token = fetch_client_credentials(
                consumer_key=consumer_key,
                consumer_secret=consumer_secret,
                base_url=resolved_url,
            )
        self.base_url = resolved_url
        self.access_token = access_token
        self.api_version = api_version
        self.services_url = f"{resolved_url}/services/data/v{api_version}"
        self.api_usage = {}
        self._max_retries = max_retries
        # Credentials are captured in a closure so they are not stored as plain
        # attributes on the instance, avoiding exposure via serialization or logging.
        if consumer_key and consumer_secret:
            _ck, _cs, _url = consumer_key, consumer_secret, resolved_url
            self._token_refresher = lambda: fetch_client_credentials(
                consumer_key=_ck, consumer_secret=_cs, base_url=_url,
            )
        else:
            self._token_refresher = None
        self._session = httpx.Client(
            headers=self._auth_headers(access_token),
            timeout=httpx.Timeout(30.0, connect=10.0),
        )

    @staticmethod
    def _auth_headers(token: str) -> dict[str, str]:
        return {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        }

    def _update_token(self, token: str) -> None:
        """Replace the bearer token on the live session."""
        self.access_token = token
        self._session.headers.update(self._auth_headers(token))

    def request(
        self,
        method: str,
        endpoint: str,
        **kwargs: Any,
    ) -> httpx.Response:
        """
        Execute a sync HTTP request.
        Handles token refresh on 401 INVALID_SESSION_ID.
        Full URL or relative endpoint both accepted.
        """
        url = (
            endpoint
            if endpoint.startswith("https")
            else f"{self.services_url}/{endpoint.lstrip('/')}"
        )
        response = self._session.request(method, url, **kwargs)
        if response.status_code == 401:
            self._handle_401(response)
            response = self._session.request(method, url, **kwargs)
        if response.status_code >= 300: raise Exception (f"HTTP {response.status_code} {method} {url}: {response.text}")
        limit_info = response.headers.get("Sforce-Limit-Info")
        if limit_info: self._parse_api_usage(limit_info)
        return response

    def _handle_401(self, response: httpx.Response) -> None:
        """Refresh the token on INVALID_SESSION_ID."""
        try: error_code = response.json()[0].get("errorCode")
        except Exception: return
        if error_code != "INVALID_SESSION_ID": return
        if self._token_refresher is None:
            raise Exception("Session expired and no credentials are available to refresh the token.")
        logger.info("Session expired. Refreshing token...")
        for attempt in range(1, self._max_retries + 1):
            new_token = self._token_refresher()
            if new_token and new_token != self.access_token:
                self._update_token(new_token)
                return
            logger.warning(f"Token refresh attempt {attempt} returned same or empty token.")
        raise Exception("Max retries exceeded: could not refresh Salesforce token.")

    def _parse_api_usage(self, sforce_limit_info: str) -> None:
        api_usage = re.match(r"[^-]?api-usage=(?P<used>\d+)/(?P<tot>\d+)", sforce_limit_info)
        pau = re.match(
            r".+per-app-api-usage=(?P<u>\d+)/(?P<t>\d+)\(appName=(?P<n>.+)\)",
            sforce_limit_info,
        )
        if api_usage:
            g = api_usage.groups()
            self.api_usage["api-usage"] = Usage(used=int(g[0]), total=int(g[1]))
        if pau:
            g = pau.groups()
            self.api_usage["per-app-api-usage"] = PerAppUsage(
                used=int(g[0]), total=int(g[1]), name=g[2]
            )

    def close(self) -> None:
        self._session.close()

    def __enter__(self) -> SfClient:
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()
    
