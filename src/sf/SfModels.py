"""SfModels.py"""
from __future__ import annotations

from dataclasses import dataclass #, field
import json
import os
from typing import Any, NamedTuple, TypedDict
from collections.abc import Mapping, MutableMapping
from enum import StrEnum, Enum

Headers = MutableMapping[str, str]
BulkDataAny = list[Mapping[str, any]]
BulkDataStr = list[Mapping[str, str]]

# Salesforce errorCodes that mean the authenticated user lacks rights to the
# object/records rather than that the request was malformed. During a bulk
# migration these are skippable per-table instead of fatal.
SF_ACCESS_ERROR_CODES = frozenset({
    "INSUFFICIENT_ACCESS",
    "INSUFFICIENT_ACCESS_OR_READONLY",
    "INSUFFICIENT_ACCESS_ON_CROSS_REFERENCE_ENTITY",
})


class SalesforceRequestError(Exception):
    """Raised when the Salesforce REST API returns a non-2xx response.

    Parses the response body for Salesforce errorCode(s) so callers can react to
    specific failures (e.g. skipping a table on INSUFFICIENT_ACCESS) rather than
    treating every HTTP error the same way.
    """

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

class HttpMethod(StrEnum):
    delete = 'DELETE'
    get = 'GET'
    head = 'HEAD'
    options = 'OPTIONS'
    patch = 'PATCH'
    post = 'POST'
    put = 'PUT'
    request = 'REQUEST'

class Operation(StrEnum):
    insert = "insert"
    upsert = "upsert"
    update = "update"
    delete = "delete"
    hard_delete = "hardDelete"
    query = "query"
    query_all = "queryAll"

# Objects excluded from migratable describe
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