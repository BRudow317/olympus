"""test_sf_mocks.py -- canned Salesforce REST API responses for offline tests.

These constants mirror the real shapes returned by the devint dev org so the SF
adapter (SfClient / Salesforce / SfModels) can be exercised without a live
connection. Two flavours are provided:

  * ``*_RESPONSE`` / ``*_PAGE*`` -- already-parsed ``dict``/``list`` payloads,
    i.e. what ``httpx.Response.json()`` returns. Feed these to a mocked client.
  * ``ERROR_*`` -- raw JSON *strings* as they appear in ``response.text``, since
    ``SalesforceRequestError`` parses the text body, not parsed JSON. Each has a
    companion ``*_STATUS`` HTTP status code.

Field/type coverage in the describe fixtures matches what the seeding tests
assert against (boolean→VARCHAR2(1), textarea→CLOB, datetime→TIMESTAMP, etc.).
"""
from __future__ import annotations

# --------------------------------------------------------------------------- #
# Auth + health
# --------------------------------------------------------------------------- #

# POST /services/oauth2/token (client_credentials / password grant success)
OAUTH_TOKEN_RESPONSE: dict = {
    "access_token": "00DfakeAccessToken!ARfakeSignatureValueForTestsOnly",
    "instance_url": "https://empathetic-narwhal-8eqg8r-dev-ed.trailblaze.my.salesforce.com",
    "id": "https://login.salesforce.com/id/00Dgs0000004CxyZ/005gs0000012AbcAAM",
    "token_type": "Bearer",
    "issued_at": "1717400000000",
    "signature": "fakeBase64SignatureForTestsOnly=",
}

# GET /services/oauth2/userinfo (drives SfClient.is_healthy)
USERINFO_RESPONSE: dict = {
    "sub": "https://login.salesforce.com/id/00Dgs0000004CxyZ/005gs0000012AbcAAM",
    "user_id": "005gs0000012AbcAAM",
    "organization_id": "00Dgs0000004CxyZEAS",
    "preferred_username": "brudow@empathetic-narwhal-8eqg8r.com",
    "name": "B Rudow",
    "email": "rudow56@gmail.com",
    "active": True,
}

# --------------------------------------------------------------------------- #
# Global describe  --  GET /services/data/vXX.0/sobjects
# Shape: {"encoding", "maxBatchSize", "sobjects": [ {sobject}, ... ]}
# Includes one migratable standard (Contact/Account), one access-restricted
# object (ConnectedApplication, still queryable per describe), and one that must
# be filtered out (AccountHistory via SKIP_SUFFIXES, AccountChangeEvent).
# --------------------------------------------------------------------------- #

def _sobject(name: str, label: str, *, queryable=True, retrieveable=True,
             layoutable=True, searchable=True, custom=False, key_prefix=None,
             triggerable=True) -> dict:
    return {
        "name": name,
        "label": label,
        "labelPlural": f"{label}s",
        "keyPrefix": key_prefix,
        "custom": custom,
        "queryable": queryable,
        "retrieveable": retrieveable,
        "layoutable": layoutable,
        "searchable": searchable,
        "createable": True,
        "updateable": True,
        "deletable": True,
        "triggerable": triggerable,
        "deprecatedAndHidden": False,
        "urls": {
            "describe": f"/services/data/v66.0/sobjects/{name}/describe",
            "sobject": f"/services/data/v66.0/sobjects/{name}",
            "rowTemplate": f"/services/data/v66.0/sobjects/{name}/{{ID}}",
        },
    }


GLOBAL_DESCRIBE_RESPONSE: dict = {
    "encoding": "UTF-8",
    "maxBatchSize": 200,
    "sobjects": [
        _sobject("Account", "Account", key_prefix="001"),
        _sobject("Contact", "Contact", key_prefix="003"),
        # Queryable in the catalog, but SELECT raises INSUFFICIENT_ACCESS:
        _sobject("ConnectedApplication", "Connected Application", key_prefix="0H4"),
        # Filtered out by SKIP_SUFFIXES ('__History' / '__ChangeEvent'):
        _sobject("AccountHistory", "Account History", layoutable=False, searchable=False),
        _sobject("AccountChangeEvent", "Account Change Event", queryable=False),
    ],
}

# --------------------------------------------------------------------------- #
# Object describe  --  GET /sobjects/{name}/describe
# One field per distinct type the migrator maps, including an 'address' field
# that describe_table must skip (_SKIP_FIELD_TYPES).
# --------------------------------------------------------------------------- #

def _field(name: str, sf_type: str, *, label=None, length=0, precision=0,
           scale=0, nillable=True, unique=False, updateable=True,
           calculated=False, id_lookup=False, external_id=False,
           reference_to=None, picklist=None, default=None) -> dict:
    return {
        "name": name,
        "label": label or name,
        "type": sf_type,
        "soapType": f"xsd:{sf_type}",
        "length": length,
        "precision": precision,
        "scale": scale,
        "nillable": nillable,
        "unique": unique,
        "updateable": updateable,
        "calculated": calculated,
        "idLookup": id_lookup,
        "externalId": external_id,
        "deprecatedAndHidden": False,
        "filterable": True,
        "sortable": True,
        "groupable": sf_type not in ("textarea", "address", "location"),
        "referenceTo": reference_to or [],
        "picklistValues": picklist or [],
        "defaultValue": default,
        "inlineHelpText": None,
    }


CONTACT_DESCRIBE_RESPONSE: dict = {
    "name": "Contact",
    "label": "Contact",
    "labelPlural": "Contacts",
    "keyPrefix": "003",
    "custom": False,
    "queryable": True,
    "searchable": True,
    "triggerable": True,
    "fields": [
        _field("Id", "id", length=18, nillable=False, id_lookup=True),
        _field("IsDeleted", "boolean", label="Deleted", updateable=False),
        _field("Name", "string", length=121, updateable=False, calculated=True),
        _field("FirstName", "string", length=40),
        _field("Email", "email", length=80, id_lookup=True),
        _field("Phone", "phone", length=40),
        _field("AccountId", "reference", length=18, reference_to=["Account"]),
        _field(
            "LeadSource", "picklist", length=40,
            picklist=[
                {"value": "Web", "label": "Web", "active": True, "defaultValue": False},
                {"value": "Phone Inquiry", "label": "Phone Inquiry", "active": True, "defaultValue": False},
                {"value": "Retired", "label": "Retired", "active": False, "defaultValue": False},
            ],
        ),
        _field("Description", "textarea", length=32000),  # > 4000 -> CLOB
        _field("Birthdate", "date"),
        _field("CreatedDate", "datetime", updateable=False),
        _field("MailingLatitude", "double", precision=18, scale=15),
        _field("NumberOfEmployees", "int", precision=8, scale=0),
        # Compound 'address' field -- describe_table must skip this one:
        _field("MailingAddress", "address", updateable=False, calculated=True),
    ],
}

ACCOUNT_DESCRIBE_RESPONSE: dict = {
    "name": "Account",
    "label": "Account",
    "labelPlural": "Accounts",
    "keyPrefix": "001",
    "custom": False,
    "queryable": True,
    "searchable": True,
    "triggerable": True,
    "fields": [
        _field("Id", "id", length=18, nillable=False, id_lookup=True),
        _field("IsDeleted", "boolean", label="Deleted", updateable=False),
        _field("Name", "string", length=255, nillable=False),
        _field("AnnualRevenue", "currency", precision=18, scale=2),
        _field("Website", "url", length=255),
        _field("CreatedDate", "datetime", updateable=False),
    ],
}

# --------------------------------------------------------------------------- #
# Query  --  GET /query/?q=...   (and follow-on /query/{locator} pages)
# Records carry an "attributes" envelope that _request_records pops off.
# --------------------------------------------------------------------------- #

def _attrs(sobject: str, record_id: str) -> dict:
    return {
        "type": sobject,
        "url": f"/services/data/v66.0/sobjects/{sobject}/{record_id}",
    }


# Page 1 of a multi-page result: done=False + nextRecordsUrl locator.
CONTACT_QUERY_PAGE1: dict = {
    "totalSize": 3,
    "done": False,
    "nextRecordsUrl": "/services/data/v66.0/query/01ggs00000ABCDEzAAF-500",
    "records": [
        {
            "attributes": _attrs("Contact", "003gs00000A1aaaAAA"),
            "Id": "003gs00000A1aaaAAA",
            "IsDeleted": False,
            "Name": "Ada Lovelace",
            "Email": "ada@example.com",
            "AccountId": "001gs00000Z9zzzAAA",
            "Description": "First programmer.",
            "Birthdate": "1815-12-10",
            "CreatedDate": "2026-01-15T09:30:00.000+0000",
            "NumberOfEmployees": None,
        },
        {
            "attributes": _attrs("Contact", "003gs00000A2bbbAAA"),
            "Id": "003gs00000A2bbbAAA",
            "IsDeleted": False,
            "Name": "Alan Turing",
            "Email": "alan@example.com",
            "AccountId": None,
            "Description": None,
            "Birthdate": "1912-06-23",
            "CreatedDate": "2026-02-01T14:05:30.000+0000",
            "NumberOfEmployees": 42,
        },
    ],
}

# Final page: done=True, no nextRecordsUrl -> _request_records stops.
CONTACT_QUERY_PAGE2: dict = {
    "totalSize": 3,
    "done": True,
    "records": [
        {
            "attributes": _attrs("Contact", "003gs00000A3cccAAA"),
            "Id": "003gs00000A3cccAAA",
            "IsDeleted": True,
            "Name": "Grace Hopper",
            "Email": "grace@example.com",
            "AccountId": "001gs00000Z9zzzAAA",
            "Description": None,
            "Birthdate": None,
            "CreatedDate": "2026-03-20T23:59:59.000+0000",
            "NumberOfEmployees": 0,
        },
    ],
}

# Single-page result (done immediately).
CONTACT_QUERY_SINGLE_PAGE: dict = {
    "totalSize": 1,
    "done": True,
    "records": [
        {
            "attributes": _attrs("Contact", "003gs00000A1aaaAAA"),
            "Id": "003gs00000A1aaaAAA",
            "IsDeleted": False,
            "Name": "Ada Lovelace",
            "Email": "ada@example.com",
        }
    ],
}

# Empty result set.
EMPTY_QUERY_RESPONSE: dict = {"totalSize": 0, "done": True, "records": []}

# Convenience: the full ordered page sequence for a paginated fetch.
CONTACT_QUERY_PAGES: tuple[dict, ...] = (CONTACT_QUERY_PAGE1, CONTACT_QUERY_PAGE2)

# --------------------------------------------------------------------------- #
# Error responses  --  raw response.text bodies (JSON arrays) + HTTP statuses.
# Salesforce returns a list of {"message", "errorCode", "fields"?} objects.
# These feed SalesforceRequestError(status, method, url, body).
# --------------------------------------------------------------------------- #

# The error from a full SF->Oracle migration over a restricted object
# (e.g. SELECT ... FROM ConnectedApplication). Access-class -> skippable.
ERROR_INSUFFICIENT_ACCESS: str = (
    '[{"message":"insufficient access rights on cross-reference id",'
    '"errorCode":"INSUFFICIENT_ACCESS"}]'
)
ERROR_INSUFFICIENT_ACCESS_STATUS: int = 400

# Bad SOQL -- NOT an access error, must stay fatal.
ERROR_MALFORMED_QUERY: str = (
    '[{"message":"\\nSELECT Bogus FROM Contact\\n       ^\\nERROR at Row:1:Column:8\\n'
    'No such column \'Bogus\' on entity \'Contact\'.","errorCode":"INVALID_FIELD"}]'
)
ERROR_MALFORMED_QUERY_STATUS: int = 400

# Expired/invalid session -- triggers SfClient._handle_401 refresh path.
ERROR_INVALID_SESSION_ID: str = (
    '[{"message":"Session expired or invalid","errorCode":"INVALID_SESSION_ID"}]'
)
ERROR_INVALID_SESSION_ID_STATUS: int = 401

# Unknown sobject / endpoint.
ERROR_NOT_FOUND: str = (
    '[{"errorCode":"NOT_FOUND","message":"The requested resource does not exist"}]'
)
ERROR_NOT_FOUND_STATUS: int = 404

# Lookup table: name -> (status_code, body). Handy for parametrized tests.
SF_ERROR_RESPONSES: dict[str, tuple[int, str]] = {
    "INSUFFICIENT_ACCESS": (ERROR_INSUFFICIENT_ACCESS_STATUS, ERROR_INSUFFICIENT_ACCESS),
    "INVALID_FIELD": (ERROR_MALFORMED_QUERY_STATUS, ERROR_MALFORMED_QUERY),
    "INVALID_SESSION_ID": (ERROR_INVALID_SESSION_ID_STATUS, ERROR_INVALID_SESSION_ID),
    "NOT_FOUND": (ERROR_NOT_FOUND_STATUS, ERROR_NOT_FOUND),
}
