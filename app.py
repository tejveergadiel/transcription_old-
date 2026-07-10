from flask import Flask, render_template, request, redirect, url_for, session, send_file, jsonify, has_request_context, flash
import requests
import json
import ibm_boto3
from ibm_botocore.client import Config
from ibm_botocore.config import Config as BotocoreConfig
import urllib.parse
import os
from datetime import datetime, timedelta, timezone
from collections import deque
from dotenv import load_dotenv
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed, wait, FIRST_COMPLETED, CancelledError
from botocore.exceptions import ClientError
from ibm_botocore.exceptions import ClientError as IBMClientError
from urllib.parse import urlparse
import re
import unicodedata
import tempfile
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
try:
    from ratelimit import limits, sleep_and_retry
except ImportError:
    def limits(calls, period):
        return lambda func: func
    def sleep_and_retry(func):
        return func
from threading import Thread, Lock
import ssl
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

load_dotenv()

REQUIRED_ENV_VARS = [
    'FLASK_SECRET_KEY',
    'COS_API_KEY',
    'COS_SERVICE_INSTANCE_ID',
    'COS_ENDPOINT',
    'COS_BUCKET',
    'ASSISTO_API_URL',
    'ASSISTO_API_TOKEN',
    'WATSONX_API_KEY',
    'WATSONX_URL',
    'WATSONX_PROJECT_ID',
    'WATSONX_MODEL_ID',
    'WATSONX_AUTH_URL',
    'SALESFORCE_CLIENT_ID',
    'SALESFORCE_CLIENT_SECRET',
    'SALESFORCE_USERNAME',
    'SALESFORCE_PASSWORD',
    'SALESFORCE_TOKEN_URL',
    'SALESFORCE_API_URL'
]


class ConsoleSafeFilter(logging.Filter):
    def filter(self, record):
        if isinstance(record.msg, str):
            record.msg = unicodedata.normalize('NFKD', record.msg).encode('ascii', 'replace').decode('ascii')
        return True


console_handler = logging.StreamHandler()
console_handler.addFilter(ConsoleSafeFilter())
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

file_handler = logging.FileHandler('app.log', encoding='utf-8')
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

logging.basicConfig(level=logging.INFO, handlers=[console_handler, file_handler])
logger = logging.getLogger(__name__)

logging.getLogger('werkzeug').setLevel(logging.WARNING)
logging.getLogger('flask').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.ERROR)
logging.getLogger('urllib3.connectionpool').setLevel(logging.ERROR)
logging.getLogger('requests').setLevel(logging.ERROR)

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
TEMP_DIR = os.path.join(BASE_DIR, 'temp')

for var in REQUIRED_ENV_VARS:
    if not os.getenv(var):
        logger.error(f"Missing required environment variable: {var}")

FLASK_SECRET_KEY              = os.getenv('FLASK_SECRET_KEY')
COS_API_KEY                   = os.getenv('COS_API_KEY')
COS_SERVICE_INSTANCE_ID       = os.getenv('COS_SERVICE_INSTANCE_ID')
COS_ENDPOINT                  = os.getenv('COS_ENDPOINT')
COS_BUCKET                    = os.getenv('COS_BUCKET')
ASSISTO_API_URL               = os.getenv('ASSISTO_API_URL')
ASSISTO_API_TOKEN             = os.getenv('ASSISTO_API_TOKEN')
ASSISTO_LANGUAGE              = os.getenv('ASSISTO_LANGUAGE', 'hi')
ASSISTO_WORKFLOW_NAME         = os.getenv('ASSISTO_WORKFLOW_NAME', 'diarized_transcription')
WATSONX_API_KEY               = os.getenv('WATSONX_API_KEY')
WATSONX_URL                   = os.getenv('WATSONX_URL')
WATSONX_PROJECT_ID            = os.getenv('WATSONX_PROJECT_ID')
WATSONX_MODEL_ID              = os.getenv('WATSONX_MODEL_ID')
WATSONX_AUTH_URL              = os.getenv('WATSONX_AUTH_URL')
SALESFORCE_CLIENT_ID          = os.getenv('SALESFORCE_CLIENT_ID')
SALESFORCE_CLIENT_SECRET      = os.getenv('SALESFORCE_CLIENT_SECRET')
SALESFORCE_USERNAME           = os.getenv('SALESFORCE_USERNAME')
SALESFORCE_PASSWORD           = os.getenv('SALESFORCE_PASSWORD')
SALESFORCE_TOKEN_URL          = os.getenv('SALESFORCE_TOKEN_URL')
SALESFORCE_API_URL            = os.getenv('SALESFORCE_API_URL')
PROXY_HOST                    = os.getenv('PROXY_HOST')
AUDIO_SEARCH_BUCKETS          = os.getenv('AUDIO_SEARCH_BUCKETS', '')
AUDIO_SEARCH_PREFIXES         = os.getenv('AUDIO_SEARCH_PREFIXES', '')

try:
    COS_LOOKBACK_HOURS = max(1, int(os.getenv('COS_LOOKBACK_HOURS', '24')))
except ValueError:
    COS_LOOKBACK_HOURS = 24

PRESTO_HOSTNAME          = os.getenv('PRESTO_HOSTNAME')
PRESTO_PORT              = os.getenv('PRESTO_PORT')
PRESTO_USERNAME          = os.getenv('PRESTO_USERNAME')
PRESTO_PASSWORD          = os.getenv('PRESTO_PASSWORD')
# ── SSL flag (must be bool, not raw string) ──────────────────
PRESTO_USE_SSL = os.getenv('PRESTO_USE_SSL', 'false').lower() in ('true', '1', 'yes')

PRESTO_LEAD_SCHEMA        = os.getenv('LEAD_SCHEMA')
PRESTO_LEAD_TABLE         = os.getenv('LEAD_TABLE')
PRESTO_LEAD_CATALOG       = os.getenv('LEAD_CATALOG')

PRESTO_OPPORTUNITY_SCHEMA = os.getenv('OPPORTUNITY_SCHEMA')
PRESTO_OPPORTUNITY_TABLE  = os.getenv('OPPORTUNITY_TABLE')
PRESTO_OPPORTUNITY_CATALOG = os.getenv('OPPORTUNITY_CATALOG')

PRESTO_TASK_SCHEMA        = os.getenv('TASK_SCHEMA') 
PRESTO_TASK_TABLE         = os.getenv('TASK_TABLE')
PRESTO_TASK_CATALOG       = os.getenv('TASK_CATALOG')

PRESTO_REPORT_CONFIG = {
    'lead': {
        'report_name': 'Lead Report',
        'catalog':     PRESTO_LEAD_CATALOG,
        'schema':      PRESTO_LEAD_SCHEMA,
        'table':       PRESTO_LEAD_TABLE,
        'id_field':    'id'
    },
    'opportunity': {
        'report_name': 'Opportunity Report',
        'catalog':     PRESTO_OPPORTUNITY_CATALOG,
        'schema':      PRESTO_OPPORTUNITY_SCHEMA,
        'table':       PRESTO_OPPORTUNITY_TABLE,
        'id_field':    'opportunity_id_c'
    },
    'task': {
        'report_name': 'Task Report',
        'catalog':     PRESTO_TASK_CATALOG,
        'schema':      PRESTO_TASK_SCHEMA,
        'table':       PRESTO_TASK_TABLE,
        'id_field':    'activity_id_c'
    }
}


def _get_non_negative_float_env(var_name, default_value):
    try:
        return max(0.0, float(os.getenv(var_name, str(default_value))))
    except ValueError:
        return float(default_value)


OUTBOUND_REQUEST_GAP_SECONDS             = _get_non_negative_float_env('OUTBOUND_REQUEST_GAP_SECONDS', 10)
ASSISTO_TRANSCRIPTION_INTERVAL_SECONDS   = _get_non_negative_float_env('ASSISTO_TRANSCRIPTION_INTERVAL_SECONDS', 10)
WATSONX_REQUEST_GAP_SECONDS              = _get_non_negative_float_env('WATSONX_REQUEST_GAP_SECONDS', 1)

try:
    WATSONX_MAX_WORKERS = max(1, int(os.getenv('WATSONX_MAX_WORKERS', '1')))
except ValueError:
    WATSONX_MAX_WORKERS = 1

try:
    WATSONX_TOKEN_CACHE_SECONDS = max(60, int(os.getenv('WATSONX_TOKEN_CACHE_SECONDS', '3300')))
except ValueError:
    WATSONX_TOKEN_CACHE_SECONDS = 3300

try:
    SALESFORCE_TOKEN_CACHE_SECONDS = max(60, int(os.getenv('SALESFORCE_TOKEN_CACHE_SECONDS', '3300')))
except ValueError:
    SALESFORCE_TOKEN_CACHE_SECONDS = 3300

SALESFORCE_PUSH_INTERVAL_SECONDS = _get_non_negative_float_env(
    'SALESFORCE_PUSH_INTERVAL_SECONDS', OUTBOUND_REQUEST_GAP_SECONDS
)

try:
    WATSONX_CONNECT_TIMEOUT_SECONDS = max(1, int(os.getenv('WATSONX_CONNECT_TIMEOUT_SECONDS', '30')))
except ValueError:
    WATSONX_CONNECT_TIMEOUT_SECONDS = 30

try:
    WATSONX_READ_TIMEOUT_SECONDS = max(1, int(os.getenv('WATSONX_READ_TIMEOUT_SECONDS', '300')))
except ValueError:
    WATSONX_READ_TIMEOUT_SECONDS = 300

try:
    WATSONX_AUTH_CONNECT_TIMEOUT_SECONDS = max(1, int(os.getenv('WATSONX_AUTH_CONNECT_TIMEOUT_SECONDS', '30')))
except ValueError:
    WATSONX_AUTH_CONNECT_TIMEOUT_SECONDS = 30

try:
    WATSONX_AUTH_READ_TIMEOUT_SECONDS = max(1, int(os.getenv('WATSONX_AUTH_READ_TIMEOUT_SECONDS', '300')))
except ValueError:
    WATSONX_AUTH_READ_TIMEOUT_SECONDS = 300

try:
    AUDIO_FALLBACK_MAX_SCAN = max(1, int(os.getenv('AUDIO_FALLBACK_MAX_SCAN', '20000')))
except ValueError:
    AUDIO_FALLBACK_MAX_SCAN = 20000

try:
    COS_FILE_CACHE_TTL_SECONDS = max(0, int(os.getenv('COS_FILE_CACHE_TTL_SECONDS', '300')))
except ValueError:
    COS_FILE_CACHE_TTL_SECONDS = 300

try:
    BATCH_PUSH_LIMIT = max(1, int(os.getenv('BATCH_PUSH_LIMIT', '500')))
except ValueError:
    BATCH_PUSH_LIMIT = 500

VERIFY_SSL_CERTIFICATES = os.getenv('VERIFY_SSL_CERTIFICATES', 'true').lower() in ('true', '1', 'yes')

COS_CLIENT_ERRORS = (ClientError, IBMClientError)

request_gap_lock = Lock()
last_request_times = {
    'assisto_transcription': 0.0,
    'salesforce_push': 0.0,
    'watsonx_inference': 0.0
}

proxies = {'http': PROXY_HOST, 'https': PROXY_HOST} if PROXY_HOST else None

# ============================================================
# UTILITY HELPERS
# ============================================================

def _normalize_string(value):
    if value is None:
        return None
    if isinstance(value, str):
        uui_str = value.strip()
        # Trim 18-character Salesforce ID to 15 characters (remove last 3 chars)
        if len(uui_str) == 18:
            uui_str = uui_str[:15]
        return uui_str
    if isinstance(value, (int, float)):
        return str(value).strip()
    return None


def _get_presto_port():
    try:
        return int(PRESTO_PORT) if PRESTO_PORT is not None else None
    except (TypeError, ValueError):
        return None


def _escape_presto_string(value):
    if value is None:
        return ''
    # Only escape single quotes and strip newlines — do NOT alter casing
    return str(value).replace("'", "''").replace('\n', ' ').strip()


def _escape_soql_string(value):
    if value is None:
        return ''
    return str(value).replace("\\", "\\\\").replace("'", "\\'").replace('\n', ' ').strip()

def _presto_is_configured():
    has_any_catalog = bool(PRESTO_LEAD_CATALOG or PRESTO_OPPORTUNITY_CATALOG or PRESTO_TASK_CATALOG)
    return bool(PRESTO_HOSTNAME and _get_presto_port() and PRESTO_USERNAME and has_any_catalog)


# ============================================================
# PRESTO QUERY ENGINE
# ============================================================

def _presto_query(sql, catalog=None, schema=None):
    if not _presto_is_configured():
        raise ValueError('Presto connection is not fully configured.')

    protocol = 'https' if PRESTO_USE_SSL else 'http'
    presto_url = f"{protocol}://{PRESTO_HOSTNAME}:{_get_presto_port()}/v1/statement"
    headers = {
        'X-Presto-User': PRESTO_USERNAME,
        'X-Presto-Catalog': catalog or PRESTO_LEAD_CATALOG or PRESTO_OPPORTUNITY_CATALOG or PRESTO_TASK_CATALOG,
        'X-Presto-Schema': schema or '',
        'X-Presto-Source': 'call-analysis'
    }
    auth = (PRESTO_USERNAME, PRESTO_PASSWORD) if PRESTO_PASSWORD else None
    
    logger.info(f"[PRESTO] Executing query: {sql}")

    response = requests.post(
        presto_url, headers=headers, data=sql, auth=auth,
        timeout=(10, 120), verify=VERIFY_SSL_CERTIFICATES, proxies=proxies
    )
    
    try:
        response.raise_for_status()
    except Exception:
        logger.error(f"[PRESTO] Query failed! Status: {response.status_code}")
        logger.error(f"[PRESTO] Response content: {response.text}")
        raise
        
    result = response.json()
    columns = [column['name'] for column in result.get('columns', []) or []]
    rows = list(result.get('data', []) or [])
    next_uri = result.get('nextUri')

    # Poll while query is still running
    import time
    while next_uri and result.get('stats', {}).get('state') != 'FINISHED':
        logger.info(f"[PRESTO] Query state: {result.get('stats', {}).get('state')} - polling...")
        time.sleep(1)  # Wait 1 second before polling

        next_response = requests.get(
            next_uri, headers=headers, auth=auth,
            timeout=(10, 120), verify=VERIFY_SSL_CERTIFICATES, proxies=proxies
        )
        next_response.raise_for_status()
        result = next_response.json()

        columns = [column['name'] for column in result.get('columns', []) or []]
        rows.extend(list(result.get('data', []) or []))
        next_uri = result.get('nextUri')

    if result.get('stats', {}).get('state') == 'FINISHED':
        logger.info(f"[PRESTO] Query FINISHED - rows returned: {len(rows)}")
    else:
        logger.warning(f"[PRESTO] Query ended with state: {result.get('stats', {}).get('state')}")

    return columns, rows


def _build_presto_query(report_type, id_value):
    report = PRESTO_REPORT_CONFIG.get(report_type)
    if not report:
        raise ValueError(f'Unknown report type: {report_type}')
    if not report['catalog']:
        raise ValueError(f'Missing catalog configuration for {report_type}')
    if not report['schema'] or not report['table']:
        raise ValueError(f'Missing schema/table configuration for {report_type}')
    # Ensure ID is 15 characters (trim 18-char to 15-char)
    id_value = str(id_value).strip()
    if len(id_value) == 18:
        id_value = id_value[:15]
    safe_value = _escape_presto_string(id_value)
    full_table = f"{report['catalog']}.{report['schema']}.{report['table']}"
    id_field = report['id_field']
    # Use LIKE for opportunity and task (flexible matching), exact match for lead
    if report_type == 'lead':
        where_clause = f"{id_field} = '{safe_value}'"
    else:
        where_clause = f"{id_field} LIKE '{safe_value}%'"
    return (f"SELECT * FROM {full_table} WHERE {where_clause} LIMIT 100",
            report['catalog'], report['schema'])


def _query_presto_report(report_type, id_value):
    try:
        if not _presto_is_configured():
            return {
                'report_type': report_type,
                'available': False,
                'error': 'Presto is not configured.'
            }

        query, catalog, schema = _build_presto_query(report_type, id_value)

        logger.info("=" * 80)
        logger.info(f"[PRESTO-REPORT] report_type={report_type}")
        logger.info(f"[PRESTO-REPORT] id_value={id_value}")
        logger.info(f"[PRESTO-REPORT] catalog={catalog}")
        logger.info(f"[PRESTO-REPORT] schema={schema}")
        logger.info(f"[PRESTO-REPORT] query={query}")
        logger.info("=" * 80)

        columns, rows = _presto_query(
            query,
            catalog=catalog,
            schema=schema
        )

        logger.info(
            f"[PRESTO-REPORT] report_type={report_type}, "
            f"rows_returned={len(rows)}"
        )

        if rows:
            logger.info(
                f"[PRESTO-REPORT] first_row_keys={list(dict(zip(columns, rows[0])).keys())}"
            )
            logger.info(
                f"[PRESTO-REPORT] first_row={dict(zip(columns, rows[0]))}"
            )
        else:
            logger.warning(
                f"[PRESTO-REPORT] NO DATA FOUND for "
                f"report_type={report_type}, "
                f"id_value={id_value}"
            )

        return {
            'report_type': report_type,
            'report_name': PRESTO_REPORT_CONFIG[report_type]['report_name'],
            'id_field': PRESTO_REPORT_CONFIG[report_type]['id_field'],
            'id_value': _normalize_string(id_value),
            'query': query,
            'available': True,
            'row_count': len(rows),
            'rows': [dict(zip(columns, row)) for row in rows[:10]],
            'columns': columns
        }

    except Exception as exc:
        logger.exception(
            f"[PRESTO-REPORT] Failed report_type={report_type}, "
            f"id_value={id_value}"
        )

        return {
            'report_type': report_type,
            'report_name': PRESTO_REPORT_CONFIG.get(report_type, {}).get('report_name'),
            'id_field': PRESTO_REPORT_CONFIG.get(report_type, {}).get('id_field'),
            'id_value': _normalize_string(id_value),
            'available': False,
            'error': str(exc)
        }


# ============================================================
# UUI EXTRACTION  (single definition — no duplicate)
# ============================================================

# Valid Salesforce ID pattern: starts with 00Q / 006 / 00T followed by 12-15 alphanumeric chars
_SF_ID_PATTERN = re.compile(r'^(00Q|006|00T)[a-zA-Z0-9]{12,15}$')
_SALESFORCE_ID_PATTERN = re.compile(r'^[a-zA-Z0-9]{15}$')


def normalize_salesforce_id(value, allowed_prefixes=None):
    """
    Normalize a Salesforce ID:
    - Strip whitespace
    - Trim 18-character IDs to 15 characters
    - Validate against the SF ID pattern
    - Optionally validate against allowed prefixes
    """
    sf_id = _normalize_string(value)
    if not sf_id or not isinstance(sf_id, str):
        return None
    # Trim 18-char to 15-char
    if len(sf_id) == 18:
        sf_id = sf_id[:15]
    if not _SF_ID_PATTERN.match(sf_id):
        return None
    if allowed_prefixes and not sf_id.startswith(tuple(allowed_prefixes)):
        return None
    return sf_id


def _first_valid_salesforce_id(values, expected_prefixes):
    for value in values:
        sf_id = _normalize_salesforce_id(value, expected_prefixes)
        if sf_id:
            return sf_id
    return None


def extract_salesforce_link_ids(data):
    """
    Extract Opportunity and Lead IDs from the payload.
    Returns (opportunity_id, lead_id) tuple.
    """
    payload = data or {}
    
    # ============================================================
    # DEBUG LOGGING - Show what we're working with
    # ============================================================
    logger.info(f"[LINK EXTRACT] Payload keys: {list(payload.keys())}")
    logger.info(f"[LINK EXTRACT] lead in payload: {payload.get('lead')}")
    logger.info(f"[LINK EXTRACT] opportunity in payload: {payload.get('opportunity')}")
    
    # ============================================================
    # DIRECT EXTRACTION from tagged lead/opportunity structures
    # This is the primary path - if we have tagged data, use it directly
    # ============================================================
    
    opportunity_id = None
    lead_id = None
    
    # Check for tagged lead first (from Presto routing)
    if isinstance(payload.get('lead'), dict):
        lead_data = payload['lead']
        logger.info(f"[LINK EXTRACT] Lead data keys: {list(lead_data.keys())}")
        lead_tagged_id = lead_data.get('tagged_id')
        logger.info(f"[LINK EXTRACT] lead_tagged_id: {lead_tagged_id}")
        
        if lead_tagged_id:
            normalized_lead = normalize_salesforce_id(lead_tagged_id, ('00Q',))
            if normalized_lead:
                lead_id = normalized_lead
                logger.info(f"[LINK EXTRACT] ✓ Direct lead tag found: {lead_id}")
    
    # Check for tagged opportunity
    if isinstance(payload.get('opportunity'), dict):
        opp_data = payload['opportunity']
        logger.info(f"[LINK EXTRACT] Opportunity data keys: {list(opp_data.keys())}")
        opp_tagged_id = opp_data.get('tagged_id')
        logger.info(f"[LINK EXTRACT] opp_tagged_id: {opp_tagged_id}")
        
        if opp_tagged_id:
            normalized_opp = normalize_salesforce_id(opp_tagged_id, ('006',))
            if normalized_opp:
                opportunity_id = normalized_opp
                logger.info(f"[LINK EXTRACT] ✓ Direct opportunity tag found: {opportunity_id}")
    
    # If we found both via direct extraction, return them
    if opportunity_id and lead_id:
        logger.info(f"[LINK EXTRACT] Found both via direct tags: Opp={opportunity_id}, Lead={lead_id}")
        return opportunity_id, lead_id
    
    # ============================================================
    # FALLBACK EXTRACTION from various field candidates
    # Only if we didn't find both IDs via direct extraction
    # ============================================================
    
    # Build candidate lists from various fields
    opportunity_candidates = [
        payload.get('Opportunity__c'),
        payload.get('opportunity__c'),
        payload.get('OpportunityId'),
        payload.get('opportunityId'),
        payload.get('opportunity_id_c'),
        payload.get('WhatId'),
        payload.get('whatId'),
        payload.get('WHATID'),
    ]
    
    lead_candidates = [
        payload.get('Lead__c'),
        payload.get('lead__c'),
        payload.get('LeadId'),
        payload.get('leadId'),
        payload.get('WhoId'),
        payload.get('whoId'),
        payload.get('WHOID'),
    ]
    
    # Check nested opportunity dict (if not already handled above)
    if isinstance(payload.get('opportunity'), dict):
        opportunity_candidates.extend([
            payload['opportunity'].get('tagged_id'),
            payload['opportunity'].get('id'),
            payload['opportunity'].get('opportunity_id_c'),
            payload['opportunity'].get('WhatId'),
        ])
    
    # Check nested lead dict (if not already handled above)
    if isinstance(payload.get('lead'), dict):
        lead_candidates.extend([
            payload['lead'].get('tagged_id'),
            payload['lead'].get('id'),
            payload['lead'].get('WhoId'),
        ])
    
    # Also check UUI field (the original Salesforce ID from the COS JSON)
    uui_id = _normalize_string(
        payload.get('UUI') or 
        payload.get('uui') or 
        payload.get('Uui') or
        payload.get('Id') or
        payload.get('id')
    )
    if uui_id:
        opportunity_candidates.append(uui_id)
        lead_candidates.append(uui_id)
    
    # Try to find a valid opportunity ID (006 prefix) - only if not already found
    if not opportunity_id:
        for candidate in opportunity_candidates:
            normalized = normalize_salesforce_id(candidate, ('006',))
            if normalized:
                opportunity_id = normalized
                logger.info(f"[LINK EXTRACT] Found opportunity ID from fallback: {normalized}")
                break
    
    # Try to find a valid lead ID (00Q prefix) - only if not already found
    if not lead_id:
        for candidate in lead_candidates:
            normalized = normalize_salesforce_id(candidate, ('00Q',))
            if normalized:
                lead_id = normalized
                logger.info(f"[LINK EXTRACT] Found lead ID from fallback: {normalized}")
                break
    
    # ============================================================
    # FINAL VERIFICATION - Ensure we have at least one valid ID
    # ============================================================
    
    logger.info(f"[LINK EXTRACT] Final result - opportunity_id: {opportunity_id}, lead_id: {lead_id}")
    
    # If we have both, log the relationship
    if opportunity_id and lead_id:
        logger.info(f"[LINK EXTRACT] Both Opportunity ({opportunity_id}) and Lead ({lead_id}) found")
    elif opportunity_id:
        logger.info(f"[LINK EXTRACT] Only Opportunity found: {opportunity_id}")
    elif lead_id:
        logger.info(f"[LINK EXTRACT] Only Lead found: {lead_id}")
    else:
        logger.warning("[LINK EXTRACT] No valid Opportunity or Lead ID found")
        logger.warning(f"[LINK EXTRACT] All candidates - Opp: {opportunity_candidates}, Lead: {lead_candidates}")
    
    return opportunity_id, lead_id

def extract_uui_from_payload(json_data):
    """
    Extract a valid Salesforce ID (00Q/006/00T prefix) from the COS JSON payload.
    18-character IDs are trimmed to 15 characters before returning.

    Strategy:
    1. Check well-known field names in priority order.
    2. Fall back to scanning every string field for the SF ID pattern.
    """
    if not json_data:
        logger.warning("[UUI] Payload is empty — cannot extract UUI")
        return None

    logger.debug(f"[UUI] Payload keys: {list(json_data.keys())}")

    candidate_keys = [
        'UUI', 'uui', 'Uui',
        'Opportunity__c', 'opportunity__c', 'opportunity_id_c', 'OpportunityId', 'opportunityId',
        'Lead__c', 'lead__c', 'activity_id_c', 'WhoId', 'WhatId',
        'Id', 'id', 'SalesforceId', 'salesforceId', 'salesforce_id',
    ]

    for key in candidate_keys:
        raw = json_data.get(key)
        if not isinstance(raw, str):
            continue
        value = raw.strip()
        # Trim 18-char SF ID to 15 chars before pattern check
        if len(value) == 18:
            value = value[:15]
        if value and _SF_ID_PATTERN.match(value):
            logger.info(f"[UUI] Found valid SF ID '{value}' in field '{key}'")
            return value  # 15-char, exact casing preserved

    # Fallback: scan all string values
    for key, raw in json_data.items():
        if isinstance(raw, str):
            value = raw.strip()
            # Trim 18-char SF ID to 15 chars before pattern check
            if len(value) == 18:
                value = value[:15]
            if value and _SF_ID_PATTERN.match(value):
                logger.info(f"[UUI] Found SF ID '{value}' by full-scan in field '{key}'")
                return value  # 15-char, exact casing preserved

    logger.warning(f"[UUI] No valid Salesforce ID found. Keys present: {list(json_data.keys())}")
    return None


# ============================================================
# RECORD-TYPE DETECTION FROM UUI PREFIX
# ============================================================

def get_record_type_from_uui(uui):
    """
    Map Salesforce ID prefix to record type.
    00Q → lead | 006 → opportunity | 00T → task
    """
    if not uui:
        return None

    prefix = str(uui)[:3].upper()
    type_map = {'00Q': 'lead', '006': 'opportunity', '00T': 'task'}
    record_type = type_map.get(prefix)

    if record_type:
        logger.info(f"[PREFIX] UUI='{uui}' | prefix='{prefix}' | type='{record_type}'")
    else:
        logger.warning(f"[PREFIX] Unknown SF ID prefix '{prefix}' for UUI='{uui}'")

    return record_type


# ============================================================
# TAGGING HELPER
# ============================================================

def _tag_record(presto_result, record_type, id_value, via_task=None):
    """
    Attach routing metadata to a Presto result so the caller
    always knows which path was taken to find this record.
    """
    tagged = dict(presto_result)
    tagged['record_type'] = record_type
    tagged['tagged_id']   = id_value
    tagged['tagged_via']  = f"task:{via_task}" if via_task else record_type
    tagged['tag_source']  = (
        '00T→WhoId→lead'         if via_task and record_type == 'lead'        else
        '00T→WhatId→opportunity' if via_task and record_type == 'opportunity' else
        '00Q→lead'               if record_type == 'lead'                     else
        '006→opportunity'
    )
    return tagged


# ============================================================
# ROUTING PATHS  (A / B / C)
# ============================================================

def _route_lead(uui):
    """Path A: 00Q → Lead Report (id = UUI) → tag lead"""
    result = {}
    try:
        logger.info(f"[ROUTE-A] Querying Lead Report | id='{uui}'")
        lead_result = _query_presto_report('lead', uui)
        if lead_result.get('available') and lead_result.get('rows'):
            result['lead'] = _tag_record(lead_result, 'lead', uui)
            logger.info(f"[ROUTE-A] ✓ Lead tagged | rows={lead_result['row_count']}")
        else:
            logger.warning(f"[ROUTE-A] No rows found for id='{uui}'")
    except Exception as e:
        logger.error(f"[ROUTE-A] Exception: {e}", exc_info=True)
    return result


def _route_opportunity(uui):
    """Path B: 006 → Opportunity Report (opportunity_id_c = UUI) → tag opportunity"""
    result = {}
    try:
        logger.info(f"[ROUTE-B] Querying Opportunity Report | opportunity_id_c='{uui}'")
        opp_result = _query_presto_report('opportunity', uui)
        if opp_result.get('available') and opp_result.get('rows'):
            result['opportunity'] = _tag_record(opp_result, 'opportunity', uui)
            logger.info(f"[ROUTE-B] ✓ Opportunity tagged | rows={opp_result['row_count']}")
        else:
            logger.warning(f"[ROUTE-B] No rows found for opportunity_id_c='{uui}'")
    except Exception as e:
        logger.error(f"[ROUTE-B] Exception: {e}", exc_info=True)
    return result


def _route_task(uui):
    result = {}

    try:
        logger.info("=" * 80)
        logger.info(f"[ROUTE-C] Starting Task Routing")
        logger.info(f"[ROUTE-C] activity_id_c='{uui}'")
        logger.info("=" * 80)

        task_result = _query_presto_report('task', uui)

        logger.info(
            f"[ROUTE-C] Task Query Result: "
            f"available={task_result.get('available')}, "
            f"row_count={task_result.get('row_count', 0)}"
        )

        if not task_result.get('available'):
            logger.warning(
                f"[ROUTE-C] Task Report unavailable for activity_id_c='{uui}'"
            )
            return result

        if not task_result.get('rows'):
            logger.warning(
                f"[ROUTE-C] No Task rows found for activity_id_c='{uui}'"
            )
            return result

        task_record = task_result['rows'][0]

        logger.info(f"[TASK] Record={task_record}")
        logger.info(f"[TASK] Columns={list(task_record.keys())}")

        # Tag the TASK record itself
        result['task'] = _tag_record(task_result, 'task', uui)
        logger.info(f"[ROUTE-C] ✓ Task tagged | rows={task_result['row_count']}")

        # Support multiple column names
        who_id = _normalize_string(
            task_record.get('WhoId')
            or task_record.get('whoid')
            or task_record.get('WHOID')
            or task_record.get('who_id')
            or task_record.get('Who_ID__c')
        )

        what_id = _normalize_string(
            task_record.get('WhatId')
            or task_record.get('whatid')
            or task_record.get('WHATID')
            or task_record.get('what_id')
            or task_record.get('What_ID__c')
        )

        logger.info(f"[TASK] WhoId={who_id}")
        logger.info(f"[TASK] WhatId={what_id}")

        # ---------------------------
        # Lead Routing
        # ---------------------------
        if who_id:
            logger.info(
                f"[ROUTE-C] WhoId detected: {who_id}"
            )

        if who_id and who_id.startswith("00Q"):

            logger.info(
                f"[ROUTE-C] Lead lookup triggered for WhoId={who_id}"
            )

            lead_result = _query_presto_report(
                'lead',
                who_id
            )

            logger.info(
                f"[ROUTE-C] Lead lookup completed. "
                f"rows_found={lead_result.get('row_count', 0)}"
            )

            if lead_result.get('available') and lead_result.get('rows'):

                logger.info(
                    f"[ROUTE-C] Lead tagging successful. "
                    f"LeadId={who_id}"
                )

                result['lead'] = _tag_record(
                    lead_result,
                    'lead',
                    who_id,
                    via_task=uui
                )
            else:
                logger.warning(
                    f"[ROUTE-C] Lead not found. "
                    f"LeadId={who_id}"
                )

        # ---------------------------
        # Opportunity Routing
        # ---------------------------
        if what_id:
            logger.info(
                f"[ROUTE-C] WhatId detected: {what_id}"
            )

        if what_id and what_id.startswith("006"):

            logger.info(
                f"[ROUTE-C] Opportunity lookup triggered for WhatId={what_id}"
            )

            opp_result = _query_presto_report(
                'opportunity',
                what_id
            )

            logger.info(
                f"[ROUTE-C] Opportunity lookup completed. "
                f"rows_found={opp_result.get('row_count', 0)}"
            )

            if opp_result.get('available') and opp_result.get('rows'):

                logger.info(
                    f"[ROUTE-C] Opportunity tagging successful. "
                    f"OpportunityId={what_id}"
                )

                result['opportunity'] = _tag_record(
                    opp_result,
                    'opportunity',
                    what_id,
                    via_task=uui
                )
            else:
                logger.warning(
                    f"[ROUTE-C] Opportunity not found. "
                    f"OpportunityId={what_id}"
                )

        logger.info(
            f"[ROUTE-C] Final Tagged Objects={list(result.keys())}"
        )

        return result

    except Exception as e:
        logger.exception(
            f"[ROUTE-C] Exception while processing activity_id_c='{uui}'"
        )
        return result


# ============================================================
# MAIN ROUTING DISPATCHER
# ============================================================

def query_lead_opportunity_from_presto(uui, json_data):
    """
    Route to the correct Presto report based on UUI prefix.

    Flow (matches diagram):
    ┌─ 00Q ──► _route_lead(uui)        → {lead}
    ├─ 006 ──► _route_opportunity(uui) → {opportunity}
    └─ 00T ──► _route_task(uui)
                  ├─ WhoId  → Lead Report  → {lead}
                  └─ WhatId → Opp  Report  → {opportunity}
    """
    if not uui:
        logger.warning("[ROUTE] No UUI provided — skipping Presto query")
        return {}

    if not _presto_is_configured():
        logger.warning("[ROUTE] Presto is not configured — skipping enrichment")
        return {}

    record_type = get_record_type_from_uui(uui)
    if not record_type:
        logger.warning(f"[ROUTE] Cannot determine record type for UUI='{uui}'")
        return {}

    logger.info(f"[ROUTE] UUI='{uui}' | type='{record_type}' | dispatching...")

    if record_type == 'lead':
        result = _route_lead(uui)
    elif record_type == 'opportunity':
        result = _route_opportunity(uui)
    elif record_type == 'task':
        result = _route_task(uui)
    else:
        result = {}

    logger.info(f"[ROUTE] Complete. Tagged: {list(result.keys()) or 'nothing'}")
    return result


# ============================================================
# ENRICHMENT ENTRY POINT  (called from api_process)
# ============================================================

def enrich_response_with_lead_opportunity(response, json_data):
    """
    Enrich an API response dict with lead / opportunity data from Presto.

    Steps:
    1. Extract UUI from COS JSON payload
    2. Route to correct Presto report via UUI prefix
    3. Merge tagged record(s) into response

    Final response shape (example for 00Q lead):
    {
      "monitorUCID": "...",
      "call_transcription": {...},
      ...
      "lead": {
        "record_type": "lead",
        "tagged_id":   "00Qabc...",
        "tag_source":  "00Q→lead",
        "available":   true,
        "row_count":   1,
        "rows":        [{...}],
        ...
      }
    }
    """
    if not response or not isinstance(response, dict):
        logger.warning("[ENRICH] Response is not a dict — skipping")
        return response

    uui = extract_uui_from_payload(json_data)
    if not uui:
        logger.info("[ENRICH] No UUI found in payload — response returned as-is")
        return response

    logger.info(f"[ENRICH] UUI='{uui}' → routing to Presto")
    presto_data = query_lead_opportunity_from_presto(uui, json_data)

    if presto_data:
        response.update(presto_data)
        logger.info(f"[ENRICH] ✓ Response enriched with: {list(presto_data.keys())}")
    else:
        logger.warning(f"[ENRICH] No Presto data found for UUI='{uui}'")

    return response


# ============================================================
# FLASK APP  (single definition)
# ============================================================

app = Flask(__name__)
app.secret_key = FLASK_SECRET_KEY or 'fallback-secret-key-for-dev'


def enforce_request_gap(channel_name, gap_seconds=OUTBOUND_REQUEST_GAP_SECONDS):
    if gap_seconds <= 0:
        return
    with request_gap_lock:
        now = time.time()
        last_request_time = last_request_times.get(channel_name, 0.0)
        wait_seconds = max(0.0, gap_seconds - (now - last_request_time))
        if wait_seconds > 0:
            logger.debug(f"Waiting {wait_seconds:.2f}s before next {channel_name} request")
            time.sleep(wait_seconds)
        last_request_times[channel_name] = time.time()


# ============================================================
# COS CLIENT
# ============================================================

def initialize_cos_client():
    try:
        session = requests.Session()
        retries = Retry(
            total=10, backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"]
        )
        session.mount('https://', HTTPAdapter(max_retries=retries))
        if proxies:
            session.proxies.update(proxies)
            logger.info(f"Using proxy: {PROXY_HOST}")

        cos_client = ibm_boto3.client(
            's3',
            ibm_api_key_id=COS_API_KEY,
            ibm_service_instance_id=COS_SERVICE_INSTANCE_ID,
            config=Config(
                signature_version='oauth',
                connect_timeout=30,
                read_timeout=300,
                retries={'max_attempts': 10, 'mode': 'standard'}
            ),
            endpoint_url=COS_ENDPOINT
        )
        logger.info("Successfully initialized COS client")
        return cos_client
    except Exception as e:
        logger.error(f"Failed to initialize COS client: {e}")
        raise


cos_client = initialize_cos_client()

# ============================================================
# GLOBAL STATE
# ============================================================

json_files             = None
json_files_cached_at   = 0.0
PAUSE_PROCESSING       = False
CANCEL_PROCESSING      = False
BATCH_PROCESSING_COMPLETED = False
BATCH_PUSH_LIMIT_REACHED   = False
STATE_FILE             = 'batch_state.json'
state_file_lock        = Lock()
push_dedupe_lock       = Lock()
push_stats_lock        = Lock()
batch_run_lock         = Lock()
batch_state_lock       = Lock()
push_in_progress                = set()
push_in_progress_monitor_ucids  = set()
pushed_file_registry   = None
pushed_monitor_registry = None
batch_thread           = None
watsonx_token_lock     = Lock()
watsonx_token_cache    = {'access_token': None, 'expires_at': 0.0}
salesforce_token_lock  = Lock()
salesforce_token_cache = {'access_token': None, 'expires_at': 0.0}


class BatchPauseRequested(Exception):
    pass


class BatchCancelRequested(Exception):
    pass


push_stats = {
    'total_push_attempts': 0,
    'successful_pushes': 0,
    'failed_pushes': 0,
    'total_transcriptions_pushed': 0,
    'transcription_errors': 0,
    'start_time': None,
    'last_push_time': None,
    'recent_pushes': []
}

FORCED_CALL_RATING          = os.getenv('FORCED_CALL_RATING', 'Unknown')
FORCED_CALL_TO_ACTION_TEXT  = "No action items identified."
FORCED_CALL_TO_ACTION_ITEMS = [FORCED_CALL_TO_ACTION_TEXT]
SHORT_CALL_TRANSCRIPT       = "Call is less than 15 sec"

# ============================================================
# FILE / PATH HELPERS
# ============================================================

def ensure_temp_dir():
    os.makedirs(TEMP_DIR, exist_ok=True)
    return TEMP_DIR


def _atomic_write_json(filepath, data):
    directory = os.path.dirname(os.path.abspath(filepath)) or BASE_DIR
    os.makedirs(directory, exist_ok=True)
    fd, temp_path = tempfile.mkstemp(prefix='tmp_', suffix='.json', dir=directory)
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as temp_file:
            json.dump(data, temp_file, indent=2, ensure_ascii=False)
            temp_file.flush()
            os.fsync(temp_file.fileno())
        os.replace(temp_path, filepath)
    except Exception:
        try:
            os.remove(temp_path)
        except OSError:
            pass
        raise


def _load_json_file(filepath, default=None):
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        return default
    except json.JSONDecodeError as e:
        logger.error(f"JSON decode failed for {filepath}: {e}")
        return default


def _is_safe_temp_path(path):
    if not path:
        return False
    try:
        requested = os.path.abspath(path)
        temp_root = os.path.abspath(ensure_temp_dir())
        return os.path.commonpath([requested, temp_root]) == temp_root and os.path.exists(requested)
    except ValueError:
        return False


def _get_safe_session_json_path():
    json_path = session.get('json_path')
    if _is_safe_temp_path(json_path):
        return os.path.abspath(json_path)
    return None


def get_output_json_path(file_key):
    safe_stem = os.path.splitext(os.path.basename(str(file_key or '').strip()))[0]
    filename = f"output_{safe_stem}.json"
    ensure_temp_dir()
    return os.path.join(TEMP_DIR, filename)


# ============================================================
# PUSH STATISTICS
# ============================================================

def _record_push_event(status, **extra):
    push_detail = {'timestamp': datetime.now().isoformat(), 'status': status, **extra}
    with push_stats_lock:
        if push_stats['start_time'] is None:
            push_stats['start_time'] = datetime.now().isoformat()
        push_stats['recent_pushes'].append(push_detail)
        if len(push_stats['recent_pushes']) > 50:
            push_stats['recent_pushes'].pop(0)


def _snapshot_push_stats():
    with push_stats_lock:
        return {
            'total_push_attempts': push_stats['total_push_attempts'],
            'successful_pushes': push_stats['successful_pushes'],
            'failed_pushes': push_stats['failed_pushes'],
            'total_transcriptions_pushed': push_stats['total_transcriptions_pushed'],
            'transcription_errors': push_stats['transcription_errors'],
            'start_time': push_stats['start_time'],
            'last_push_time': push_stats['last_push_time'],
            'recent_pushes': list(push_stats['recent_pushes'])
        }


def _get_recent_pushes_for_ui(limit=10):
    stats_snapshot = _snapshot_push_stats()
    successful_pushes = [p for p in stats_snapshot['recent_pushes'] if p.get('status') == 'success']
    recent_pushes = list(reversed(successful_pushes[-limit:]))
    return stats_snapshot, recent_pushes


# ============================================================
# BATCH STATE
# ============================================================

def _check_batch_pause(file_key=None, stage=None):
    if has_request_context():
        return
    if CANCEL_PROCESSING:
        file_label = file_key or "unknown file"
        stage_label = f" during {stage}" if stage else ""
        raise BatchCancelRequested(f"Cancel requested for {file_label}{stage_label}")
    if not PAUSE_PROCESSING:
        return
    file_label = file_key or "unknown file"
    stage_label = f" during {stage}" if stage else ""
    raise BatchPauseRequested(f"Pause requested for {file_label}{stage_label}")


def _start_batch_thread(max_workers=4, batch_size=100):
    global batch_thread, PAUSE_PROCESSING, CANCEL_PROCESSING, BATCH_PROCESSING_COMPLETED, BATCH_PUSH_LIMIT_REACHED
    with batch_state_lock:
        if batch_thread and batch_thread.is_alive():
            return False
        PAUSE_PROCESSING = False
        CANCEL_PROCESSING = False
        BATCH_PROCESSING_COMPLETED = False
        BATCH_PUSH_LIMIT_REACHED = False
        batch_thread = Thread(
            target=process_and_push_all_jsons,
            kwargs={'max_workers': max_workers, 'batch_size': batch_size},
            daemon=True
        )
        batch_thread.start()
        return True


def save_batch_state(processed_files, failed_files, pushed_files, pushed_monitor_ucids, current_batch_start, total_files):
    state = {
        'processed_files': list(processed_files),
        'failed_files': failed_files,
        'pushed_files': list(pushed_files),
        'pushed_monitor_ucids': list(pushed_monitor_ucids or []),
        'current_batch_start': current_batch_start,
        'total_files': total_files,
        'timestamp': datetime.now().isoformat()
    }
    try:
        with state_file_lock:
            _atomic_write_json(STATE_FILE, state)
        logger.info(f"Saved batch state to {STATE_FILE}")
    except Exception as e:
        logger.error(f"Failed to save batch state: {e}")


def load_batch_state(log_loaded=True):
    if not os.path.exists(STATE_FILE):
        logger.debug("No batch state file found, starting fresh")
        return set(), [], set(), set(), 0, 0
    try:
        with state_file_lock:
            state = _load_json_file(STATE_FILE, default={}) or {}
        processed_files      = set(state.get('processed_files', []))
        failed_files         = state.get('failed_files', [])
        pushed_files         = set(state.get('pushed_files', []))
        pushed_monitor_ucids = set(state.get('pushed_monitor_ucids', []))
        current_batch_start  = state.get('current_batch_start', 0)
        total_files          = state.get('total_files', 0)
        if log_loaded:
            logger.info(
                f"Loaded batch state: {len(processed_files)} processed, "
                f"{len(pushed_files)} pushed, {len(failed_files)} failed, "
                f"{len(pushed_monitor_ucids)} monitorUCID entries"
            )
        return processed_files, failed_files, pushed_files, pushed_monitor_ucids, current_batch_start, total_files
    except Exception as e:
        logger.error(f"Failed to load batch state: {e}")
        return set(), [], set(), set(), 0, 0


def sync_pushed_file_registry(pushed_files, pushed_monitor_ucids=None):
    global pushed_file_registry, pushed_monitor_registry
    with push_dedupe_lock:
        if pushed_file_registry is None:
            pushed_file_registry = set()
        pushed_file_registry.update(pushed_files or set())
        if pushed_monitor_ucids is not None:
            if pushed_monitor_registry is None:
                pushed_monitor_registry = set()
            pushed_monitor_registry.update(pushed_monitor_ucids or set())


def reserve_push_attempt(file_key, monitor_ucid=None):
    global pushed_file_registry, pushed_monitor_registry
    with push_dedupe_lock:
        if pushed_file_registry is None or pushed_monitor_registry is None:
            _, _, pushed_files, pushed_monitor_ucids, _, _ = load_batch_state()
            if pushed_file_registry is None:
                pushed_file_registry = set(pushed_files)
            if pushed_monitor_registry is None:
                pushed_monitor_registry = set(pushed_monitor_ucids)

        if file_key in pushed_file_registry:
            return 'already_pushed'
        if monitor_ucid and monitor_ucid in pushed_monitor_registry:
            return 'already_pushed'
        if file_key in push_in_progress:
            return 'push_in_progress'
        if monitor_ucid and monitor_ucid in push_in_progress_monitor_ucids:
            return 'push_in_progress'

        push_in_progress.add(file_key)
        if monitor_ucid:
            push_in_progress_monitor_ucids.add(monitor_ucid)
        return 'reserved'


def release_push_attempt(file_key, success=False, monitor_ucid=None):
    global pushed_file_registry, pushed_monitor_registry
    with push_dedupe_lock:
        push_in_progress.discard(file_key)
        if monitor_ucid:
            push_in_progress_monitor_ucids.discard(monitor_ucid)
        if success:
            if pushed_file_registry is None:
                pushed_file_registry = set()
            pushed_file_registry.add(file_key)
            if monitor_ucid:
                if pushed_monitor_registry is None:
                    pushed_monitor_registry = set()
                pushed_monitor_registry.add(monitor_ucid)


def persist_pushed_file(file_key, monitor_ucid=None):
    try:
        with state_file_lock:
            state = _load_json_file(STATE_FILE, default={}) or {}
            processed_files      = set(state.get('processed_files', []))
            pushed_files         = set(state.get('pushed_files', []))
            pushed_monitor_ucids = set(state.get('pushed_monitor_ucids', []))
            failed_files         = state.get('failed_files', [])
            current_batch_start  = state.get('current_batch_start', 0)
            total_files          = state.get('total_files', 0)

            processed_files.add(file_key)
            pushed_files.add(file_key)
            if monitor_ucid:
                pushed_monitor_ucids.add(monitor_ucid)

            _atomic_write_json(STATE_FILE, {
                'processed_files': list(processed_files),
                'failed_files': failed_files,
                'pushed_files': list(pushed_files),
                'pushed_monitor_ucids': list(pushed_monitor_ucids),
                'current_batch_start': current_batch_start,
                'total_files': total_files,
                'timestamp': datetime.now().isoformat()
            })
        sync_pushed_file_registry({file_key}, pushed_monitor_ucids)
    except Exception as e:
        logger.error(f"Failed to persist pushed file {file_key}: {e}")


def persist_processed_file(file_key, errors=None, pushed=False, monitor_ucid=None):
    try:
        with state_file_lock:
            state = _load_json_file(STATE_FILE, default={}) or {}
            processed_files      = set(state.get('processed_files', []))
            pushed_files         = set(state.get('pushed_files', []))
            pushed_monitor_ucids = set(state.get('pushed_monitor_ucids', []))
            failed_files         = state.get('failed_files', [])
            current_batch_start  = state.get('current_batch_start', 0)
            total_files          = state.get('total_files', 0)

            processed_files.add(file_key)
            failed_files = [e for e in failed_files if e and e[0] != file_key]

            if pushed:
                pushed_files.add(file_key)
                if monitor_ucid:
                    pushed_monitor_ucids.add(monitor_ucid)
            elif errors:
                failed_files.append((file_key, errors))

            _atomic_write_json(STATE_FILE, {
                'processed_files': list(processed_files),
                'failed_files': failed_files,
                'pushed_files': list(pushed_files),
                'pushed_monitor_ucids': list(pushed_monitor_ucids),
                'current_batch_start': current_batch_start,
                'total_files': total_files,
                'timestamp': datetime.now().isoformat()
            })

        if pushed:
            sync_pushed_file_registry({file_key}, {monitor_ucid} if monitor_ucid else None)
    except Exception as e:
        logger.error(f"Failed to persist processed file {file_key}: {e}")


def filter_batch_state_to_current_window(processed_files, failed_files, pushed_files, allowed_files):
    allowed_file_set  = set(allowed_files or [])
    filtered_processed = set(processed_files or set()) & allowed_file_set
    filtered_pushed    = set(pushed_files or set()) & allowed_file_set
    filtered_failed    = [e for e in (failed_files or []) if e and e[0] in allowed_file_set]
    return filtered_processed, filtered_failed, filtered_pushed


def is_file_key_in_current_window(file_key):
    return file_key in set(get_cos_files(force_refresh=True))


def get_current_month_window():
    window_start = datetime(2020, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    window_end = datetime(2026, 6, 30, 23, 59, 59, tzinfo=timezone.utc)
    return window_start, window_end


def is_root_bucket_json_key(key):
    normalized_key = str(key or '').strip()
    return (
        bool(normalized_key)
        and normalized_key.endswith('.json')
        and '/' not in normalized_key
        and '\\' not in normalized_key
    )


def is_cos_item_in_current_window(item):
    last_modified = item.get('LastModified') if isinstance(item, dict) else None
    if not last_modified:
        logger.warning(f"[COS WINDOW] Skipping {item.get('Key') if isinstance(item, dict) else 'unknown'}; missing LastModified")
        return False

    if last_modified.tzinfo is None:
        last_modified = last_modified.replace(tzinfo=timezone.utc)
    else:
        last_modified = last_modified.astimezone(timezone.utc)

    window_start, now = get_current_month_window()
    return window_start <= last_modified <= now


# ============================================================
# COS FILE LISTING
# ============================================================

def get_cos_files(force_refresh=False):
    global json_files, json_files_cached_at
    cache_age = time.time() - json_files_cached_at
    if (
        not force_refresh
        and json_files is not None
        and (COS_FILE_CACHE_TTL_SECONDS <= 0 or cache_age < COS_FILE_CACHE_TTL_SECONDS)
    ):
        logger.info(f"Using cached list of {len(json_files)} JSON files")
        return json_files
    try:
        logger.info(f"Fetching JSON files from COS bucket {COS_BUCKET}")
        fetched_json_items = []
        continuation_token = None

        while True:
            list_kwargs = {'Bucket': COS_BUCKET, 'Delimiter': '/'}
            if continuation_token:
                list_kwargs['ContinuationToken'] = continuation_token

            response = cos_client.list_objects_v2(**list_kwargs)
            for item in response.get('Contents', []) or []:
                key = item.get('Key', '')
                if is_root_bucket_json_key(key) and is_cos_item_in_current_window(item):
                    fetched_json_items.append(item)

            if response.get('IsTruncated'):
                continuation_token = response.get('NextContinuationToken')
                if not continuation_token:
                    break
            else:
                break

        window_start, now = get_current_month_window()
        window_label = f"{window_start.date().isoformat()} to {now.date().isoformat()}"

        if not fetched_json_items:
            logger.warning(f"No root-level JSON objects found in COS bucket for {window_label}")
            return []

        fetched_json_items.sort(key=lambda item: item.get('LastModified') or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
        fetched_json_files = [item.get('Key') for item in fetched_json_items if item.get('Key')]
        json_files = fetched_json_files
        json_files_cached_at = time.time()
        logger.info(f"Found {len(json_files)} root-level JSON files in COS bucket for {window_label}")
        return json_files
    except COS_CLIENT_ERRORS as e:
        json_files = None
        json_files_cached_at = 0.0
        logger.error(f"Error fetching files from COS: {e}")
        raise


def get_json_from_cos(bucket, key):
    try:
        logger.info(f"Fetching JSON from COS: {key}")
        response = cos_client.get_object(Bucket=bucket, Key=key)
        json_content = response['Body'].read().decode('utf-8')
        return json.loads(json_content)
    except COS_CLIENT_ERRORS as e:
        logger.error(f"Error fetching JSON from COS: {e}")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON: {e}")
        return None


# ============================================================
# AUDIO DOWNLOAD HELPERS
# ============================================================

def parse_call_duration(duration_str):
    try:
        parts = duration_str.split(':')
        if len(parts) == 3:
            h, m, s = map(int, parts)
            return h * 3600 + m * 60 + s
        elif len(parts) == 2:
            m, s = map(int, parts)
            return m * 60 + s
        else:
            return int(duration_str)
    except (ValueError, AttributeError):
        logger.warning(f"Failed to parse call duration: {duration_str}")
        return 0


def get_monitor_ucid(json_data):
    data = json_data or {}
    return (
        data.get('monitor_ucid')
        or data.get('monitorUCID')
        or data.get('monitorUcid__c')
        or data.get('MonitorUCID')
        or 'Unknown'
    )


def get_uui(json_data):
    """Extract UUI from COS JSON payload — trimmed to 15 chars if 18-char ID."""
    return extract_uui_from_payload(json_data) or 'Unknown'


def _parse_csv_values(value):
    if not value:
        return []
    return [v.strip() for v in value.split(',') if v.strip()]


def _unique_preserve_order(items):
    seen = set()
    unique_items = []
    for item in items:
        if item not in seen:
            seen.add(item)
            unique_items.append(item)
    return unique_items


def _extract_audio_filename(audio_url):
    try:
        parsed = urlparse(audio_url or '')
        filename = os.path.basename(parsed.path or '')
        return filename if filename else ''
    except Exception:
        return ''


def _is_access_denied_response(response):
    if response is None:
        return False
    if response.status_code == 403:
        return True
    content_type = (response.headers.get('Content-Type') or '').lower()
    if 'xml' in content_type or response.status_code >= 400:
        body = (response.text or '')[:2000]
        if '<Code>AccessDenied</Code>' in body or 'Access Denied' in body:
            return True
    return False


def _extract_bucket_and_key_from_url(audio_url):
    parsed = urlparse(audio_url or '')
    path = (parsed.path or '').lstrip('/')
    host = (parsed.netloc or '').lower()
    path_parts = path.split('/') if path else []
    bucket = None
    key = None
    if host.startswith('s3.') and len(path_parts) >= 2:
        bucket = path_parts[0]
        key = '/'.join(path_parts[1:])
    elif '.s3.' in host:
        bucket = host.split('.s3.')[0]
        key = path
    return bucket, key


def _build_audio_search_context(audio_url):
    parsed = urlparse(audio_url or '')
    bucket_from_url, key_from_url = _extract_bucket_and_key_from_url(audio_url)
    filename = _extract_audio_filename(audio_url)

    buckets = []
    buckets.extend(_parse_csv_values(AUDIO_SEARCH_BUCKETS))
    if COS_BUCKET:
        buckets.append(COS_BUCKET)
    if bucket_from_url:
        buckets.append(bucket_from_url)
    buckets = _unique_preserve_order([b for b in buckets if b])

    prefixes = _parse_csv_values(AUDIO_SEARCH_PREFIXES)
    if key_from_url and '/' in key_from_url:
        key_dir = key_from_url.rsplit('/', 1)[0].strip('/')
        if key_dir:
            prefixes.append(f"{key_dir}/")
    path_dir = os.path.dirname((parsed.path or '').lstrip('/')).strip('/')
    if path_dir:
        prefixes.append(f"{path_dir}/")
    prefixes.append('')
    prefixes = _unique_preserve_order(prefixes)

    key_candidates = []
    if key_from_url:
        key_candidates.append(key_from_url)
    if filename:
        stem, ext = os.path.splitext(filename)
        key_candidates.append(filename)
        if ext.lower() != '.mp3':
            key_candidates.append(f"{stem}.mp3")
        if ext.lower() != '.wav':
            key_candidates.append(f"{stem}.wav")
        for prefix in prefixes:
            if prefix:
                key_candidates.append(f"{prefix.rstrip('/')}/{filename}")
                if ext.lower() != '.mp3':
                    key_candidates.append(f"{prefix.rstrip('/')}/{stem}.mp3")
                if ext.lower() != '.wav':
                    key_candidates.append(f"{prefix.rstrip('/')}/{stem}.wav")
    key_candidates = _unique_preserve_order([k for k in key_candidates if k])
    return filename, buckets, prefixes, key_candidates


def _write_stream_to_temp_file(streaming_iter, filename_hint='audio.mp3'):
    safe_name = filename_hint if filename_hint.lower().endswith('.mp3') else 'audio.mp3'
    safe_name = os.path.basename(safe_name)
    ensure_temp_dir()
    fd, filepath = tempfile.mkstemp(
        prefix=f"temp_audio_{datetime.now().strftime('%Y%m%d%H%M%S')}_",
        suffix=f"_{safe_name}",
        dir=TEMP_DIR
    )
    with os.fdopen(fd, 'wb') as f:
        for chunk in streaming_iter:
            if chunk:
                f.write(chunk)
    return filepath


def _download_from_cos_object(bucket, key, filename_hint):
    try:
        response = cos_client.get_object(Bucket=bucket, Key=key)
        body = response.get('Body')
        if not body:
            return None
        filepath = _write_stream_to_temp_file(iter(lambda: body.read(8192), b''), filename_hint)
        logger.info(f"Downloaded audio from COS object: bucket={bucket}, key={key}")
        return filepath
    except COS_CLIENT_ERRORS as e:
        logger.warning(f"COS object download failed for bucket={bucket}, key={key}: {e}")
        return None
    except Exception as e:
        logger.warning(f"COS object download unexpected error for bucket={bucket}, key={key}: {e}")
        return None


def _find_cos_key_by_filename(bucket, filename, prefixes):
    if not filename:
        return None
    filename_stem = os.path.splitext(filename)[0].lower()
    scanned = 0
    try:
        for prefix in prefixes:
            paginator = cos_client.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                for obj in page.get('Contents', []) or []:
                    scanned += 1
                    key = obj.get('Key', '')
                    key_basename = os.path.basename(key).lower()
                    key_stem = os.path.splitext(key_basename)[0]
                    if key.endswith(f"/{filename}") or key == filename or key_stem == filename_stem:
                        logger.info(f"Found fallback object key in COS: {key}")
                        return key
                    if scanned >= AUDIO_FALLBACK_MAX_SCAN:
                        logger.warning(f"Reached fallback scan limit ({AUDIO_FALLBACK_MAX_SCAN}) for bucket={bucket}")
                        return None
    except COS_CLIENT_ERRORS as e:
        logger.warning(f"Fallback list_objects failed for bucket={bucket}: {e}")
        return None
    except Exception as e:
        logger.warning(f"Fallback list_objects unexpected error for bucket={bucket}: {e}")
        return None
    return None


def download_audio_file(audio_url, monitor_ucid='Unknown'):
    try:
        logger.info(f"Downloading audio from {audio_url}")
        session = requests.Session()
        retries = Retry(total=10, backoff_factor=5, status_forcelist=[429, 500, 502, 503, 504])
        session.mount('https://', HTTPAdapter(max_retries=retries))
        response = session.get(audio_url, stream=True, timeout=(30, 300))
        if response.status_code == 200 and not _is_access_denied_response(response):
            filename_hint = _extract_audio_filename(audio_url) or 'audio.mp3'
            filepath = _write_stream_to_temp_file(response.iter_content(chunk_size=8192), filename_hint)
            logger.info(f"Audio downloaded for monitorUCID={monitor_ucid}: {filepath}")
            return filepath

        logger.warning(f"Direct audio URL failed for monitorUCID={monitor_ucid}; status={response.status_code}")
        filename, buckets, prefixes, key_candidates = _build_audio_search_context(audio_url)
        if not filename:
            logger.error(f"Fallback skipped: cannot extract filename for monitorUCID={monitor_ucid}")
            return None

        for bucket in buckets:
            for key in key_candidates:
                filepath = _download_from_cos_object(bucket, key, filename)
                if filepath:
                    return filepath
            found_key = _find_cos_key_by_filename(bucket, filename, prefixes)
            if found_key:
                filepath = _download_from_cos_object(bucket, found_key, filename)
                if filepath:
                    return filepath

        logger.error(f"Failed to resolve audio for monitorUCID={monitor_ucid}, url={audio_url}")
        return None
    except requests.RequestException as e:
        logger.error(f"Failed to download audio for monitorUCID={monitor_ucid}: {e}")
        filename, buckets, prefixes, key_candidates = _build_audio_search_context(audio_url)
        if filename:
            for bucket in buckets:
                for key in key_candidates:
                    filepath = _download_from_cos_object(bucket, key, filename)
                    if filepath:
                        return filepath
                found_key = _find_cos_key_by_filename(bucket, filename, prefixes)
                if found_key:
                    filepath = _download_from_cos_object(bucket, found_key, filename)
                    if filepath:
                        return filepath
        return None


# ============================================================
# TRANSCRIPT NORMALISATION
# ============================================================

def blind_text_response(text):
    return len(text) > 50


def _fix_mojibake_text(text):
    if not isinstance(text, str):
        return text
    cleaned = text.strip()
    if not cleaned:
        return ""
    if re.search(r'à[\x80-\xBF]|Ã[\x80-\xBF]|â[\x80-\xBF]', cleaned):
        try:
            repaired = cleaned.encode('latin1').decode('utf-8')
            if repaired:
                return repaired
        except Exception:
            return cleaned
    return cleaned


def _decode_escaped_text(text):
    if not isinstance(text, str):
        return text
    cleaned = text.strip()
    if not cleaned:
        return ""
    if re.search(r'\\u[0-9a-fA-F]{4}', cleaned):
        try:
            return _fix_mojibake_text(json.loads(f'"{cleaned}"'))
        except Exception:
            try:
                return _fix_mojibake_text(cleaned.encode('utf-8').decode('unicode_escape'))
            except Exception:
                return _fix_mojibake_text(cleaned)
    return _fix_mojibake_text(cleaned)


def _extract_transcript_entries(payload):
    if isinstance(payload, str):
        stripped = payload.strip()
        if stripped.startswith('{') or stripped.startswith('['):
            try:
                return _extract_transcript_entries(json.loads(stripped))
            except Exception:
                return []
        return []

    if isinstance(payload, dict):
        for key in ("result", "results", "transcription", "data", "messages"):
            value = payload.get(key)
            if isinstance(value, (list, dict, str)):
                entries = _extract_transcript_entries(value)
                if entries:
                    return entries
        return []

    if isinstance(payload, list):
        entries = []
        for item in payload:
            if isinstance(item, dict):
                message = _decode_escaped_text(item.get("message") or item.get("text") or "")
                speaker = str(item.get("speaker", "")).strip()
                if message:
                    entries.append({"speaker": speaker, "message": re.sub(r'\s+', ' ', message).strip()})
            elif isinstance(item, str):
                message = _decode_escaped_text(item)
                if message:
                    entries.append({"speaker": "", "message": re.sub(r'\s+', ' ', message).strip()})
        return entries

    return []


def _label_from_speaker(speaker):
    speaker = str(speaker or "").strip()
    if speaker == "0":
        return "Speaker1"
    if speaker == "1":
        return "Speaker2"
    if speaker:
        return f"Speaker{speaker}" if speaker.isdigit() else f"Speaker {speaker}"
    return "Speaker"


def normalize_transcript_payload(transcript):
    if transcript is None:
        return "No transcription available"
    if isinstance(transcript, (dict, list)):
        entries = _extract_transcript_entries(transcript)
        if entries:
            return '\n'.join(f"{_label_from_speaker(e['speaker'])}: {e['message']}" for e in entries)
        return json.dumps(transcript, ensure_ascii=False)
    if not isinstance(transcript, str):
        return str(transcript)
    entries = _extract_transcript_entries(transcript)
    if entries:
        return '\n'.join(f"{_label_from_speaker(e['speaker'])}: {e['message']}" for e in entries)
    return _decode_escaped_text(transcript)


def _normalize_transcript_line(line):
    line = (line or "").strip()
    if not line:
        return ""
    lowered = line.lower().strip(" :-")
    if lowered in {"call transcription", "transcription", "conversation", "conversation transcript", "call transcript"}:
        return ""
    if lowered.startswith("call transcription"):
        return ""
    line = line.replace('Speaker 1:', 'Speaker1:').replace('Speaker 2:', 'Speaker2:')
    return re.sub(r'\s+', ' ', line).strip()


def _transcript_line_signature(speaker, text):
    normalized_text = re.sub(r'[\W_]+', ' ', (text or '').lower()).strip()
    return f"{speaker.lower()}::{normalized_text}"


def clean_transcript_lines(transcript):
    if not isinstance(transcript, str) or not transcript.strip():
        return []

    cleaned_lines = []
    seen_signatures = set()
    current_speaker = None
    current_text = []

    def flush_current():
        nonlocal current_speaker, current_text
        if not current_speaker:
            current_text = []
            return
        text = re.sub(r'\s+', ' ', ' '.join(current_text)).strip(" :-")
        if not text:
            current_speaker = None
            current_text = []
            return
        signature = _transcript_line_signature(current_speaker, text)
        if signature not in seen_signatures:
            seen_signatures.add(signature)
            cleaned_lines.append({"speaker": current_speaker, "text": text})
        current_speaker = None
        current_text = []

    for raw_line in transcript.splitlines():
        line = _normalize_transcript_line(raw_line)
        if not line:
            continue
        line = re.sub(r'\b(?:Agent|Customer|Speaker1|Speaker2):(?=\s*(?:Agent|Customer|Speaker1|Speaker2):|$)', '', line, flags=re.IGNORECASE)
        line = re.sub(r'\s+', ' ', line).strip()
        if not line:
            continue
        speaker_match = re.match(r'^(Agent|Customer|Speaker1|Speaker2):\s*(.*)$', line, re.IGNORECASE)
        if speaker_match:
            flush_current()
            speaker = speaker_match.group(1).strip()
            text = speaker_match.group(2).strip()
            if text in {"[Text]", "[Missing text]", "(No text)", "(No response)"}:
                continue
            current_speaker = speaker
            current_text = [text] if text else []
        elif current_speaker and line not in {"[Text]", "[Missing text]", "(No text)", "(No response)"}:
            current_text.append(line)

    flush_current()
    return cleaned_lines


def format_transcript_for_storage(transcript):
    transcript = normalize_transcript_payload(transcript)
    cleaned_lines = clean_transcript_lines(transcript)
    if not cleaned_lines:
        return transcript if isinstance(transcript, str) else "No transcription available"
    return '\n'.join(f"{item['speaker']}: {item['text']}" for item in cleaned_lines)


# ============================================================
# RATING / QUALITY HELPERS
# ============================================================

def normalize_rating_value(rating_text):
    if not rating_text or rating_text == "Unknown":
        return "Unknown"
    if "out of 10" in rating_text:
        try:
            v = float(rating_text.split("out of 10")[0].strip())
            if 0 <= v <= 10:
                return f"{v:g}/10"
        except (ValueError, IndexError):
            pass
    if "/" in rating_text:
        try:
            parts = rating_text.split("/")
            if len(parts) == 2:
                v = float(parts[0].strip())
                if 0 <= v <= 10:
                    return f"{v:g}/10"
        except ValueError:
            pass
    try:
        v = float(rating_text.strip())
        if 0 <= v <= 10:
            return f"{v:g}/10"
    except ValueError:
        pass
    return "Unknown"


def rating_text_to_float(rating_text):
    if not rating_text or rating_text == "Unknown":
        return None
    match = re.search(r"(\d+(?:\.\d+)?)", str(rating_text))
    if not match:
        return None
    try:
        v = float(match.group(1))
    except ValueError:
        return None
    return v if 0 <= v <= 10 else None


def _is_low_info_text(text):
    normalized = re.sub(r'\s+', ' ', (text or '').lower()).strip(" :-.,!?")
    if not normalized:
        return True
    low_info_phrases = {
        "hello", "hi", "hey", "bye", "goodbye", "thank you", "thanks",
        "ok", "okay", "sir", "ma'am", "madam", "ji", "yes", "no",
        "hello hello", "thankyou",
    }
    if normalized in low_info_phrases:
        return True
    return bool(re.fullmatch(
        r'(?:hello|hi|hey|bye|okay|ok|thanks?|thank you|sir|madam|ma\'?am|ji)'
        r'(?:\s+(?:hello|hi|hey|bye|okay|ok|thanks?|thank you|sir|madam|ma\'?am|ji))*',
        normalized
    ))


def estimate_call_rating(transcript, request_text="", action_text="", reasons=None, model_rating=None):
    reasons = reasons or []
    normalized_transcript = normalize_transcript_payload(transcript)
    cleaned_lines = clean_transcript_lines(normalized_transcript)
    transcript_text = '\n'.join(f"{i['speaker']}: {i['text']}" for i in cleaned_lines) if cleaned_lines else normalized_transcript

    transcript_lower = re.sub(r'\s+', ' ', transcript_text).lower()
    combined_text = " ".join(p for p in [
        transcript_lower,
        (request_text or "").lower(),
        (action_text or "").lower(),
        " ".join(reasons).lower()
    ] if p)

    meaningful_turns = [i for i in cleaned_lines if not _is_low_info_text(i.get("text"))]
    if not meaningful_turns:
        return 1.0

    score = 5.5
    if len(meaningful_turns) <= 2:
        score -= 1.0
    elif len(meaningful_turns) >= 8:
        score += 0.5

    if re.search(r'\b(resolved|fixed|completed|processed|closed|confirmed|approved|done|sorted)\b', combined_text):
        score += 2.5
    elif re.search(r'\b(scheduled|arranged|follow[- ]?up|call back|callback|connect later|will call|will follow up)\b', combined_text):
        score += 0.8

    if re.search(r'\b(apolog(?:y|ize|ised|ized)?|sorry|understand|sure|okay|thank you|thanks)\b', combined_text):
        score += 0.5
    if re.search(r'\b(request|requirement|need|want|inquiry|issue|problem|complaint)\b', combined_text):
        score += 0.4
    if re.search(r'\b(unresolved|could not|unable|not able|no response|not reachable|unclear|incomplete|insufficient context)\b', combined_text):
        score -= 1.8
    if re.search(r'\b(frustrat|angry|upset|busy|hold|wait|multiple calls|repeated calls|hello hello|no one spoke)\b', combined_text):
        score -= 1.2
    if re.search(r'\b(call back|callback|follow[- ]?up)\b', combined_text) and not re.search(r'\b(resolved|fixed|completed|processed|closed|done|sorted)\b', combined_text):
        score -= 0.8
    if re.search(r'\b(no specific request identified|no specific agent action identified|no request identified|no action identified)\b', combined_text):
        score -= 0.4
    if re.search(r'\b(incomplete transcript|insufficient context|unclear resolution|lack of clear communication)\b', " ".join(reasons).lower()):
        score -= 1.0

    if model_rating is not None:
        score = (score * 0.8) + (model_rating * 0.2)

    return round(max(0.0, min(10.0, score)), 1)


def parse_call_quality(callquality):
    parsed = {
        "request": "No specific customer request identified",
        "action": "No specific agent action identified",
        "rating": "Unknown",
        "reasons": []
    }
    if isinstance(callquality, dict):
        parsed["request"] = callquality.get("request") or parsed["request"]
        parsed["action"]  = callquality.get("action")  or parsed["action"]
        rating = callquality.get("rating")
        if rating:
            parsed["rating"] = normalize_rating_value(rating)
        reasons = callquality.get("reasons") or []
        parsed["reasons"] = reasons if reasons else parsed["reasons"]
        return parsed

    if not isinstance(callquality, str):
        return parsed

    # Check if it's all in one line and split by markers
    text = callquality.strip()
    markers = [
        "Add-on Request by Customer:",
        "Action Taken for the request:", 
        "Call Rating:",
        "Reason:"
    ]
    # First check if we have any markers
    has_markers = any(marker in text for marker in markers)
    reasons = []
    if has_markers:
        # Use string find to get all markers and their values
        parts = []
        marker_positions = []
        for marker in markers:
            idx = 0
            while True:
                pos = text.find(marker, idx)
                if pos == -1:
                    break
                marker_positions.append((pos, marker))
                idx = pos + len(marker)
        # Sort marker positions by their index
        marker_positions.sort(key=lambda x: x[0])
        # Now extract values between markers
        for i in range(len(marker_positions)):
            pos, marker = marker_positions[i]
            start = pos + len(marker)
            if i < len(marker_positions) - 1:
                end = marker_positions[i+1][0]
                value = text[start:end].strip()
            else:
                value = text[start:].strip()
            parts.append((marker, value))
        # Now process each part
        for marker, value in parts:
            if marker == "Add-on Request by Customer:":
                parsed["request"] = value or parsed["request"]
            elif marker == "Action Taken for the request:":
                parsed["action"] = value or parsed["action"]
            elif marker == "Call Rating:":
                parsed["rating"] = normalize_rating_value(value)
            elif marker == "Reason:":
                if value and value not in reasons:
                    reasons.append(value)
        if reasons:
            parsed["reasons"] = reasons
    else:
        # Try splitting by lines first
        for raw_line in callquality.splitlines():
            line = raw_line.strip()
            if not line:
                continue
            if line.startswith("Add-on Request by Customer:"):
                parsed["request"] = line[len("Add-on Request by Customer:"):].strip() or parsed["request"]
            elif line.startswith("Action Taken for the request:"):
                parsed["action"] = line[len("Action Taken for the request:"):].strip() or parsed["action"]
            elif line.startswith("Call Rating:"):
                parsed["rating"] = normalize_rating_value(line[len("Call Rating:"):].strip())
            elif line.startswith("Reason:"):
                reason = line[len("Reason:"):].strip()
                if reason and reason not in reasons:
                    reasons.append(reason)
        if reasons:
            parsed["reasons"] = reasons
    return parsed


def clean_call_to_action_items(call_to_action):
    if isinstance(call_to_action, list):
        raw_items = call_to_action
    elif isinstance(call_to_action, str) and call_to_action.strip():
        raw_items = call_to_action.splitlines()
    else:
        return []

    items = []
    seen = set()
    for line in raw_items:
        line = str(line).strip()
        if not line:
            continue
        if any(line.lower().startswith(marker) for marker in ["action item:", "follow-up:", "next step:", "todo:", "-", "*"]):
            extracted = False
            for marker in ["action item:", "follow-up:", "next step:", "todo:"]:
                if line.lower().startswith(marker):
                    item = line[len(marker):].strip()
                    if item and item not in items:
                        items.append(item)
                    extracted = True
                    break
            if not extracted and line.startswith(("-", "*")):
                item = line[1:].strip()
                key = item.lower()
                if item and key not in seen:
                    seen.add(key)
                    items.append(item)
        elif len(line) > 10 and not any(line.startswith(x) for x in ["agent:", "customer:", "summary", "insight", "reason"]):
            key = line.lower()
            if key not in seen:
                seen.add(key)
                items.append(line)

    return items if items else ["No specific action items identified"]


def _fallback_call_to_action_items(transcript):
    if not isinstance(transcript, str) or not transcript.strip():
        return []

    keyword_patterns = [
        (r'\bprice|cost|rate|floor wise\b', "Customer asked for pricing details."),
        (r'\bone bhk|1 bhk\b', "Customer asked about 1 BHK availability."),
        (r'\btwo bhk|2 bhk\b', "Customer asked about 2 BHK pricing/availability."),
        (r'\bwhere|location|tower|block|dream homes\b', "Customer asked about the property location/tower details."),
        (r'\bcall\b.*\bconfirm|confirm\b.*\bcall\b', "Customer requested a callback/confirmation on availability."),
        (r'\bcontact number|number\b', "Customer requested or shared a contact number for follow-up.")
    ]

    actions = []
    seen = set()
    for item in clean_transcript_lines(transcript):
        if item["speaker"] != "Customer":
            continue
        lowered = item["text"].lower()
        for pattern, action in keyword_patterns:
            if re.search(pattern, lowered, re.IGNORECASE) and action.lower() not in seen:
                seen.add(action.lower())
                actions.append(action)
    return actions


def build_request_points(callquality, action_items):
    rating_data = parse_call_quality(callquality)
    points = []
    request_text = (rating_data.get("request") or "").strip()
    if request_text and "no specific" not in request_text.lower() and "no request" not in request_text.lower():
        points.append(f"Customer request: {request_text}")

    for item in action_items or []:
        cleaned = re.sub(r'\s+', ' ', str(item)).strip(" .")
        if not cleaned:
            continue
        normalized = cleaned.lower()
        if "no specific action" in normalized or "no action items identified" in normalized:
            continue
        if normalized.startswith("customer request:"):
            points.append(cleaned)
        elif normalized.startswith("customer asked") or normalized.startswith("customer requested"):
            points.append(cleaned)
        else:
            points.append(f"Customer request: {cleaned}")

    deduped = []
    seen = set()
    for point in points:
        key = point.lower()
        if key not in seen:
            seen.add(key)
            deduped.append(point)
    return deduped or ["No specific customer request identified."]


def _split_text_to_bullets(text):
    if not isinstance(text, str):
        return []
    normalized = text.replace('\r', '\n')
    segments = []
    for raw_line in normalized.splitlines():
        line = raw_line.strip().lstrip("-*• ").strip()
        if not line:
            continue
        for part in re.split(r'(?<=[.!?])\s+|;\s+', line):
            cleaned = re.sub(r'\s+', ' ', part).strip(" .-")
            if cleaned:
                segments.append(cleaned)
    deduped = []
    seen = set()
    for item in segments:
        key = item.lower()
        if key not in seen:
            seen.add(key)
            deduped.append(item)
    return deduped


def build_insight_points(insight_summary, sentiment):
    points = []
    if insight_summary and insight_summary != "No insights available":
        points.extend(_split_text_to_bullets(insight_summary))
    if sentiment and sentiment != "Unknown":
        points.append(f"Sentiment: {sentiment}")
    return points or ["No insights available"]


def ensure_list(value, fallback):
    if isinstance(value, list):
        return value if value else list(fallback)
    if value is None:
        return list(fallback)
    if isinstance(value, tuple):
        return list(value) if value else list(fallback)
    if isinstance(value, str):
        cleaned = value.strip()
        return [cleaned] if cleaned else list(fallback)
    return list(fallback)


def normalize_customer_details(customer_details):
    details = dict(customer_details or {})
    normalized = {
        "caller_tone": details.get("caller_tone") or "Unknown",
        "customer_tone": details.get("customer_tone") or "Unknown",
        "intent": details.get("intent") or "Unknown",
        "urgency": details.get("urgency") or "Unknown"
    }

    # Keep backwards-compatible aliases for downstream consumers.
    normalized["caller_tone_emotion"] = normalized["caller_tone"]
    normalized["customer_tone_emotion"] = normalized["customer_tone"]
    return normalized


def build_analysis_payload(
    file_key,
    transcript,
    insights,
    callquality,
    separated,
    customer_details,
    call_to_action=None,
    json_data=None,
    source="app"
):
    try:
        insight_summary, sentiment = insights
    except (TypeError, ValueError):
        insight_summary = "Error: Could not extract summary"
        sentiment = "Error: Could not extract sentiment"

    json_data = json_data or {}
    monitor_ucid = get_monitor_ucid(json_data)

    transcript_text = normalize_transcript_payload(transcript)
    separated_source = separated if separated else transcript_text
    separated_text = format_transcript_for_storage(separated_source)

    payload = {
        "monitorUCID": monitor_ucid,
        "call_transcription": {
            "raw_transcript": transcript_text,
            "separated_transcript": separated_text
        },
        "call_insight": {
            "summary": insight_summary
        },
        "call_rating": {
            "quality": callquality
        },
        "call_sentiment_analysis": {
            "sentiment": sentiment
        }
    }

    return payload


# ============================================================
# TRANSCRIPT CHUNKING
# ============================================================

def validate_enriched_payload(data, file_key):
    """Check if payload has required enrichment fields before pushing.
    Works with NORMALIZED Salesforce field names."""
    # The core required fields (the rest are optional)
    required_fields = ['Transcript__c', 'Separated_Transcript__c', 'Call_Insight__c', 'Sentiment__c', 'call_Rating__c']
    logger.debug(f"[VALIDATION CHECK] {file_key} payload keys: {list(data.keys()) if data else 'None'}")

    # Normalize rating field casing: older saved JSON files may have
    # 'Call_Rating__c' (capital C) instead of 'call_Rating__c'.
    if not data.get('call_Rating__c') and data.get('Call_Rating__c'):
        data['call_Rating__c'] = data['Call_Rating__c']

    missing = [f for f in required_fields if not data.get(f)]

    # Check for lead or opportunity
    has_lead_or_opp = (
        (data.get('lead') and isinstance(data.get('lead'), dict) and data.get('lead').get('tagged_id'))
        or (data.get('Lead__c'))
        or (data.get('opportunity') and isinstance(data.get('opportunity'), dict) and data.get('opportunity').get('tagged_id'))
        or (data.get('Opportunity__c'))
    )
    if not has_lead_or_opp:
        logger.error(f"[VALIDATION FAILED] {file_key} missing lead or opportunity tagging")
        missing.append('lead/opportunity')

    if missing:
        logger.error(f"[VALIDATION FAILED] {file_key} missing fields: {missing}")
        logger.warning(f"[VALIDATION DEBUG] Present: {[f for f in required_fields if data.get(f)]}")
        return False

    logger.info(f"[VALIDATION PASSED] {file_key} has all enrichment fields")
    return True


def chunk_transcript(transcript_text, turns_per_chunk=4):
    """
    Separate transcript by speaker and group into chunks.
    Returns array of text chunks, each containing grouped Agent/Customer dialogue.

    Args:
        transcript_text: Raw transcript string with "Agent:" and "Customer:" labels
        turns_per_chunk: Number of speaker turns to group per chunk (default 4)

    Returns:
        list: Array of text chunks with agent/customer separated and grouped
    """
    if not transcript_text or not isinstance(transcript_text, str):
        return ["No transcription available"]

    lines = transcript_text.strip().split('\n')
    turns = []

    for line in lines:
        line = line.strip()
        if not line:
            continue
        if line.startswith('Agent:') or line.startswith('Customer:'):
            turns.append(line)

    if not turns:
        return ["No transcription available"]

    chunks = []
    for i in range(0, len(turns), turns_per_chunk):
        chunk_lines = turns[i:i + turns_per_chunk]
        chunk_text = '\n'.join(chunk_lines)
        chunks.append(chunk_text)

    return chunks


def collapse_repeated_block(text, min_block_len=40):
    """
    Detect a block of text that repeats consecutively 3+ times near the
    end of the transcript (a degenerate LLM loop) and collapse it down
    to a single occurrence.
    """
    n = len(text)
    if n < min_block_len * 3:
        return text

    tail_len = min(n, 4000)
    tail = text[-tail_len:]
    tail_start_in_text = n - tail_len

    best_cut = None
    for period in range(min_block_len, tail_len // 3 + 1):
        block = tail[-period:]
        reps = 1
        pos = tail_len - period
        while pos - period >= 0 and tail[pos - period:pos] == block:
            reps += 1
            pos -= period
        if reps >= 3:
            cut = tail_start_in_text + pos + period  # keep ONE copy of block
            if best_cut is None or cut < best_cut:
                best_cut = cut

    if best_cut is None:
        return text

    truncated = text[:best_cut].rstrip()
    return truncated if truncated else text


def truncate_self_repetition(text, min_anchor_len=60):
    """
    Detect runaway LLM repetition/restart loops in transcript text:

    1. A stray mid-text code-fence marker (```python etc.) signals the
       model restarted generation from scratch — truncate there.
    2. A verbatim repeat of the opening of the text signals a full
       transcript restart — truncate at the second occurrence.
    3. A short block repeating 3+ times consecutively near the end
       signals a degenerate loop — collapse to a single occurrence.
    """
    if not isinstance(text, str) or len(text) < min_anchor_len:
        return text

    fence_match = re.search(r'```(?:python|json|text)?', text, flags=re.IGNORECASE)
    if fence_match and fence_match.start() > min_anchor_len:
        truncated = text[:fence_match.start()].rstrip()
        for boundary in ('\n', '. '):
            idx = truncated.rfind(boundary)
            if idx != -1 and idx > len(truncated) * 0.3:
                truncated = truncated[:idx + len(boundary)].rstrip()
                break
        return truncated if truncated else text

    if len(text) >= min_anchor_len * 2:
        anchor = text[:min_anchor_len]
        second_occurrence = text.find(anchor, min_anchor_len)
        if second_occurrence != -1:
            truncated = text[:second_occurrence].rstrip()
            for boundary in ('\n', '. '):
                idx = truncated.rfind(boundary)
                if idx != -1 and idx > len(truncated) * 0.5:
                    truncated = truncated[:idx + len(boundary)].rstrip()
                    break
            if truncated:
                text = truncated

    text = collapse_repeated_block(text)

    return text


def clean_salesforce_text_value(value):
    if not isinstance(value, str):
        return value
    text = value.replace('\x00', '').strip()
    text = re.sub(r'\n?\s*```(?:python|json|text)?\s*$', '', text, flags=re.IGNORECASE).strip()
    text = truncate_self_repetition(text)
    return text


NO_TRANSCRIPT_PLACEHOLDER = "No transcription available"
MISSING_AUDIO_TRANSCRIPT = "No audio URL is available"
MISSING_AUDIO_REASON = "Not processed due to missing URL"


def normalize_rating_value(rating_text):
    """Extract and normalize the rating value from text like 'X.X out of 10'"""
    if not rating_text or rating_text == "Unknown":
        return "Unknown"
    
    # Handle format like "8.5 out of 10"
    if "out of 10" in rating_text:
        try:
            rating_str = rating_text.split("out of 10")[0].strip()
            rating_value = float(rating_str)
            # Validate rating is between 0 and 10
            if 0 <= rating_value <= 10:
                return f"{rating_value:g}/10"
        except (ValueError, IndexError):
            pass
    
    # Handle format like "8.5/10"
    if "/" in rating_text:
        try:
            parts = rating_text.split("/")
            if len(parts) == 2:
                rating_value = float(parts[0])
                denominator = parts[1].strip()
                if denominator == "10" and 0 <= rating_value <= 10:
                    return f"{rating_value:g}/10"
        except ValueError:
            pass
    
    # If we can't parse, return as-is
    return str(rating_text).strip()


def parse_call_quality(callquality):
    parsed = {
        "request": "No specific customer request identified",
        "action": "No specific agent action identified",
        "rating": "Unknown",
        "reasons": []
    }

    if isinstance(callquality, dict) and callquality.get("short_call"):
        parsed["request"] = "No specific customer request identified"
        parsed["action"] = "No specific agent action identified"
        parsed["rating"] = "Short call"
        parsed["reasons"] = ["Call duration was 15 seconds or shorter."]
        return parsed

    if isinstance(callquality, dict):
        parsed["request"] = callquality.get("request") or parsed["request"]
        parsed["action"] = callquality.get("action") or parsed["action"]
        rating = callquality.get("rating")
        if rating:
            parsed["rating"] = normalize_rating_value(rating)
        parsed["reasons"] = ensure_list(callquality.get("reasons"), [])
    elif isinstance(callquality, str):
        # First try parsing structured lines like "Add-on Request by Customer:", "Call Rating:", etc.
        reasons = []
        # Check if it's all in one line and split by markers
        text = callquality.strip()
        markers = [
            "Add-on Request by Customer:",
            "Action Taken for the request:", 
            "Call Rating:",
            "Reason:"
        ]
        # Create a regex to split on any of these markers
        import re
        pattern = '|'.join(re.escape(marker) for marker in markers)
        # Split the text, then process each segment with its marker
        # First check if we have any markers
        has_markers = any(marker in text for marker in markers)
        if has_markers:
            # Use regex to find all markers and their values
            # Let's build a list of (marker, value) pairs
            parts = []
            # First, find all positions of markers
            marker_positions = []
            for marker in markers:
                idx = 0
                while True:
                    pos = text.find(marker, idx)
                    if pos == -1:
                        break
                    marker_positions.append((pos, marker))
                    idx = pos + len(marker)
            # Sort marker positions by their index
            marker_positions.sort(key=lambda x: x[0])
            # Now extract values between markers
            for i in range(len(marker_positions)):
                pos, marker = marker_positions[i]
                start = pos + len(marker)
                if i < len(marker_positions) - 1:
                    end = marker_positions[i+1][0]
                    value = text[start:end].strip()
                else:
                    value = text[start:].strip()
                parts.append((marker, value))
            # Now process each part
            for marker, value in parts:
                if marker == "Add-on Request by Customer:":
                    parsed["request"] = value or parsed["request"]
                elif marker == "Action Taken for the request:":
                    parsed["action"] = value or parsed["action"]
                elif marker == "Call Rating:":
                    parsed["rating"] = normalize_rating_value(value)
                elif marker == "Reason:":
                    if value and value not in reasons:
                        reasons.append(value)
            if reasons:
                parsed["reasons"] = reasons
        else:
            # Try splitting by lines first
            for raw_line in callquality.splitlines():
                line = raw_line.strip()
                if not line:
                    continue
                if line.startswith("Add-on Request by Customer:"):
                    parsed["request"] = line[len("Add-on Request by Customer:"):].strip() or parsed["request"]
                elif line.startswith("Action Taken for the request:"):
                    parsed["action"] = line[len("Action Taken for the request:"):].strip() or parsed["action"]
                elif line.startswith("Call Rating:"):
                    parsed["rating"] = normalize_rating_value(line[len("Call Rating:"):].strip())
                elif line.startswith("Reason:"):
                    reason = line[len("Reason:"):].strip()
                    if reason and reason not in reasons:
                        reasons.append(reason)
            if reasons:
                parsed["reasons"] = reasons
            else:
                # If no structured lines found, try just using it as a rating
                parsed_rating = normalize_rating_value(callquality)
                if parsed_rating != "Unknown":
                    parsed["rating"] = parsed_rating
    return parsed


def migrate_legacy_output_schema(payload):
    """
    Convert legacy output JSON shape to the current schema in-place-compatible form.
    Currently:
    - call_transcription: "<text>"  ->  call_transcription.separated_transcript
    Returns: (migrated_payload, changed)
    """
    data = dict(payload or {})
    changed = False

    call_transcription = data.get("call_transcription")
    if isinstance(call_transcription, str):
        data["call_transcription"] = {"separated_transcript": call_transcription}
        changed = True
    elif call_transcription is None:
        data["call_transcription"] = {"separated_transcript": NO_TRANSCRIPT_PLACEHOLDER}
        changed = True

    return data, changed


# ============================================================
# SALESFORCE PAYLOAD HELPERS
# ============================================================

def normalize_salesforce_payload(data):
    """Upgrade legacy payload shapes to the nested structure Salesforce expects."""
    payload = dict(data or {})
    monitor_ucid = payload.get("monitorUCID") or payload.get("monitorUcid") or payload.get("monitor_ucid")

    transcript_value = payload.get("call_transcription")
    if isinstance(transcript_value, str):
        payload["call_transcription"] = {
            "separated_transcript": transcript_value
        }
    elif transcript_value is None:
        payload["call_transcription"] = {
            "separated_transcript": "No transcription available"
        }
    elif isinstance(transcript_value, dict):
        raw_transcript = transcript_value.get("raw_transcript") or transcript_value.get("rawTranscript")
        separated_transcript = (
            transcript_value.get("separated_transcript")
            or transcript_value.get("separatedTranscript")
            or raw_transcript
        )
        payload["call_transcription"] = {
            "separated_transcript": separated_transcript or raw_transcript or "No transcription available"
        }

    # Check both lowercase call_rating and capital call_Rating
    rating_value = payload.get("call_rating") or payload.get("call_Rating")
    if isinstance(rating_value, str):
        parsed_rating = parse_call_quality(rating_value)
        payload["call_rating"] = {
            "rating": parsed_rating.get("rating", "Unknown"),
            "reasons": ensure_list(parsed_rating.get("reasons"), ["No specific reason provided"])
        }
    elif isinstance(rating_value, dict):
        # Check if rating field is a string that needs parsing
        rating_str = rating_value.get("rating")
        if isinstance(rating_str, str) and any(marker in rating_str for marker in ["Add-on Request by Customer:", "Call Rating:", "Action Taken for the request:", "Reason:"]):
            # Parse the string with parse_call_quality
            parsed_rating = parse_call_quality(rating_str)
            payload["call_rating"] = {
                "rating": parsed_rating.get("rating", "Unknown"),
                "reasons": ensure_list(parsed_rating.get("reasons"), ["No specific reason provided"])
            }
        elif "quality" in rating_value and ("rating" not in rating_value or "reasons" not in rating_value):
            parsed_rating = parse_call_quality(rating_value.get("quality"))
            payload["call_rating"] = {
                "rating": parsed_rating.get("rating", "Unknown"),
                "reasons": ensure_list(parsed_rating.get("reasons"), ["No specific reason provided"])
            }
        else:
            payload["call_rating"] = {
                "rating": normalize_rating_value(rating_value.get("rating")),
                "reasons": ensure_list(rating_value.get("reasons"), ["No specific reason provided"])
            }

    if "call_to_action" in payload:
        payload["call_to_action"] = ensure_list(payload.get("call_to_action"), [])

    # Populate Salesforce field aliases expected by the object schema.
    transcript_text = str(payload.get("call_transcription", {}).get("separated_transcript") or "No transcription available")
    transcript_text = transcript_text
    payload["call_transcription"]["separated_transcript"] = transcript_text

    if monitor_ucid and not payload.get("monitorUcid__c"):
        payload["monitorUcid__c"] = str(monitor_ucid).strip()

    if transcript_text and not payload.get("Separated_Transcript__c"):
        payload["Separated_Transcript__c"] = transcript_text

    call_insight = payload.get("call_insight")
    if isinstance(call_insight, dict):
        summary_text = str(call_insight.get("summary") or "").strip()
        if summary_text and not payload.get("Call_Insight__c"):
            payload["Call_Insight__c"] = summary_text

    sentiment_block = payload.get("call_sentiment_analysis")
    if isinstance(sentiment_block, dict):
        sentiment_text = str(sentiment_block.get("sentiment") or "").strip()
        if sentiment_text and not payload.get("Sentiment__c"):
            payload["Sentiment__c"] = sentiment_text

    logger.info(f"[normalize_salesforce_payload] Looking for rating_block, call_rating exists: {bool(payload.get('call_rating'))}, call_Rating exists: {bool(payload.get('call_Rating'))}")
    rating_block = payload.get("call_rating") or payload.get("call_Rating")
    logger.info(f"[normalize_salesforce_payload] rating_block type: {type(rating_block)}, value: {repr(rating_block)}")
    if isinstance(rating_block, dict):
        rating_value = rating_block.get("quality") or rating_block.get("rating")
        rating_text = normalize_rating_value(rating_value)
        logger.info(f"[normalize_salesforce_payload] rating_value: {repr(rating_value)}")
        reasons = ensure_list(rating_block.get("reasons"), [])
        logger.info(f"[normalize_salesforce_payload] rating_text: {repr(rating_text)}, reasons: {reasons}")
        if reasons:
            rating_parts = [f"Call Rating: {rating_text}"]
            rating_parts.extend([f"Reason {i + 1}: {reason}" for i, reason in enumerate(reasons)])
            final_rating_text = " | ".join(rating_parts)
        else:
            final_rating_text = f"Call Rating: {rating_text}"
        logger.info(f"[normalize_salesforce_payload] final_rating_text: {repr(final_rating_text)}")
        if rating_text and not payload.get("call_Rating__c"):
            payload["call_Rating__c"] = clean_salesforce_text_value(final_rating_text)
            logger.info(f"[normalize_salesforce_payload] Set call_Rating__c to: {repr(payload.get('call_Rating__c'))}")
            # Also set the other variations for compatibility
            if not payload.get("Call_Rating__c"):
                payload["Call_Rating__c"] = payload["call_Rating__c"]
            if not payload.get("call_rating__c"):
                payload["call_rating__c"] = payload["call_Rating__c"]

    # Handle Opportunity__c
    opportunity_id = (
        payload.get("Opportunity__c")
        or payload.get("opportunity__c")
        or payload.get("OpportunityId")
        or payload.get("opportunityId")
        or payload.get("opportunity_id_c")
    )
    if not opportunity_id and isinstance(payload.get("opportunity"), dict):
        opportunity_id = (
            payload["opportunity"].get("tagged_id")
            or payload["opportunity"].get("opportunity_id_c")
            or payload["opportunity"].get("id")
        )
    if opportunity_id and str(opportunity_id).startswith("006"):
        payload["Opportunity__c"] = opportunity_id

    # Handle Lead__c
    lead_id = (
        payload.get("Lead__c")
        or payload.get("lead__c")
        or payload.get("LeadId")
        or payload.get("leadId")
        or payload.get("WhoId")
    )
    if not lead_id and isinstance(payload.get("lead"), dict):
        lead_id = (
            payload["lead"].get("tagged_id")
            or payload["lead"].get("id")
            or payload["lead"].get("WhoId")
        )
    if lead_id and str(lead_id).startswith("00Q"):
        payload["Lead__c"] = lead_id

    return payload


SALESFORCE_OUTPUT_FIELDS = [
    'monitorUcid__c',
    'Transcript__c',
    'Separated_Transcript__c',
    'Call_Insight__c',
    'Sentiment__c',
    'call_Rating__c',
    'Opportunity__c',
    'Lead__c'
]


def compact_salesforce_output_payload(data):
    """Keep only required fields as nested structure for Salesforce push."""
    input_data = dict(data or {})
    payload = {}
    # Copy required nested fields
    for field in SALESFORCE_OUTPUT_FIELDS:
        if field in input_data and input_data[field] not in (None, '', [], {}):
            payload[field] = input_data[field]
    # Also ensure we have Opportunity__c and Lead__c if available
    if 'Opportunity__c' not in payload and 'opportunity' in input_data and isinstance(input_data['opportunity'], dict):
        payload['Opportunity__c'] = input_data['opportunity'].get('tagged_id')
    if 'Lead__c' not in payload and 'lead' in input_data and isinstance(input_data['lead'], dict):
        payload['Lead__c'] = input_data['lead'].get('tagged_id')
    return payload


def build_salesforce_compatible_payload(data):
    """Return payload with flat Salesforce fields; preserves already-flat files."""
    payload = dict(data or {})

    if 'monitorUCID' in payload:
        payload['monitorUcid__c'] = payload.get('monitorUCID')

    transcript_block = payload.get('call_transcription') or {}
    transcript_text = ''
    if isinstance(transcript_block, dict):
        transcript_text = (
            transcript_block.get('raw_transcript')
            or transcript_block.get('rawTranscript')
            or transcript_block.get('separated_transcript')
            or transcript_block.get('separatedTranscript')
            or ''
        )
    elif isinstance(transcript_block, str):
        transcript_text = transcript_block

    payload['Transcript__c'] = payload.get('Transcript__c') or transcript_text or 'No transcription available'
    if isinstance(transcript_block, dict):
        payload['Separated_Transcript__c'] = payload.get('Separated_Transcript__c') or (
            transcript_block.get('separated_transcript')
            or transcript_block.get('separatedTranscript')
            or transcript_text
            or 'No transcription available'
        )
    else:
        payload['Separated_Transcript__c'] = payload.get('Separated_Transcript__c') or transcript_text or 'No transcription available'
    payload['Transcript__c'] = clean_salesforce_text_value(payload.get('Transcript__c'))
    payload['Separated_Transcript__c'] = clean_salesforce_text_value(payload.get('Separated_Transcript__c'))

    insight_block = payload.get('call_insight') or {}
    if isinstance(insight_block, dict):
        payload['Call_Insight__c'] = payload.get('Call_Insight__c') or insight_block.get('summary') or 'No summary available'
    elif isinstance(insight_block, str):
        payload['Call_Insight__c'] = payload.get('Call_Insight__c') or insight_block
    payload['Call_Insight__c'] = clean_salesforce_text_value(payload.get('Call_Insight__c'))

    sentiment_block = payload.get('call_sentiment_analysis') or {}
    if isinstance(sentiment_block, dict):
        payload['Sentiment__c'] = payload.get('Sentiment__c') or sentiment_block.get('sentiment') or 'Unknown'
    elif isinstance(sentiment_block, str):
        payload['Sentiment__c'] = payload.get('Sentiment__c') or sentiment_block

    # First check if any existing rating fields are already present
    existing_rating = (
        payload.get('call_Rating__c') 
        or payload.get('Call_Rating__c') 
        or payload.get('call_rating__c') 
        or payload.get('Rating__c')
    )
    
    if existing_rating:
        payload['call_Rating__c'] = clean_salesforce_text_value(existing_rating)
    else:
        # Build rating from call_rating dict (check both lowercase and capitalized)
        rating_block = payload.get('call_rating') or payload.get('call_Rating') or {}
        if isinstance(rating_block, dict):
            rating_value = (
                rating_block.get('quality')  # Prioritize "quality" like test.py
                or rating_block.get('rating')
                or rating_block.get('score')
                or 'Unknown'
            )
            rating_value = str(rating_value).strip() or 'Unknown'
            rating_parts = [f"Call Rating: {rating_value}"]
            reasons = ensure_list(rating_block.get('reasons', []), [])
            if reasons:
                rating_parts.extend([f"Reason {i + 1}: {reason}" for i, reason in enumerate(reasons)])
            rating_text = " | ".join(rating_parts)
            payload['call_Rating__c'] = clean_salesforce_text_value(rating_text)
        elif isinstance(rating_block, str):
            payload['call_Rating__c'] = clean_salesforce_text_value(rating_block.strip())
        else:
            # Fallback: always set call_Rating__c to something
            payload['call_Rating__c'] = clean_salesforce_text_value("Call Rating: Unknown")
    
    # Keep other variations for backward compatibility if needed
    if payload.get('call_Rating__c'):
        payload['Call_Rating__c'] = payload.get('Call_Rating__c') or payload['call_Rating__c']
        payload['call_rating__c'] = payload.get('call_rating__c') or payload['call_Rating__c']

    uui_id = _normalize_string(payload.get('UUI') or payload.get('uui'))

    opportunity_id = (
        payload.get('Opportunity__c')
        or payload.get('opportunity__c')
        or payload.get('OpportunityId')
        or payload.get('opportunityId')
        or payload.get('opportunity_id_c')
    )
    if not opportunity_id and isinstance(payload.get('opportunity'), dict):
        opportunity_id = (
            payload['opportunity'].get('tagged_id')
            or payload['opportunity'].get('opportunity_id_c')
            or payload['opportunity'].get('id')
        )
    if not opportunity_id and uui_id and str(uui_id).startswith('006'):
        opportunity_id = uui_id
    opportunity_id = _normalize_string(opportunity_id)
    if opportunity_id and str(opportunity_id).startswith('006'):
        payload['Opportunity__c'] = opportunity_id

    lead_id = (
        payload.get('Lead__c')
        or payload.get('lead__c')
        or payload.get('LeadId')
        or payload.get('leadId')
        or payload.get('WhoId')
    )
    if not lead_id and isinstance(payload.get('lead'), dict):
        lead_id = (
            payload['lead'].get('tagged_id')
            or payload['lead'].get('id')
            or payload['lead'].get('WhoId')
        )
    if not lead_id and uui_id and str(uui_id).startswith('00Q'):
        lead_id = uui_id
    lead_id = _normalize_string(lead_id)
    if lead_id and str(lead_id).startswith('00Q'):
        payload['Lead__c'] = lead_id

    # Add full analysis data as Additional_Data__c
    try:
        payload['Additional_Data__c'] = clean_salesforce_text_value(json.dumps(data, ensure_ascii=False))
    except Exception:
        payload['Additional_Data__c'] = None

    return payload


def validate_analysis_payload(data, file_key):
    # Check for either 'call_rating' or 'call_Rating'
    has_rating = bool(data.get('call_rating') or data.get('call_Rating'))
    required_fields = ['monitorUCID', 'call_transcription', 'call_insight', 'call_sentiment_analysis']
    missing = [f for f in required_fields if not data.get(f)]
    if not has_rating:
        missing.append('call_rating/call_Rating')

    # Check for lead or opportunity
    has_lead_or_opp = (
        (data.get('lead') and isinstance(data.get('lead'), dict) and data.get('lead').get('tagged_id'))
        or (data.get('Lead__c'))
        or (data.get('opportunity') and isinstance(data.get('opportunity'), dict) and data.get('opportunity').get('tagged_id'))
        or (data.get('Opportunity__c'))
    )
    if not has_lead_or_opp:
        logger.error(f"[VALIDATION FAILED] {file_key} missing lead or opportunity tagging")
        missing.append('lead/opportunity')

    if missing:
        logger.error(f"[VALIDATION FAILED] {file_key} missing canonical fields: {missing}")
        return False

    transcript_block = data.get('call_transcription')
    if not isinstance(transcript_block, dict):
        logger.error(f"[VALIDATION FAILED] {file_key} call_transcription is not a dict")
        return False

    if not (
        transcript_block.get('raw_transcript')
        or transcript_block.get('separated_transcript')
        or transcript_block.get('rawTranscript')
        or transcript_block.get('separatedTranscript')
    ):
        logger.error(f"[VALIDATION FAILED] {file_key} missing transcript text")
        return False

    rating_block = data.get('call_rating') or data.get('call_Rating')
    if isinstance(rating_block, dict):
        if not (rating_block.get('rating') or rating_block.get('quality') or rating_block.get('score')):
            logger.error(f"[VALIDATION FAILED] {file_key} missing rating value")
            return False

    logger.info(f"[VALIDATION PASSED] {file_key} has canonical analysis fields")
    return True


def should_skip_salesforce_push(data):
    payload = data or {}
    transcript_block = payload.get("call_transcription") or {}
    transcript_text = payload.get("Separated_Transcript__c") or payload.get("Transcript__c") or ""
    if isinstance(transcript_block, dict):
        transcript_text = (
            transcript_block.get("separated_transcript")
            or transcript_block.get("separatedTranscript")
            or transcript_block.get("raw_transcript")
            or transcript_block.get("rawTranscript")
            or transcript_text
        )
    elif isinstance(transcript_block, str):
        transcript_text = transcript_block
    return str(transcript_text).strip() == SHORT_CALL_TRANSCRIPT


def _salesforce_text_matches(value, expected):
    return str(value or "").strip().lower() == expected.lower()


def _salesforce_text_contains(value, expected):
    return expected.lower() in str(value or "").strip().lower()


def is_missing_audio_placeholder_payload(payload):
    payload = payload or {}
    description = payload.get("Description__c")
    description_is_blank = description is None or str(description).strip().lower() in ("", "null", "none")
    rating_text = (
        payload.get("call_Rating__c")
        or payload.get("Call_Rating__c")
        or payload.get("call_rating__c")
        or payload.get("Rating__c")
    )

    return (
        _salesforce_text_matches(payload.get("Sentiment__c"), "Unknown")
        and _salesforce_text_matches(payload.get("Transcript__c"), MISSING_AUDIO_TRANSCRIPT)
        and _salesforce_text_matches(payload.get("Separated_Transcript__c"), MISSING_AUDIO_TRANSCRIPT)
        and _salesforce_text_matches(payload.get("Call_Insight__c"), MISSING_AUDIO_REASON)
        and _salesforce_text_contains(rating_text, MISSING_AUDIO_REASON)
        and description_is_blank
    )


# ============================================================
# JSON OUTPUT BUILDER
# ============================================================

def create_json_output(file_key, transcript, insights, callquality, separated, customer_details, call_to_action=None):
    logger.info(f"Processing insights: {insights!r}")
    json_data = get_json_from_cos(COS_BUCKET, file_key)

    try:
        initial_output = build_analysis_payload(
            file_key=file_key,
            transcript=transcript,
            insights=insights,
            callquality=callquality,
            separated=separated,
            customer_details=customer_details,
            call_to_action=call_to_action,
            json_data=json_data,
            source="create_json_output"
        )

        logger.info(f"[CREATE_JSON] COS JSON keys for enrichment: {list((json_data or {}).keys())}")
        output_data = enrich_response_with_lead_opportunity(initial_output, json_data)
        
        logger.info(f"[CREATE_JSON] Full output keys: {list(output_data.keys())}")
        json_str    = json.dumps(output_data, indent=2, ensure_ascii=False)
        temp_path   = get_output_json_path(file_key)
        filename    = os.path.basename(temp_path)
        _atomic_write_json(temp_path, output_data)

        if has_request_context():
            session['json_filename']    = filename
            session['json_path']        = temp_path
            logger.info(f"Stored JSON output in session: {temp_path}")
        else:
            logger.info(f"Created JSON output (batch mode): {temp_path}")
        return json_str
    except Exception as e:
        logger.error(f"Failed to create JSON output: {e}")
        return json.dumps({"error": "Failed to create JSON output"})


# ============================================================
# AUDIO PROCESSING PIPELINE
# ============================================================

def process_audio_with_assisto(filename, apikey, json_data):
    try:
        with open(filename, 'rb') as audio_file:
            language      = (json_data or {}).get('language') or ASSISTO_LANGUAGE or 'hi'
            workflow_name = (json_data or {}).get('workflow_name') or ASSISTO_WORKFLOW_NAME or 'diarized_transcription'
            files = {'file': audio_file}
            data  = {'language': language, 'workflow_name': workflow_name}

            logger.info(f"Sending audio to Assisto API: {ASSISTO_API_URL}")
            sess = requests.Session()
            retries = Retry(
                total=10, connect=5, read=5, backoff_factor=5,
                status_forcelist=[429, 500, 502, 503, 504],
                allowed_methods=["POST"], raise_on_status=False
            )
            adapter = HTTPAdapter(max_retries=retries, pool_connections=1, pool_maxsize=1, pool_block=False)
            sess.mount('https://', adapter)
            sess.mount('http://', adapter)
            if proxies:
                sess.proxies.update(proxies)
            sess.headers.update({'Connection': 'close', 'User-Agent': 'call-analysis-app/1.0'})

            headers = {}
            token = ASSISTO_API_TOKEN or apikey
            if token:
                headers["Authorization"] = f"Bearer {token}"
            else:
                logger.warning("No Assisto API token provided")

            enforce_request_gap('assisto_transcription', ASSISTO_TRANSCRIPTION_INTERVAL_SECONDS)
            try:
                response = sess.post(ASSISTO_API_URL, headers=headers, files=files, data=data, timeout=(30, 600))
            except requests.exceptions.SSLError as e:
                logger.warning(f"Assisto SSL error: {e}")
                raise

            if response.status_code == 200:
                raw_response = response.text.strip()
                try:
                    response_data = json.loads(raw_response)
                    if isinstance(response_data, dict) and 'transcription' in response_data:
                        combined = normalize_transcript_payload(response_data['transcription'])
                        return {"status": "success", "transcription": combined, "response": response_data}
                    elif isinstance(response_data, dict):
                        combined = normalize_transcript_payload(response_data)
                        if combined and combined != "No transcription available":
                            return {"status": "success", "transcription": combined, "response": response_data}
                    elif isinstance(response_data, list):
                        combined = normalize_transcript_payload(response_data)
                        return {"status": "success", "transcription": combined, "response": response_data}
                    else:
                        return {"status": "success", "transcription": normalize_transcript_payload(raw_response), "response": response_data}
                except ValueError:
                    if blind_text_response(response.text.strip()):
                        return {"status": "success", "transcription": normalize_transcript_payload(response.text), "response": response.text}
                    return {"status": "success", "transcription": None, "response": response.text}
            else:
                logger.error(f"Assisto API failed: {response.status_code}: {response.text}")
                return {"status": "error", "response": f"API request failed with status code {response.status_code}"}
    except requests.RequestException as e:
        logger.error(f"Assisto request error: {e}")
        return {"status": "error", "response": f"Error during API request: {e}"}
    except FileNotFoundError:
        logger.error(f"File not found: {filename}")
        return {"status": "error", "response": f"File not found: {filename}"}
    except Exception as e:
        logger.error(f"Unexpected error in process_audio_with_assisto: {e}", exc_info=True)
        return {"status": "error", "response": f"Unexpected error: {str(e)}"}


def process_audio_from_cos(file_key):
    errors = []
    filename = None
    if has_request_context():
        session.clear()
        logger.info("Cleared session for web interface processing")
    else:
        logger.info("Skipping session clear in batch processing mode")
    try:
        logger.info(f"Processing file from COS: {file_key}")
        _check_batch_pause(file_key, "processing start")

        if not is_root_bucket_json_key(file_key):
            logger.warning(f"Skipping non-root or non-JSON COS key: {file_key}")
            errors.append("Only root bucket JSON files are allowed.")
            return errors, None, None, None, None, None, None

        json_data = get_json_from_cos(COS_BUCKET, file_key)
        _check_batch_pause(file_key, "JSON retrieval")
        if not json_data:
            errors.append("Failed to retrieve or parse JSON data.")
            return errors, None, None, None, None, None, None

        audio_url    = json_data.get('AudioFile', '').strip()
        monitor_ucid = get_monitor_ucid(json_data)

        if not audio_url or not urllib.parse.urlparse(audio_url).scheme:
            logger.info(f"File {file_key} has no valid AudioFile URL")
            transcript = "No audio URL is available"
            create_json_output(file_key, transcript, ["Not processed due to missing URL", "Unknown"], "Not processed due to missing URL", transcript, None, None)
            return errors, transcript, ["Not processed due to missing URL", "Unknown"], "Not processed due to missing URL", transcript, None, None

        duration_str     = json_data.get('CallDuration', '00:00:00')
        duration_seconds = parse_call_duration(duration_str)

        if duration_seconds <= 15:
            logger.info(f"File {file_key} has duration <= 15 seconds ({duration_seconds}s)")
            transcript = "Call is less than 15 sec"
            create_json_output(file_key, transcript, ["Not processed due to short duration", "Unknown"], "Not processed due to short duration", transcript, None, None)
            return errors, transcript, ["Not processed due to short duration", "Unknown"], "Not processed due to short duration", transcript, None, None

        logger.info(f"Downloading audio from {audio_url}")
        filename = download_audio_file(audio_url, monitor_ucid)
        _check_batch_pause(file_key, "audio download")
        if not filename:
            errors.append("Failed to download audio file.")
            return errors, None, None, None, None, None, None

        assisto_result = process_audio_with_assisto(filename, json_data.get('Apikey'), json_data)
        _check_batch_pause(file_key, "transcription")
        if assisto_result["status"] == "error":
            errors.append(f"Assisto API error: {assisto_result['response']}")
            return errors, None, None, None, None, None, None

        transcript = assisto_result.get("transcription", "No transcription available.")
        logger.info(f"Assisto transcription length: {len(transcript)}")

        _check_batch_pause(file_key, "analysis scheduling")
        with ThreadPoolExecutor(max_workers=5) as executor:
            future_separated      = executor.submit(Separatespeakers, transcript)
            future_insights       = executor.submit(Getinsights, transcript)
            future_callquality    = executor.submit(Getcallquality, transcript)
            future_customerdetails = executor.submit(GetCustomerDetails, transcript)
            future_calltoaction   = executor.submit(GetCallToAction, transcript)

            separated_transcript = future_separated.result()
            insights             = future_insights.result()
            callquality          = future_callquality.result()
            customer_details     = future_customerdetails.result()
            call_to_action       = future_calltoaction.result()

        _check_batch_pause(file_key, "analysis completion")
        logger.info(f"Separated transcript length: {len(separated_transcript)}")
        logger.info(f"Insights: {insights}")
        logger.info(f"Call quality: {callquality}")
        logger.info(f"Customer details: {customer_details}")
        logger.info(f"Call to action: {call_to_action}")

        create_json_output(file_key, transcript, insights, callquality, separated_transcript, customer_details, call_to_action)
        return errors, separated_transcript, insights, callquality, transcript, customer_details, call_to_action

    except Exception as e:
        logger.error(f"Unexpected error in process_audio_from_cos: {e}")
        errors.append(f"Error processing {file_key}: {e}")
        return errors, None, None, None, None, None, None
    finally:
        if filename and os.path.exists(filename):
            try:
                os.remove(filename)
                logger.info(f"Temporary file {filename} deleted.")
            except OSError as e:
                logger.error(f"Failed to delete temporary file: {e}")


# ============================================================
# WATSONX TOKEN + SESSION
# ============================================================

def access_token():
    now = time.time()
    with watsonx_token_lock:
        cached  = watsonx_token_cache.get('access_token')
        expires = watsonx_token_cache.get('expires_at', 0.0)
        if cached and now < expires:
            return cached

    headers = {"Content-Type": "application/x-www-form-urlencoded", "Accept": "application/json"}
    data    = {"grant_type": "urn:ibm:params:oauth:grant-type:apikey", "apikey": WATSONX_API_KEY}
    try:
        sess = requests.Session()
        retries = Retry(
            total=10, connect=5, read=5, backoff_factor=5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["POST"], raise_on_status=False
        )
        adapter = HTTPAdapter(max_retries=retries, pool_connections=1, pool_maxsize=1, pool_block=False)
        sess.mount('https://', adapter)
        sess.mount('http://', adapter)
        if proxies:
            sess.proxies.update(proxies)
        sess.headers.update({'Connection': 'close', 'User-Agent': 'call-analysis-app/1.0'})
        response = sess.post(
            WATSONX_AUTH_URL, headers=headers, data=data,
            timeout=(WATSONX_AUTH_CONNECT_TIMEOUT_SECONDS, WATSONX_AUTH_READ_TIMEOUT_SECONDS)
        )
        response.raise_for_status()
        token = response.json()['access_token']
        with watsonx_token_lock:
            watsonx_token_cache['access_token'] = token
            watsonx_token_cache['expires_at']   = time.time() + WATSONX_TOKEN_CACHE_SECONDS
        return token
    except requests.RequestException as e:
        logger.error(f"Failed to get WatsonX access token: {e}")
        raise


def create_watsonx_session():
    sess = requests.Session()
    retries = Retry(
        total=20, connect=8, read=8, backoff_factor=3,
        status_forcelist=[429, 500, 502, 503, 504, 101],
        allowed_methods=["POST"], raise_on_status=False
    )
    adapter = HTTPAdapter(max_retries=retries, pool_connections=1, pool_maxsize=1, pool_block=False)
    sess.mount('https://', adapter)
    sess.mount('http://', adapter)
    if proxies:
        sess.proxies.update(proxies)
    sess.headers.update({'Connection': 'close', 'User-Agent': 'call-analysis-app/1.0'})
    sess.verify = VERIFY_SSL_CERTIFICATES
    return sess


def post_to_watsonx(sess, headers, body, operation_name, retry_count=0, max_retries=5):
    try:
        enforce_request_gap('watsonx_inference', WATSONX_REQUEST_GAP_SECONDS)
        response = sess.post(
            WATSONX_URL, headers=headers, json=body,
            timeout=(WATSONX_CONNECT_TIMEOUT_SECONDS, WATSONX_READ_TIMEOUT_SECONDS),
            verify=VERIFY_SSL_CERTIFICATES
        )
        response.raise_for_status()
        return response
    except requests.exceptions.HTTPError as e:
        status_code = e.response.status_code if e.response is not None else None
        if status_code == 401 and retry_count < max_retries:
            logger.warning(f"WatsonX 401 during {operation_name} — refreshing token")
            with watsonx_token_lock:
                watsonx_token_cache['access_token'] = None
                watsonx_token_cache['expires_at']   = 0.0
            refreshed_headers = dict(headers)
            refreshed_headers["Authorization"] = f"Bearer {access_token()}"
            fresh = create_watsonx_session()
            try:
                return post_to_watsonx(fresh, refreshed_headers, body, operation_name, retry_count + 1, max_retries)
            finally:
                fresh.close()
        logger.error(
            f"WatsonX HTTP error during {operation_name}: {status_code} - "
            f"{(e.response.text or '')[:300] if e.response is not None else str(e)}"
        )
        raise
    except (requests.exceptions.SSLError, requests.exceptions.ConnectionError,
            requests.exceptions.Timeout, requests.exceptions.ChunkedEncodingError) as e:
        logger.warning(f"WatsonX connection error during {operation_name} (attempt {retry_count+1}/{max_retries}): {type(e).__name__}: {str(e)[:200]}")
        if retry_count < max_retries:
            wait_time = (2 ** retry_count) + 1
            logger.info(f"Retrying {operation_name} in {wait_time}s...")
            time.sleep(wait_time)
            fresh = create_watsonx_session()
            try:
                return post_to_watsonx(fresh, headers, body, operation_name, retry_count + 1, max_retries)
            finally:
                fresh.close()
        logger.error(f"WatsonX {operation_name} failed after {max_retries} retries")
        raise


def extract_watsonx_generated_text(response, operation_name):
    try:
        payload = response.json()
    except ValueError as e:
        raise ValueError(f"WatsonX returned invalid JSON during {operation_name}") from e
    results = payload.get('results')
    if not isinstance(results, list) or not results:
        raise ValueError(f"WatsonX returned no results during {operation_name}")
    first_result = results[0] or {}
    generated_text = first_result.get('generated_text')
    if not isinstance(generated_text, str):
        raise ValueError(f"WatsonX returned no generated_text during {operation_name}")
    return generated_text


def first_token_or_default(value, default="Unknown"):
    text = (value or "").strip()
    if not text:
        return default
    parts = text.split()
    return parts[0] if parts else default


# ============================================================
# TRANSCRIPT CHUNKER
# ============================================================

def chunk_transcript(transcript, max_chunk_length=1500, overlap_size=100):
    """
    Smart transcript chunker with:
    - Better sentence splitting
    - Overlapping context between chunks for continuity
    - Higher max chunk size
    - Preserves sentence boundaries
    """
    try:
        if not transcript or not transcript.strip():
            return ["No transcription available"]
        
        transcript = transcript.strip()
        
        # Better sentence splitting - handles multiple punctuation types
        # Split on: .!?; followed by whitespace or end of string
        sentences = re.split(r'(?<=[.!?;])\s+', transcript)
        
        # Clean up empty sentences
        sentences = [s.strip() for s in sentences if s.strip()]
        
        if not sentences:
            logger.warning("No valid sentences found, falling back to fixed-length chunks")
            return [transcript[i:i+max_chunk_length] for i in range(0, len(transcript), max_chunk_length)]
        
        chunks = []
        current_chunk = []
        current_length = 0
        
        for sentence in sentences:
            sl = len(sentence)
            
            # If adding this sentence would exceed max length, finish current chunk
            if current_length + sl > max_chunk_length and current_chunk:
                # Join current chunk and add to chunks
                chunk_text = ' '.join(current_chunk)
                chunks.append(chunk_text)
                
                # Now, create new chunk with overlap: take last few sentences from previous chunk
                # Find how many sentences to keep for overlap (around overlap_size chars)
                overlap_text = ''
                overlap_length = 0
                for prev_sent in reversed(current_chunk):
                    if overlap_length + len(prev_sent) <= overlap_size:
                        overlap_text = prev_sent + ' ' + overlap_text
                        overlap_length += len(prev_sent) + 1
                    else:
                        break
                
                # Start new chunk with overlap + current sentence
                if overlap_text.strip():
                    current_chunk = [overlap_text.strip(), sentence]
                    current_length = len(overlap_text.strip()) + 1 + sl
                else:
                    current_chunk = [sentence]
                    current_length = sl
            else:
                # Handle extra-long single sentences that exceed max_chunk_length
                if sl > max_chunk_length:
                    # Finish any current chunk first
                    if current_chunk:
                        chunks.append(' '.join(current_chunk))
                        current_chunk = []
                        current_length = 0
                    
                    # Split this very long sentence into overlapping sub-chunks
                    long_sent_chunks = []
                    for i in range(0, len(sentence), max_chunk_length - overlap_size):
                        start = max(0, i - overlap_size)
                        end = min(i + max_chunk_length - overlap_size, len(sentence))
                        sub_chunk = sentence[start:end]
                        if sub_chunk.strip():
                            long_sent_chunks.append(sub_chunk.strip())
                    chunks.extend(long_sent_chunks)
                else:
                    current_chunk.append(sentence)
                    current_length += sl
        
        # Add the last chunk
        if current_chunk:
            chunks.append(' '.join(current_chunk))
        
        # Fallback if all else fails
        if not chunks:
            chunks = [transcript[i:i+max_chunk_length] for i in range(0, len(transcript), max_chunk_length)]
        
        logger.info(f"Split transcript into {len(chunks)} chunks: {[len(c) for c in chunks]}")
        return chunks
    except Exception as e:
        logger.error(f"Failed to chunk transcript: {e}", exc_info=True)
        return [transcript[i:i+max_chunk_length] for i in range(0, len(transcript), max_chunk_length)]


# ============================================================
# WATSONX ANALYSIS FUNCTIONS
# ============================================================

def determine_speaker_roles(transcript):
    sales_keywords    = ['offer', 'purchase', 'buy', 'product', 'service', 'plan', 'package',
                         'discount', 'deal', 'promotion', 'sale', 'subscription', 'upgrade',
                         'interested in buying', 'would you like to', 'we have an offer']
    inquiry_keywords  = ['help', 'support', 'issue', 'problem', 'question', 'how do i',
                         'where is', 'when will', 'complaint', 'not working', 'need assistance',
                         'information about', 'tell me about', 'explain']
    lower_transcript  = transcript.lower()
    sales_count       = sum(1 for k in sales_keywords if k in lower_transcript)
    inquiry_count     = sum(1 for k in inquiry_keywords if k in lower_transcript)
    if sales_count > inquiry_count:
        return 'sales'
    elif inquiry_count > sales_count:
        return 'inquiry'
    else:
        return 'sales' if ('would you like' in lower_transcript or 'interested in' in lower_transcript) else 'inquiry'


def process_chunk_separatespeakers(chunk, chunk_index, total_chunks, call_type='sales'):
    start_time = time.time()
    prompt = (
        "Convert the following transcript into a conversation format with turns labeled as "
        "\"Speaker1:\" or \"Speaker2:\". The transcript may contain English and Hindi text. "
        "Alternate speakers naturally, ensuring each turn is on a new line.\n"
        f"Transcript:\n{chunk}\n"
        "Output format:\nSpeaker1: [Text]\nSpeaker2: [Text]"
    )
    body = {
        "input": prompt,
        "parameters": {
            "decoding_method": "greedy", "max_new_tokens": 1000,
            "min_new_tokens": 30, "repetition_penalty": 1.05, "temperature": 0.1
        },
        "model_id": WATSONX_MODEL_ID,
        "project_id": WATSONX_PROJECT_ID
    }
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token()}"
    }
    try:
        logger.info(f"Separatespeakers chunk {chunk_index+1}/{total_chunks} len={len(chunk)}")
        with create_watsonx_session() as sess:
            response      = post_to_watsonx(sess, headers, body, "Separatespeakers")
            response_text = extract_watsonx_generated_text(response, "Separatespeakers")
        logger.info(f"Separatespeakers chunk {chunk_index+1} done in {time.time()-start_time:.2f}s")
        with open('app.log', 'a', encoding='utf-8') as f:
            f.write(f"{datetime.now()} - Separatespeakers chunk {chunk_index+1} response: {response_text}\n")

        clean_lines = []
        for line in response_text.split('\n'):
            line = line.strip()
            if not line or "transcript" in line.lower() or "conversation" in line.lower():
                continue
            if line.startswith(('Speaker1:', 'Speaker2:', 'Speaker 1:', 'Speaker 2:',
                                '**Speaker1**:', '**Speaker2**:', '**Speaker 1**:', '**Speaker 2**:')):
                line = (line
                        .replace('Speaker 1:', 'Speaker1:').replace('Speaker 2:', 'Speaker2:')
                        .replace('**Speaker1**:', 'Speaker1:').replace('**Speaker2**:', 'Speaker2:')
                        .replace('**Speaker 1**:', 'Speaker1:').replace('**Speaker 2**:', 'Speaker2:'))
                if call_type == 'sales':
                    line = line.replace('Speaker1:', 'Agent:').replace('Speaker2:', 'Customer:')
                else:
                    line = line.replace('Speaker1:', 'Customer:').replace('Speaker2:', 'Agent:')
                clean_lines.append(line)

        # Guard against WatsonX repetition loops: if the model gets stuck
        # repeating the same exchange (e.g. "Customer: ... Agent: Ok sir no
        # problem sir. Thank you have a good day." dozens of times), drop
        # the runaway repeats. Keep at most 2 consecutive occurrences of any
        # identical line.
        deduped_lines = []
        repeat_count = 0
        for line in clean_lines:
            if deduped_lines and line == deduped_lines[-1]:
                repeat_count += 1
                if repeat_count >= 2:
                    continue
            else:
                repeat_count = 0
            deduped_lines.append(line)

        # Also catch repeated PAIRS of lines (e.g. a Customer/Agent exchange
        # that repeats as a 2-line block over and over).
        final_lines = []
        i = 0
        while i < len(deduped_lines):
            if (
                i + 3 < len(deduped_lines)
                and deduped_lines[i] == deduped_lines[i + 2]
                and deduped_lines[i + 1] == deduped_lines[i + 3]
            ):
                # Found a repeating 2-line block; keep one occurrence and
                # skip the rest of the run.
                final_lines.append(deduped_lines[i])
                final_lines.append(deduped_lines[i + 1])
                j = i + 2
                while (
                    j + 1 < len(deduped_lines)
                    and deduped_lines[j] == deduped_lines[i]
                    and deduped_lines[j + 1] == deduped_lines[i + 1]
                ):
                    j += 2
                i = j
            else:
                final_lines.append(deduped_lines[i])
                i += 1

        clean_lines = final_lines
        return chunk_index, '\n'.join(clean_lines) if clean_lines else chunk
    except requests.RequestException as e:
        logger.error(f"Separatespeakers chunk {chunk_index+1} failed: {e}")
        sentences = chunk.split('.')
        alternate_lines = []
        is_speaker1 = True
        for sentence in sentences:
            sentence = sentence.strip()
            if sentence:
                speaker = ("Agent" if is_speaker1 else "Customer") if call_type == 'sales' else ("Customer" if is_speaker1 else "Agent")
                alternate_lines.append(f"{speaker}: {sentence}.")
                is_speaker1 = not is_speaker1
        return chunk_index, '\n'.join(alternate_lines)


def Separatespeakers(trans):
    try:
        call_type = determine_speaker_roles(trans)
        logger.info(f"Detected call type: {call_type}")
        chunks = chunk_transcript(trans)
        separated_chunks = [''] * len(chunks)
        with ThreadPoolExecutor(max_workers=WATSONX_MAX_WORKERS) as executor:
            futures = [executor.submit(process_chunk_separatespeakers, chunk, i, len(chunks), call_type) for i, chunk in enumerate(chunks)]
            for future in futures:
                chunk_index, result = future.result()
                separated_chunks[chunk_index] = result
                time.sleep(1)
        combined = '\n'.join(separated_chunks)
        if combined.strip():
            logger.info(f"Combined separated transcript length: {len(combined)}")
            return combined
        logger.warning("No valid separated transcript produced, returning original")
        return trans
    except Exception as e:
        logger.error(f"Unexpected error in Separatespeakers: {e}")
        return trans


def process_chunk_getcallquality(chunk, chunk_index, total_chunks):
    start_time = time.time()
    low_info_patterns = [
        r'^((?:Agent|Customer): (?:sir|hello|hi|bye)\.? ?)+$',
        r'^\s*$'
    ]
    if any(re.match(pattern, chunk.strip(), re.IGNORECASE) for pattern in low_info_patterns):
        logger.info(f"Skipping low-info chunk {chunk_index+1}/{total_chunks}")
        return chunk_index, None

    body = {
        "input": f"""
        Analyze the call quality of the following conversation transcript, which may contain English and Hindi text. Focus on meaningful interactions such as customer requests, agent responses, and issue resolution. Ignore repetitive greetings or farewells. Provide a concise summary of the customer's request and the agent's action, a call rating as a single number from 0 to 10, and multiple specific reasons for the rating.
        Output format:
        Add-on Request by Customer: [Customer request]
        Action Taken for the request: [Agent action]
        Call Rating: [X]
        Reason: [First specific reason]
        Reason: [Second specific reason]
        Reason: [Third specific reason]
        Transcript:
        {chunk or 'No transcription available'}
        """,
        "parameters": {
            "decoding_method": "greedy", "max_new_tokens": 500,
            "min_new_tokens": 50, "stop_sequences": ["/"],
            "repetition_penalty": 1.1, "temperature": 0.1
        },
        "model_id": WATSONX_MODEL_ID,
        "project_id": WATSONX_PROJECT_ID
    }
    headers = {
        "Accept": "application/json", "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token()}"
    }
    try:
        logger.info(f"Getcallquality chunk {chunk_index+1}/{total_chunks} len={len(chunk)}")
        with create_watsonx_session() as sess:
            response = post_to_watsonx(sess, headers, body, "Getcallquality")
            result   = extract_watsonx_generated_text(response, "Getcallquality")
        logger.info(f"Getcallquality chunk {chunk_index+1} done in {time.time()-start_time:.2f}s")
        with open('app.log', 'a', encoding='utf-8') as f:
            f.write(f"{datetime.now()} - Getcallquality chunk {chunk_index+1} response: {result}\n")

        req, action, rating, reasons = "No request identified", "No action identified", "Unknown", []
        for line in result.split('\n'):
            line = line.strip()
            if line.startswith("Add-on Request by Customer:"):
                req = line[len("Add-on Request by Customer:"):].strip()
            elif line.startswith("Action Taken for the request:"):
                action = line[len("Action Taken for the request:"):].strip()
            elif line.startswith("Call Rating:"):
                rating = normalize_rating_value(line[len("Call Rating:"):].strip())
            elif line.startswith("Reason:"):
                reason = line[len("Reason:"):].strip()
                if reason and reason not in reasons:
                    reasons.append(reason)
        return chunk_index, {"request": req, "action": action, "rating": rating, "reasons": reasons or ["No reason provided"]}
    except requests.RequestException as e:
        logger.error(f"Getcallquality chunk {chunk_index+1} failed: {e}")
        return chunk_index, None


def Getcallquality(trans):
    chunks = chunk_transcript(trans)
    quality_results = [None] * len(chunks)
    with ThreadPoolExecutor(max_workers=WATSONX_MAX_WORKERS) as executor:
        futures = [executor.submit(process_chunk_getcallquality, chunk, i, len(chunks)) for i, chunk in enumerate(chunks)]
        for future in futures:
            chunk_index, result = future.result()
            quality_results[chunk_index] = result
            time.sleep(1)

    valid_results = [r for r in quality_results if r is not None]
    if not valid_results:
        return "Error: No meaningful call quality results"

    reqs, actions, all_reasons, ratings = [], [], [], []
    for r in valid_results:
        if r["request"] != "No request identified":
            reqs.append(r["request"])
        if r["action"] != "No action identified":
            actions.append(r["action"])
        all_reasons.extend(r["reasons"])
        rv = rating_text_to_float(r["rating"])
        if rv is not None:
            ratings.append(rv)

    combined_request = " ".join(reqs) or "No specific customer request identified"
    combined_action  = " ".join(actions) or "No specific action taken by agent"
    unique_reasons   = list(dict.fromkeys(r for r in all_reasons if r != "No reason provided")) or ["No specific reason provided"]
    model_avg        = round(sum(ratings) / len(ratings), 1) if ratings else None
    estimated        = estimate_call_rating(trans, request_text=combined_request, action_text=combined_action, reasons=unique_reasons, model_rating=model_avg)
    combined_rating  = f"{estimated:g}/10"

    logger.info(f"Call rating estimated at {estimated:g}/10" + (f" (model avg {model_avg:g}/10)" if model_avg else ""))

    result_str = f"Add-on Request by Customer: {combined_request}\nAction Taken for the request: {combined_action}\nCall Rating: {combined_rating}"
    for reason in unique_reasons:
        result_str += f"\nReason: {reason}"
    return result_str


def process_chunk_getinsights(chunk, chunk_index, total_chunks):
    start_time = time.time()
    body = {
        "input": f"""
        Provide an insight summary and sentiment for the following conversation transcript, which may contain English and Hindi text.
        The sentiment must be exactly one word: "Positive", "Negative", or "Neutral".
        Output format:
        Insights Summary: insight summary...
        Sentiment: [Positive|Negative|Neutral]
        Transcript:
        {chunk or 'No transcription available'}
        """,
        "parameters": {
            "decoding_method": "greedy", "max_new_tokens": 1000,
            "min_new_tokens": 30, "stop_sequences": ["/"],
            "repetition_penalty": 1.05, "temperature": 0.5
        },
        "model_id": WATSONX_MODEL_ID,
        "project_id": WATSONX_PROJECT_ID
    }
    headers = {
        "Accept": "application/json", "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token()}"
    }
    try:
        logger.info(f"Getinsights chunk {chunk_index+1}/{total_chunks} len={len(chunk)}")
        with create_watsonx_session() as sess:
            response       = post_to_watsonx(sess, headers, body, "Getinsights")
            generated_text = extract_watsonx_generated_text(response, "Getinsights")
        logger.info(f"Getinsights chunk {chunk_index+1} done in {time.time()-start_time:.2f}s")
        with open('app.log', 'a', encoding='utf-8') as f:
            f.write(f"{datetime.now()} - Getinsights chunk {chunk_index+1} response: {generated_text}\n")

        insight_summary, sentiment = "No summary available", "Unknown"
        for line in generated_text.split('\n'):
            line = line.strip()
            if line.startswith("Insights Summary:"):
                insight_summary = line[len("Insights Summary:"):].strip()
            elif line.startswith("Sentiment:"):
                sentiment_text = line[len("Sentiment:"):].strip()
                first_word = sentiment_text.split()[0] if sentiment_text else "Unknown"
                sentiment = first_word if first_word in ["Positive", "Negative", "Neutral"] else "Unknown"
        return chunk_index, (insight_summary, sentiment)
    except requests.RequestException as e:
        logger.error(f"Getinsights chunk {chunk_index+1} failed: {e}")
        if len(chunk) > 400:
            logger.info(f"Retrying chunk {chunk_index+1} with smaller size")
            smaller_chunks = chunk_transcript(chunk, max_chunk_length=400)
            for i, small_chunk in enumerate(smaller_chunks):
                try:
                    body["input"] = f"""
                    Provide an insight summary and sentiment for the following conversation transcript.
                    Output format:
                    Insights Summary: insight summary...
                    Sentiment: [Positive|Negative|Neutral]
                    Transcript:
                    {small_chunk or 'No transcription available'}
                    """
                    with create_watsonx_session() as sess:
                        response       = post_to_watsonx(sess, headers, body, f"Getinsights retry {chunk_index+1}.{i+1}")
                        generated_text = extract_watsonx_generated_text(response, f"Getinsights retry {chunk_index+1}.{i+1}")
                    insight_summary, sentiment = "Partial summary from retry", "Unknown"
                    for line in generated_text.split('\n'):
                        line = line.strip()
                        if line.startswith("Insights Summary:"):
                            insight_summary = line[len("Insights Summary:"):].strip()
                        elif line.startswith("Sentiment:"):
                            st = line[len("Sentiment:"):].strip()
                            fw = st.split()[0] if st else "Unknown"
                            sentiment = fw if fw in ["Positive", "Negative", "Neutral"] else "Unknown"
                    return chunk_index, (insight_summary, sentiment)
                except requests.RequestException as e2:
                    logger.error(f"Failed retry sub-chunk {chunk_index+1}.{i+1}: {e2}")
                    continue
        return chunk_index, ("Error: Unable to retrieve insights", "Unknown")


def Getinsights(trans):
    chunks = chunk_transcript(trans)
    results = [None] * len(chunks)
    with ThreadPoolExecutor(max_workers=WATSONX_MAX_WORKERS) as executor:
        futures = [executor.submit(process_chunk_getinsights, chunk, i, len(chunks)) for i, chunk in enumerate(chunks)]
        for future in futures:
            chunk_index, result = future.result()
            results[chunk_index] = result
            time.sleep(1)

    summaries  = [r[0] for r in results if r]
    sentiments = [r[1] for r in results if r]
    combined_summary   = " ".join(s for s in summaries if s and "Error" not in s) or "No insights available"
    valid_sentiments   = [s for s in sentiments if s in ["Positive", "Negative", "Neutral"]]
    combined_sentiment = max(set(valid_sentiments), key=valid_sentiments.count) if valid_sentiments else "Unknown"
    return [combined_summary, combined_sentiment]


def process_chunk_getcustomerdetails(chunk, chunk_index, total_chunks):
    start_time = time.time()
    low_info_patterns = [r'^((?:Agent|Customer): (?:sir|hello|hi|bye)\.? ?)+$', r'^\s*$']
    if any(re.match(pattern, chunk.strip(), re.IGNORECASE) for pattern in low_info_patterns):
        return chunk_index, None

    body = {
        "input": f"""
        Analyze the following conversation transcript to determine the caller's tone/emotion, customer's tone/emotion, customer's intent, and customer's urgency.
        - Caller tone/emotion: Exactly one word from 'Rude', 'Polite', 'Neutral'.
        - Customer tone/emotion: Exactly one word from 'Interested', 'Just enquired'.
        - Customer intent: A short phrase (1-2 words) like 'Purchase', 'Complaint', 'Inquiry'.
        - Customer urgency: Exactly one word from 'High', 'Medium', 'Low'.
        Output format:
        Caller Tone: [Rude|Polite|Neutral]
        Customer Tone: [Interested|Just enquired]
        Customer Intent: [Short phrase]
        Customer Urgency: [High|Medium|Low]
        Transcript:
        {chunk or 'No transcription available'}
        """,
        "parameters": {
            "decoding_method": "greedy", "max_new_tokens": 600,
            "min_new_tokens": 20, "stop_sequences": ["/"],
            "repetition_penalty": 1.05, "temperature": 0.3
        },
        "model_id": WATSONX_MODEL_ID,
        "project_id": WATSONX_PROJECT_ID
    }
    headers = {
        "Accept": "application/json", "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token()}"
    }
    try:
        logger.info(f"GetCustomerDetails chunk {chunk_index+1}/{total_chunks} len={len(chunk)}")
        with create_watsonx_session() as sess:
            response = post_to_watsonx(sess, headers, body, "GetCustomerDetails")
            result   = extract_watsonx_generated_text(response, "GetCustomerDetails")
        logger.info(f"GetCustomerDetails chunk {chunk_index+1} done in {time.time()-start_time:.2f}s")
        with open('app.log', 'a', encoding='utf-8') as f:
            f.write(f"{datetime.now()} - GetCustomerDetails chunk {chunk_index+1} response: {result}\n")

        caller_tone, customer_tone, intent, urgency = "Unknown", "Unknown", "Unknown", "Unknown"
        for line in result.split('\n'):
            line = line.strip()
            if line.startswith("Caller Tone:"):
                t = first_token_or_default(line[len("Caller Tone:"):].strip())
                caller_tone = t if t in ["Rude", "Polite", "Neutral"] else "Unknown"
            elif line.startswith("Customer Tone:"):
                t = first_token_or_default(line[len("Customer Tone:"):].strip())
                customer_tone = t if t in ["Interested", "Just enquired"] else "Unknown"
            elif line.startswith("Customer Intent:"):
                i = line[len("Customer Intent:"):].strip()
                intent = i if len(i.split()) <= 2 else "Unknown"
            elif line.startswith("Customer Urgency:"):
                u = first_token_or_default(line[len("Customer Urgency:"):].strip())
                urgency = u if u in ["High", "Medium", "Low"] else "Unknown"
        return chunk_index, {"caller_tone": caller_tone, "customer_tone": customer_tone, "intent": intent, "urgency": urgency}
    except requests.RequestException as e:
        logger.error(f"GetCustomerDetails chunk {chunk_index+1} failed: {e}")
        return chunk_index, None


def GetCustomerDetails(trans):
    chunks = chunk_transcript(trans)
    details_results = [None] * len(chunks)
    with ThreadPoolExecutor(max_workers=WATSONX_MAX_WORKERS) as executor:
        futures = [executor.submit(process_chunk_getcustomerdetails, chunk, i, len(chunks)) for i, chunk in enumerate(chunks)]
        for future in futures:
            chunk_index, result = future.result()
            details_results[chunk_index] = result
            time.sleep(1)

    valid_results = [r for r in details_results if r is not None]
    if not valid_results:
        return {"caller_tone": "Unknown", "customer_tone": "Unknown", "intent": "Unknown", "urgency": "Unknown"}

    caller_tones   = [r["caller_tone"]   for r in valid_results if r["caller_tone"]   != "Unknown"]
    customer_tones = [r["customer_tone"] for r in valid_results if r["customer_tone"] != "Unknown"]
    intents        = [r["intent"]        for r in valid_results if r["intent"]        != "Unknown"]
    urgencies      = [r["urgency"]       for r in valid_results if r["urgency"]       != "Unknown"]

    return {
        "caller_tone":   max(set(caller_tones),   key=caller_tones.count)   if caller_tones   else "Unknown",
        "customer_tone": max(set(customer_tones), key=customer_tones.count) if customer_tones else "Unknown",
        "intent":        max(set(intents),        key=intents.count)        if intents        else "Unknown",
        "urgency":       max(set(urgencies),      key=urgencies.count)      if urgencies      else "Unknown"
    }


def process_chunk_getcalltoaction(chunk, chunk_index, total_chunks):
    start_time     = time.time()
    normalized_chunk = normalize_transcript_payload(chunk)
    body = {
        "input": f"""
        Analyze the following conversation transcript and extract only concrete customer requests or follow-up needs.
        Include things like price enquiries, unit availability, location/tower details, callback requests, and contact-sharing for follow-up.
        Do not include generic statements, greetings, or agent actions.
        If no concrete customer request exists, return exactly: Action: None
        Output format:
        Action: [short customer request]
        Transcript:
        {normalized_chunk or 'No transcription available'}
        """,
        "parameters": {
            "decoding_method": "greedy", "max_new_tokens": 300,
            "min_new_tokens": 10, "repetition_penalty": 1.05, "temperature": 0.1
        },
        "model_id": WATSONX_MODEL_ID,
        "project_id": WATSONX_PROJECT_ID
    }
    headers = {
        "Accept": "application/json", "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token()}"
    }
    try:
        logger.info(f"GetCallToAction chunk {chunk_index+1}/{total_chunks}")
        with create_watsonx_session() as sess:
            response = post_to_watsonx(sess, headers, body, "GetCallToAction")
            result   = extract_watsonx_generated_text(response, "GetCallToAction")
        logger.info(f"GetCallToAction chunk {chunk_index+1} done in {time.time()-start_time:.2f}s")

        action_items = []
        for line in result.split('\n'):
            line = line.strip()
            norm = re.sub(r'^\d+\.\s*', '', line)
            if norm.lower().startswith('action:'):
                action_text = norm.split(':', 1)[1].strip()
                if action_text and action_text.lower() not in {"none", "no specific actions identified", "no action items identified"}:
                    action_items.append(action_text)
            elif norm.startswith(('•', '-', '*')):
                action_text = norm.lstrip('•-* ').strip()
                if action_text and "no specific action" not in action_text.lower():
                    action_items.append(action_text)
        return chunk_index, action_items if action_items else []
    except requests.RequestException as e:
        logger.error(f"GetCallToAction chunk {chunk_index+1} failed: {e}")
        return chunk_index, []


def GetCallToAction(trans):
    normalized = normalize_transcript_payload(trans)
    chunks = chunk_transcript(normalized)
    action_results = [None] * len(chunks)
    with ThreadPoolExecutor(max_workers=WATSONX_MAX_WORKERS) as executor:
        futures = [executor.submit(process_chunk_getcalltoaction, chunk, i, len(chunks)) for i, chunk in enumerate(chunks)]
        for future in futures:
            chunk_index, result = future.result()
            action_results[chunk_index] = result
            time.sleep(1)

    all_actions = []
    for result in action_results:
        if result:
            all_actions.extend(result)

    unique_actions = []
    seen = set()
    for action in all_actions:
        if action.lower() not in seen:
            seen.add(action.lower())
            unique_actions.append(action)

    return unique_actions if unique_actions else _fallback_call_to_action_items(normalized)


# ============================================================
# SALESFORCE TOKEN + PUSH
# ============================================================

def get_salesforce_access_token():
    now = time.time()
    with salesforce_token_lock:
        cached  = salesforce_token_cache.get('access_token')
        expires = salesforce_token_cache.get('expires_at', 0.0)
        if cached and now < expires:
            logger.debug("Using cached Salesforce access token")
            return cached

    headers = {"Content-Type": "application/x-www-form-urlencoded", "Accept": "application/json"}
    token_urls = [SALESFORCE_TOKEN_URL]
    standard_url = "https://login.salesforce.com/services/oauth2/token"
    if SALESFORCE_TOKEN_URL != standard_url:
        token_urls.append(standard_url)

    auth_payloads = [
        {"grant_type": "client_credentials", "client_id": SALESFORCE_CLIENT_ID, "client_secret": SALESFORCE_CLIENT_SECRET},
        {"grant_type": "password", "client_id": SALESFORCE_CLIENT_ID, "client_secret": SALESFORCE_CLIENT_SECRET,
         "username": SALESFORCE_USERNAME, "password": SALESFORCE_PASSWORD}
    ]

    last_error = None
    for payload in auth_payloads:
        for token_url in token_urls:
            try:
                t0 = time.time()
                response = requests.post(token_url, headers=headers, data=payload, timeout=(30, 60))
                response.raise_for_status()
                response_json = response.json()
                token = response_json.get("access_token")
                if not token:
                    raise ValueError("Salesforce token response did not include access_token")
                expires_in = response_json.get("expires_in")
                if not isinstance(expires_in, (int, float)) or expires_in <= 0:
                    expires_in = SALESFORCE_TOKEN_CACHE_SECONDS
                expires_at = time.time() + min(expires_in, SALESFORCE_TOKEN_CACHE_SECONDS)
                with salesforce_token_lock:
                    salesforce_token_cache['access_token'] = token
                    salesforce_token_cache['expires_at']   = expires_at
                logger.info(f"Salesforce token obtained from {token_url} grant={payload['grant_type']} in {time.time()-t0:.2f}s")
                return token
            except requests.HTTPError as e:
                resp_text = (e.response.text or "").strip()[:500] if e.response is not None else ""
                logger.warning(f"Salesforce token failed for {token_url} grant={payload['grant_type']}: {e}. Response: {resp_text}")
                last_error = e
            except (requests.RequestException, ValueError) as e:
                logger.warning(f"Salesforce token failed for {token_url} grant={payload['grant_type']}: {e}")
                last_error = e

    logger.error(f"Failed to get Salesforce access token: {last_error}")
    raise last_error


def check_duplicate_in_salesforce(monitor_ucid, token):
    """Check Salesforce if a record with this monitorUcid__c already exists."""
    monitor_ucid = str(monitor_ucid or '').strip()
    if not monitor_ucid or monitor_ucid == "Unknown":
        return False, None

    try:
        # Parse base URL from SALESFORCE_API_URL
        base_url = SALESFORCE_API_URL
        if "/services/data/" in base_url:
            base_url = base_url.split("/services/data/")[0]
        
        # SOQL query to find existing record
        soql = f"SELECT Id FROM Call_Transcript__c WHERE monitorUcid__c = '{_escape_soql_string(monitor_ucid)}' LIMIT 1"
        query_url = f"{base_url}/services/data/v58.0/query?q={requests.utils.quote(soql)}"
        
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        response = requests.get(query_url, headers=headers, timeout=(30, 60))
        
        if response.status_code == 200:
            response_json = response.json()
            if response_json.get('totalSize', 0) > 0:
                return True, response_json.get('records', [{}])[0].get('Id')
        
        return False, None
    except Exception as e:
        logger.error(f"Error checking duplicate in Salesforce for monitorUCID {monitor_ucid}: {str(e)[:200]}")
        return False, None


def should_skip_existing_salesforce_record(file_key, json_data):
    monitor_ucid = get_monitor_ucid(json_data)
    if not monitor_ucid or monitor_ucid == "Unknown":
        logger.info(f"[PREFLIGHT] {file_key} has no monitorUCID; duplicate check skipped")
        return False

    try:
        _check_batch_pause(file_key, "Salesforce duplicate preflight")
        token = get_salesforce_access_token()
        duplicate_exists, sf_record_id = check_duplicate_in_salesforce(monitor_ucid, token)
        if not duplicate_exists:
            return False

        logger.info(
            f"[PREFLIGHT SKIP] {file_key} - monitorUcid__c {monitor_ucid} "
            f"already exists in Salesforce (Id: {sf_record_id})"
        )
        _record_push_event(
            'already_pushed',
            file_key=file_key,
            monitor_ucid=monitor_ucid,
            salesforce_id=sf_record_id,
            preflight=True
        )
        release_push_attempt(file_key, success=True, monitor_ucid=monitor_ucid)
        persist_pushed_file(file_key, monitor_ucid=monitor_ucid)
        return True
    except BatchPauseRequested:
        raise
    except Exception as e:
        logger.warning(f"[PREFLIGHT] Duplicate check failed for {file_key}; continuing with processing: {str(e)[:200]}")
        return False


def push_to_salesforce(data, file_key):
    _check_batch_pause(file_key, "Salesforce push preparation")

    if has_request_context() and not is_file_key_in_current_window(file_key):
        logger.warning(f"[PUSH SKIP] {file_key} outside allowed COS LastModified date window {_describe_cos_date_window()}")
        return 'out_of_range'

    input_payload = data if isinstance(data, dict) else {}

    # ============================================================
    # DEBUG LOGGING - Check what we have before extraction
    # ============================================================
    logger.info(f"[PUSH DEBUG] {file_key} - input_payload keys: {list(input_payload.keys())}")
    logger.info(f"[PUSH DEBUG] {file_key} - lead in payload: {input_payload.get('lead')}")
    logger.info(f"[PUSH DEBUG] {file_key} - opportunity in payload: {input_payload.get('opportunity')}")
    logger.info(f"[PUSH DEBUG] {file_key} - UUI: {input_payload.get('UUI') or input_payload.get('uui')}")
    
    # ============================================================
    # EXTRACT OPPORTUNITY AND LEAD IDs
    # ============================================================
    opportunity_id, lead_id = extract_salesforce_link_ids(input_payload)
    logger.info(f"[PUSH DEBUG] {file_key} - Extracted opportunity_id: {opportunity_id}")
    logger.info(f"[PUSH DEBUG] {file_key} - Extracted lead_id: {lead_id}")

    if should_skip_salesforce_push(input_payload):
        if is_missing_audio_payload(input_payload):
            logger.info(f"[PUSH SKIP] {file_key} has no audio URL placeholder data; not pushing")
        else:
            logger.info(f"[PUSH SKIP] {file_key} is a short call; not pushing")
        return 'short_duration'

    # ============================================================
    # BUILD PAYLOAD
    # ============================================================
    payload_to_send = {}

    # Preserve already-flat Salesforce payloads as well as nested analysis output.
    for field in ("monitorUcid__c", "Transcript__c", "Separated_Transcript__c", "Call_Insight__c", "Sentiment__c"):
        if input_payload.get(field):
            payload_to_send[field] = clean_salesforce_text_value(input_payload.get(field))
    for field in ("call_Rating__c", "Call_Rating__c", "call_rating__c"):
        if input_payload.get(field):
            payload_to_send[field] = clean_salesforce_text_value(input_payload.get(field))
    
    # Add monitorUcid__c
    if input_payload.get("monitorUCID"):
        payload_to_send["monitorUcid__c"] = input_payload["monitorUCID"]

    # Add transcript fields
    transcription_data = input_payload.get("call_transcription", {})
    if isinstance(transcription_data, dict):
        raw_transcript = clean_salesforce_text_value(transcription_data.get("raw_transcript", ""))
        separated_transcript = clean_salesforce_text_value(transcription_data.get("separated_transcript", ""))
        if raw_transcript:
            payload_to_send["Transcript__c"] = raw_transcript
        if separated_transcript:
            payload_to_send["Separated_Transcript__c"] = separated_transcript

    # Add call insight field
    insight_data = input_payload.get("call_insight", {})
    if isinstance(insight_data, dict):
        insight_summary = clean_salesforce_text_value(insight_data.get("summary", ""))
        if insight_summary:
            payload_to_send["Call_Insight__c"] = insight_summary

    # Add sentiment field
    sentiment_data = input_payload.get("call_sentiment_analysis", {})
    if isinstance(sentiment_data, dict) and sentiment_data.get("sentiment"):
        payload_to_send["Sentiment__c"] = sentiment_data.get("sentiment", "")

    # Add call rating field (from "quality" key)
    rating_data = input_payload.get("call_rating", {})
    if isinstance(rating_data, dict):
        rating_value = rating_data.get("quality", "Unknown")
        # Try to parse and format the rating nicely
        try:
            parsed_rating = parse_call_quality(rating_value)
            if parsed_rating.get("rating") and parsed_rating.get("rating") != "Unknown":
                rating_parts = [f"Call Rating: {parsed_rating['rating']}"]
                if parsed_rating.get("reasons"):
                    rating_parts.extend([f"Reason {i + 1}: {reason}" for i, reason in enumerate(parsed_rating["reasons"])])
                final_rating_text = " | ".join(rating_parts)
            else:
                final_rating_text = f"Call Rating: {rating_value}"
        except Exception as e:
            final_rating_text = f"Call Rating: {rating_value}"
        payload_to_send["call_Rating__c"] = payload_to_send.get("call_Rating__c") or clean_salesforce_text_value(final_rating_text)
        # Also add variations for compatibility
        payload_to_send["Call_Rating__c"] = payload_to_send["call_Rating__c"]
        payload_to_send["call_rating__c"] = payload_to_send["call_Rating__c"]

    # ============================================================
    # ADD OPPORTUNITY AND LEAD IDs (with validation)
    # ============================================================
    if opportunity_id:
        payload_to_send["Opportunity__c"] = opportunity_id
        payload_to_send["OpportunityId"] = opportunity_id
        logger.info(f"[PUSH] {file_key} - ✓ Setting Opportunity__c = {opportunity_id} and OpportunityId = {opportunity_id}")
    else:
        logger.warning(f"[PUSH] {file_key} - ✗ No opportunity_id found to set Opportunity__c/OpportunityId")
    
    if lead_id:
        payload_to_send["Lead__c"] = lead_id
        payload_to_send["LeadId"] = lead_id
        logger.info(f"[PUSH] {file_key} - ✓ Setting Lead__c = {lead_id} and LeadId = {lead_id}")
    else:
        logger.warning(f"[PUSH] {file_key} - ✗ No lead_id found to set Lead__c/LeadId")
    
    # Also check if there are direct fields in the input payload
    if input_payload.get("Opportunity__c") and not opportunity_id:
        logger.warning(f"[PUSH LINK CHECK] Ignoring invalid Opportunity__c={input_payload.get('Opportunity__c')!r}")
    if input_payload.get("Lead__c") and not lead_id:
        logger.warning(f"[PUSH LINK CHECK] Ignoring invalid Lead__c={input_payload.get('Lead__c')!r}")

    # Keep the original nested fields too (optional, for debugging)
    for field in ["monitorUCID", "call_transcription", "call_insight", "call_rating", "call_sentiment_analysis", "opportunity", "lead"]:
        if input_payload.get(field):
            payload_to_send[field] = input_payload[field]

    logger.info(f"[PUSH] {file_key} - payload_to_send keys: {list(payload_to_send.keys())}")
    logger.info(f"[PUSH] {file_key} - Opportunity__c: {payload_to_send.get('Opportunity__c')}")
    logger.info(f"[PUSH] {file_key} - Lead__c: {payload_to_send.get('Lead__c')}")

    # ============================================================
    # VALIDATION
    # ============================================================
    has_lead_or_opp = payload_to_send.get("Opportunity__c") or payload_to_send.get("Lead__c")
    has_transcript = payload_to_send.get("Transcript__c") or payload_to_send.get("Separated_Transcript__c")
    
    if not has_lead_or_opp:
        logger.error(f"[PUSH ABORT] {file_key} - No lead or opportunity tagged")
        logger.error(f"[PUSH ABORT] {file_key} - Opportunity__c: {payload_to_send.get('Opportunity__c')}")
        logger.error(f"[PUSH ABORT] {file_key} - Lead__c: {payload_to_send.get('Lead__c')}")
        return 'validation_failed'
    
    if not has_transcript:
        logger.error(f"[PUSH ABORT] {file_key} - No transcript data")
        return 'validation_failed'

    if should_skip_salesforce_push(payload_to_send):
        if is_missing_audio_payload(payload_to_send):
            logger.info(f"[PUSH SKIP] {file_key} has no audio URL placeholder data; not pushing")
        else:
            logger.info(f"[PUSH SKIP] {file_key} is a short call; not pushing")
        return 'short_duration'

    monitor_ucid = payload_to_send.get("monitorUcid__c") or "Unknown"
    reservation = reserve_push_attempt(file_key, monitor_ucid=monitor_ucid)

    if reservation == 'already_pushed':
        logger.info(f"[PUSH SKIP] {file_key} already pushed")
        return 'already_pushed'
    if reservation == 'push_in_progress':
        logger.info(f"[PUSH SKIP] {file_key} push already in progress")
        return 'push_in_progress'

    push_start = time.time()
    with push_stats_lock:
        push_stats['total_push_attempts'] += 1
        push_stats['last_push_time'] = datetime.now().isoformat()

    try:
        separated_transcript = payload_to_send.get("Separated_Transcript__c") or ""
        transcript_length = len(separated_transcript) if separated_transcript else 0

        if separated_transcript:
            with push_stats_lock:
                push_stats['total_transcriptions_pushed'] += 1
            logger.info(f"[PUSH] Pushing transcription for {file_key} — {transcript_length} chars")

        _check_batch_pause(file_key, "Salesforce access token request")
        t0 = time.time()
        token = get_salesforce_access_token()
        access_dur = time.time() - t0
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

        # Check for duplicate in Salesforce
        if monitor_ucid and monitor_ucid != "Unknown":
            duplicate_exists, sf_record_id = check_duplicate_in_salesforce(monitor_ucid, token)
            if duplicate_exists:
                logger.info(f"[PUSH SKIP] {file_key} - Record with monitorUcid__c {monitor_ucid} already exists in Salesforce (Id: {sf_record_id})")
                _record_push_event('already_pushed', file_key=file_key, monitor_ucid=monitor_ucid, transcript_length=transcript_length)
                release_push_attempt(file_key, success=False, monitor_ucid=monitor_ucid)
                return 'already_pushed'

        logger.info(f"[PUSH LINK CHECK] {file_key} | Opportunity__c={payload_to_send.get('Opportunity__c') or 'None'} | Lead__c={payload_to_send.get('Lead__c') or 'None'}")
        
        # Log the full payload for debugging (truncated)
        payload_str = json.dumps(payload_to_send, ensure_ascii=False)
        logger.info(f"Salesforce payload for {file_key}: {payload_str[:3000]}")

        enforce_request_gap('salesforce_push', SALESFORCE_PUSH_INTERVAL_SECONDS)
        _check_batch_pause(file_key, "Salesforce API request")
        t1 = time.time()
        response = requests.post(SALESFORCE_API_URL, headers=headers, json=payload_to_send, timeout=(30, 300))
        request_dur = time.time() - t1
        total_dur = time.time() - push_start

        if response.status_code in (200, 201, 204):
            with push_stats_lock:
                push_stats['successful_pushes'] += 1
                current_pushed = push_stats['successful_pushes']
            logger.info(f"[PUSH SUCCESS] {file_key} | Total={current_pushed} | access={access_dur:.2f}s req={request_dur:.2f}s total={total_dur:.2f}s")
            
            # Try to get Salesforce record ID from response to verify
            salesforce_id = None
            try:
                response_json = response.json()
                salesforce_id = response_json.get('id')
                logger.info(f"[VERIFY] Got Salesforce record ID: {salesforce_id}")
            except Exception as e:
                logger.debug(f"[VERIFY] Could not parse Salesforce response for ID: {e}")
            
            # Verify the record if we have an ID
            if salesforce_id:
                verify_salesforce_record(salesforce_id, payload_to_send, token)
            
            _record_push_event('success', file_key=file_key, monitor_ucid=monitor_ucid, transcript_length=transcript_length,
                               status_code=response.status_code, access_time=round(access_dur, 2),
                               request_time=round(request_dur, 2), total_time=round(total_dur, 2),
                               salesforce_id=salesforce_id)
            release_push_attempt(file_key, success=True, monitor_ucid=monitor_ucid)
            persist_pushed_file(file_key, monitor_ucid=monitor_ucid)
            return 'pushed'
        else:
            with push_stats_lock:
                push_stats['failed_pushes'] += 1
                push_stats['transcription_errors'] += 1
            response_text = response.text or ''
            logger.error(f"[PUSH FAILED] {file_key} | Status={response.status_code} | Response={response_text[:500]} | total={total_dur:.2f}s")
            _record_push_event('failed', file_key=file_key, monitor_ucid=monitor_ucid, transcript_length=transcript_length,
                               status_code=response.status_code, error=response_text[:200],
                               access_time=round(access_dur, 2), request_time=round(request_dur, 2), total_time=round(total_dur, 2))
            release_push_attempt(file_key, success=False, monitor_ucid=monitor_ucid)

            # Detect the Apex governor-limit error
            if 'LimitException' in response_text and 'Too many query rows' in response_text:
                logger.error(
                    f"[PUSH ABORT - APEX LIMIT] {file_key} | Salesforce Apex class "
                    f"CreateCallTranscriptFromJsonAPI is hitting a SOQL governor limit "
                    f"(Too many query rows: 50001). This is independent of payload "
                    f"content and will fail for ALL pushes until fixed on the "
                    f"Salesforce/Apex side. Escalate to Salesforce admin."
                )
                return 'apex_limit_error'

            return 'failed'

    except BatchPauseRequested:
        release_push_attempt(file_key, success=False, monitor_ucid=monitor_ucid)
        return 'paused'
    except (requests.RequestException, ValueError) as e:
        total_dur = time.time() - push_start
        with push_stats_lock:
            push_stats['failed_pushes'] += 1
            push_stats['transcription_errors'] += 1
        logger.error(f"[PUSH ERROR] {file_key} | {str(e)[:100]} | total={total_dur:.2f}s")
        _record_push_event('error', file_key=file_key, error=str(e)[:200], total_time=round(total_dur, 2))
        release_push_attempt(file_key, success=False)
        return 'failed'
    except Exception as e:
        logger.error(f"Unexpected push error for {file_key}: {e}", exc_info=True)
        with push_stats_lock:
            push_stats['failed_pushes'] += 1
        release_push_attempt(file_key, success=False)
        return 'failed'


def verify_salesforce_record(salesforce_id, expected_payload, token):
    """
    Verify the created Salesforce record by querying it back and checking fields.
    Logs any discrepancies or null fields.
    """
    if not salesforce_id:
        logger.warning("[VERIFY] No Salesforce ID provided to verify")
        return

    # Parse Salesforce API URL to get base URL
    base_url = SALESFORCE_API_URL
    if "/services/data/" in base_url:
        base_url = base_url.split("/services/data/")[0]

    # Query the record
    query_fields = ", ".join(SALESFORCE_OUTPUT_FIELDS)
    query_url = f"{base_url}/services/data/v58.0/sobjects/Call_Transcript__c/{salesforce_id}?fields={query_fields}"

    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    try:
        response = requests.get(query_url, headers=headers, timeout=(30, 60))
        if response.status_code == 200:
            sf_record = response.json()
            logger.info(f"[VERIFY] Retrieved Salesforce record {salesforce_id}")
            
            # Check each field
            null_fields = []
            for field in SALESFORCE_OUTPUT_FIELDS:
                sf_value = sf_record.get(field)
                expected_value = expected_payload.get(field)
                
                if sf_value in (None, "", [], {}):
                    null_fields.append(field)
                    logger.warning(f"[VERIFY] Field '{field}' is NULL/empty in Salesforce!")
                else:
                    logger.debug(f"[VERIFY] Field '{field}': OK (value present)")
            
            if null_fields:
                logger.warning(f"[VERIFY] Record {salesforce_id} has NULL fields: {null_fields}")
            else:
                logger.info(f"[VERIFY] Record {salesforce_id} has all fields populated!")
            
            return sf_record
        else:
            logger.error(f"[VERIFY] Failed to retrieve record {salesforce_id}: {response.status_code} {response.text[:300]}")
            return None
    except Exception as e:
        logger.error(f"[VERIFY] Error verifying record {salesforce_id}: {str(e)[:200]}")
        return None


# ============================================================
# RATE LIMIT WRAPPERS
# ============================================================

ASSISTO_CALLS_PER_MINUTE   = 60
WATSONX_CALLS_PER_MINUTE   = 120
SALESFORCE_CALLS_PER_MINUTE = 100

@sleep_and_retry
@limits(calls=ASSISTO_CALLS_PER_MINUTE, period=60)
def rate_limited_assisto_call(*args, **kwargs):
    return process_audio_with_assisto(*args, **kwargs)

@sleep_and_retry
@limits(calls=WATSONX_CALLS_PER_MINUTE, period=60)
def rate_limited_watsonx_call(*args, **kwargs):
    return create_watsonx_session().post(*args, **kwargs)

@sleep_and_retry
@limits(calls=SALESFORCE_CALLS_PER_MINUTE, period=60)
def rate_limited_salesforce_call(*args, **kwargs):
    return requests.post(*args, **kwargs)


# ============================================================
# BATCH PROCESSING
# ============================================================

def process_single_json(file_key):
    logger.info(f"Processing JSON file: {file_key}")
    try:
        _check_batch_pause(file_key, "file start")
        json_data = get_json_from_cos(COS_BUCKET, file_key)
        if not json_data:
            logger.error(f"Failed to retrieve JSON data from COS for {file_key}")
            return file_key, 'failed', ["Failed to retrieve JSON data from COS"]
        if should_skip_existing_salesforce_record(file_key, json_data):
            return file_key, 'already_pushed', []

        errors, separated, insights, callquality, transcript, customer_details, call_to_action = process_audio_from_cos(file_key)

        logger.warning(f"[EXTRACTION RESULTS] {file_key} | transcript:{len(transcript) if transcript else 0} chars | insights:{bool(insights)} | quality:{bool(callquality)} | errors:{errors}")

        if errors or (not transcript and transcript not in ["No audio URL is available", "Call is less than 15 sec"]):
            logger.error(f"Failed to process {file_key}: {errors or 'No valid transcript'}")
            return file_key, False, errors or ["No valid transcript"]

        transcript_length = len(transcript) if transcript else 0
        logger.debug(f"[FILE DETAILS] {file_key} | transcript={transcript_length} chars | insights={bool(insights)} | actions={len(call_to_action) if call_to_action else 0}")

        json_str  = create_json_output(file_key, transcript, insights, callquality, separated, customer_details, call_to_action)

        # Read back the saved file to get the fully enriched payload
        # (create_json_output writes enrichment to disk; reading it back
        #  ensures push_to_salesforce receives the complete enriched dict)
        temp_path   = get_output_json_path(file_key)
        output_data = _load_json_file(temp_path) or json.loads(json_str)

        logger.info(f"[PUSH ATTEMPT] Starting push for {file_key} (~{transcript_length} chars) | enriched keys: {list(output_data.keys())}")
        logger.debug(f"[ENRICHED DATA] Full structure: {json.dumps({k: type(v).__name__ for k, v in output_data.items()}, ensure_ascii=False)}")
        has_compact_fields = any(output_data.get(k) for k in ['Transcript__c', 'Separated_Transcript__c', 'Call_Insight__c', 'Sentiment__c', 'call_Rating__c'])
        has_canonical_fields = any(output_data.get(k) for k in ['call_transcription', 'call_insight', 'call_sentiment_analysis', 'call_rating'])
        logger.warning(
            f"[DATA CHECKPOINT] {file_key} | compact:{has_compact_fields} | "
            f"transcript:{bool(output_data.get('call_transcription') or output_data.get('Transcript__c'))} | "
            f"insight:{bool(output_data.get('call_insight') or output_data.get('Call_Insight__c'))} | "
            f"sentiment:{bool(output_data.get('call_sentiment_analysis') or output_data.get('Sentiment__c'))} | "
            f"rating:{bool(output_data.get('call_rating') or output_data.get('call_Rating__c'))}"
        )
        if not (has_canonical_fields or has_compact_fields):
            logger.error(f"[DATA EMPTY] {file_key} - All enrichment fields missing! Raw json_str keys: {list(json.loads(json_str).keys()) if json_str else 'None'}")
        _check_batch_pause(file_key, "Salesforce push")
        push_status = push_to_salesforce(output_data, file_key)

        if push_status == 'pushed':
            logger.info(f"[PUSHED] {file_key}")
            return file_key, 'pushed', []
        elif push_status == 'short_duration':
            return file_key, 'short_duration', []
        elif push_status == 'missing_audio_url':
            return file_key, 'missing_audio_url', []
        elif push_status == 'already_pushed':
            return file_key, 'already_pushed', []
        elif push_status == 'push_in_progress':
            return file_key, 'push_in_progress', []
        elif push_status == 'out_of_range':
            return file_key, 'out_of_range', ["File is outside the allowed date window"]
        elif push_status == 'validation_failed':
            return file_key, 'validation_failed', ["Payload missing required enrichment fields"]
        elif push_status == 'apex_limit_error':
            return file_key, 'apex_limit_error', [
                "Salesforce Apex error: 'Too many query rows: 50001' in "
                "CreateCallTranscriptFromJsonAPI. This is a Salesforce-side "
                "issue unrelated to this file's data and will recur for every "
                "file until fixed by a Salesforce admin."
            ]
        else:
            return file_key, 'failed', ["Failed to push to Salesforce"]

    except BatchPauseRequested as e:
        logger.info(str(e))
        return file_key, 'paused', [str(e)]
    except Exception as e:
        logger.error(f"Error processing {file_key}: {e}")
        return file_key, 'failed', [str(e)]


def process_and_push_all_jsons(max_workers=4, batch_size=100):
    global PAUSE_PROCESSING, CANCEL_PROCESSING, BATCH_PROCESSING_COMPLETED, BATCH_PUSH_LIMIT_REACHED, batch_thread
    processed_files, failed_files, pushed_files, current_batch_start, total_files = set(), [], set(), 0, 0

    if not batch_run_lock.acquire(blocking=False):
        logger.warning("Batch processing already running; skipping duplicate start")
        return

    try:
        logger.info("Starting batch processing of JSON files")
        start_time = time.time()
        cos_files  = get_cos_files()
        if not cos_files:
            logger.warning("No JSON files found to process")
            return

        processed_files, failed_files, pushed_files, pushed_monitor_ucids, current_batch_start, total_files = load_batch_state()
        processed_files, failed_files, pushed_files = filter_batch_state_to_current_window(
            processed_files, failed_files, pushed_files, cos_files
        )
        sync_pushed_file_registry(pushed_files, pushed_monitor_ucids)
        total_files = len(cos_files)

        if not processed_files:
            processed_files, failed_files, current_batch_start = set(), [], 0

        pending_indexes = [i for i, fk in enumerate(cos_files) if fk not in processed_files and fk not in pushed_files]
        current_batch_start = pending_indexes[0] if pending_indexes else total_files

        total_processed = len(processed_files)
        total_pushed    = len(pushed_files)
        run_pushed      = 0

        for batch_start in range(current_batch_start, total_files, batch_size):
            if run_pushed >= BATCH_PUSH_LIMIT:
                BATCH_PUSH_LIMIT_REACHED = True
                save_batch_state(processed_files, failed_files, pushed_files, pushed_monitor_registry or set(), batch_start, total_files)
                logger.info(f"[BATCH LIMIT] Stopped automatically after {run_pushed} successful pushes")
                return

            if CANCEL_PROCESSING:
                logger.info(f"Cancel requested at batch start {batch_start}")
                save_batch_state(processed_files, failed_files, pushed_files, pushed_monitor_registry or set(), batch_start, total_files)
                CANCEL_PROCESSING = False
                return

            if PAUSE_PROCESSING:
                logger.info(f"Pausing at batch start {batch_start}")
                save_batch_state(processed_files, failed_files, pushed_files, pushed_monitor_registry or set(), batch_start, total_files)
                PAUSE_PROCESSING = False
                return

            batch_files = [
                f for f in cos_files[batch_start:batch_start + batch_size]
                if f not in processed_files and f not in pushed_files
            ]
            if not batch_files:
                logger.info(f"Batch {batch_start // batch_size + 1} empty (all processed)")
                continue

            logger.info(f"Processing batch {batch_start // batch_size + 1} with {len(batch_files)} files")

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                queued        = deque(batch_files)
                future_to_file = {}
                pending        = set()

                while queued and len(pending) < max_workers and run_pushed + len(pending) < BATCH_PUSH_LIMIT:
                    if CANCEL_PROCESSING:
                        break
                    nf = queued.popleft()
                    fut = executor.submit(process_single_json, nf)
                    future_to_file[fut] = nf
                    pending.add(fut)

                pause_logged = False
                cancel_logged = False

                while pending:
                    if CANCEL_PROCESSING and not cancel_logged:
                        logger.info(f"Cancel requested; waiting for {len(pending)} in-flight files")
                        cancel_logged = True
                    elif PAUSE_PROCESSING and not pause_logged:
                        logger.info(f"Pause requested; waiting for {len(pending)} in-flight files")
                        pause_logged = True

                    done_futures, _ = wait(pending, timeout=0.5, return_when=FIRST_COMPLETED)
                    if not done_futures:
                        continue

                    for future in done_futures:
                        pending.discard(future)
                        file_key    = future_to_file[future]
                        push_status = None

                        if future.cancelled():
                            continue
                        try:
                            file_key, push_status, errors = future.result()
                            if push_status in ('pushed', 'already_pushed'):
                                total_processed += 1
                                processed_files.add(file_key)
                                pushed_files.add(file_key)
                                sync_pushed_file_registry({file_key})
                                persist_processed_file(file_key, pushed=True)
                                if push_status == 'pushed':
                                    total_pushed += 1
                                    run_pushed += 1
                            elif push_status in ('short_duration', 'missing_audio_url'):
                                total_processed += 1
                                processed_files.add(file_key)
                                persist_processed_file(file_key)
                            elif push_status == 'push_in_progress':
                                continue
                            elif push_status == 'paused':
                                logger.info(f"Paused on {file_key}")
                            else:
                                failed_files.append((file_key, errors))
                                persist_processed_file(file_key, errors=errors)
                        except BatchCancelRequested:
                            logger.info(f"Cancel processing on {file_key}")
                        except CancelledError:
                            continue
                        except Exception as e:
                            logger.error(f"Unexpected error for {file_key}: {e}")
                            failed_files.append((file_key, [str(e)]))
                            persist_processed_file(file_key, errors=[str(e)])

                    if CANCEL_PROCESSING and not pending:
                        save_batch_state(processed_files, failed_files, pushed_files, pushed_monitor_registry or set(), batch_start, total_files)
                        CANCEL_PROCESSING = False
                        logger.info("✓ BATCH PROCESSING CANCELLED")
                        return

                    if push_status == 'paused':
                        save_batch_state(processed_files, failed_files, pushed_files, pushed_monitor_registry or set(), batch_start, total_files)
                        PAUSE_PROCESSING = False
                        return

                    if run_pushed >= BATCH_PUSH_LIMIT:
                        BATCH_PUSH_LIMIT_REACHED = True
                        save_batch_state(processed_files, failed_files, pushed_files, pushed_monitor_registry or set(), batch_start, total_files)
                        logger.info(f"[BATCH LIMIT] Stopped automatically after {run_pushed} successful pushes")
                        return

                    if not CANCEL_PROCESSING and not PAUSE_PROCESSING and queued and run_pushed + len(pending) < BATCH_PUSH_LIMIT:
                        nf  = queued.popleft()
                        nft = executor.submit(process_single_json, nf)
                        future_to_file[nft] = nf
                        pending.add(nft)

                    if total_processed % 100 == 0:
                        with push_stats_lock:
                            current_pushed = push_stats['successful_pushes']
                        logger.info(f"[BATCH PROGRESS] Processed={total_processed}/{total_files} Pushed={current_pushed}")
                        save_batch_state(processed_files, failed_files, pushed_files, pushed_monitor_registry or set(), batch_start, total_files)

                if PAUSE_PROCESSING and not pending:
                    save_batch_state(processed_files, failed_files, pushed_files, pushed_monitor_registry or set(), batch_start, total_files)
                    PAUSE_PROCESSING = False
                    return

            next_batch_start = min(batch_start + batch_size, total_files)
            save_batch_state(processed_files, failed_files, pushed_files, pushed_monitor_registry or set(), next_batch_start, total_files)
            time.sleep(1)

        save_batch_state(processed_files, failed_files, pushed_files, pushed_monitor_registry or set(), total_files, total_files)
        elapsed = time.time() - start_time
        logger.info(f"[BATCH COMPLETE] elapsed={elapsed:.2f}s processed={total_processed} pushed={total_pushed} failed={len(failed_files)}")

        with push_stats_lock:
            logger.info(f"[FINAL STATS] successful={push_stats['successful_pushes']} failed={push_stats['failed_pushes']}")

        BATCH_PROCESSING_COMPLETED = True
        logger.info("✓ BATCH PROCESSING COMPLETED")

        if push_stats['total_push_attempts'] > 0:
            rate = (push_stats['successful_pushes'] / push_stats['total_push_attempts']) * 100
            logger.info(f"[PUSH SUCCESS RATE] {rate:.2f}%")

        if failed_files:
            logger.warning(f"[FAILED FILES] {len(failed_files)} files: {[f[0] for f in failed_files[:10]]}")

    except BatchCancelRequested:
        logger.info("✓ BATCH PROCESSING CANCELLED")
        save_batch_state(processed_files, failed_files, pushed_files, pushed_monitor_registry or set(), current_batch_start, total_files)
        CANCEL_PROCESSING = False
    except Exception as e:
        logger.error(f"Unexpected error in batch processing: {e}")
        save_batch_state(processed_files, failed_files, pushed_files, pushed_monitor_registry or set(), current_batch_start, total_files)
    finally:
        batch_thread = None
        batch_run_lock.release()


# ============================================================
# FLASK ROUTES
# ============================================================

@app.route('/', methods=['GET', 'POST'])
def index():
    session.setdefault('transcript_ref', None)
    session.setdefault('insights_ref', None)
    session.setdefault('callquality_ref', None)
    session.setdefault('json_filename', None)
    session.setdefault('json_path', None)
    session.setdefault('errors', [])
    session.setdefault('customer_details', {})
    session.setdefault('call_to_action', [])

    try:
        cos_json_files = get_cos_files()
    except Exception as e:
        logger.error(f"Error fetching COS files: {e}")
        cos_json_files = []
        session['errors'].append(f"Failed to fetch COS files: {str(e)}")

    errors = session.get('errors', [])
    push_stats_snapshot, recent_pushes = _get_recent_pushes_for_ui()

    if request.method == 'POST':
        selected_file = request.form.get('selected_file')
        if selected_file and selected_file in cos_json_files:
            logger.info(f"Processing selected file: {selected_file}")
            errors, separated, insights, callquality, transcript, customer_details, call_to_action = process_audio_from_cos(selected_file)
            if not errors:
                os.makedirs('temp', exist_ok=True)
                session['last_processed'] = selected_file
                session['errors'] = []
                call_transcript  = format_transcript_for_storage(separated if separated else transcript)
                insight_summary, sentiment = insights if insights else ("No insights available", "Unknown")
                call_insights    = build_insight_points(insight_summary, sentiment)
                call_quality     = parse_call_quality(callquality)
                transcript_lines = clean_transcript_lines(call_transcript)
                action_items     = clean_call_to_action_items(call_to_action)
                request_points   = build_request_points(callquality, action_items)
                
                # Create temp JSON output file for Salesforce push
                create_json_output(
                    file_key=selected_file,
                    transcript=transcript,
                    insights=insights,
                    callquality=callquality,
                    separated=separated,
                    customer_details=customer_details,
                    call_to_action=call_to_action
                )
                
                return render_template(
                    'index.html',
                    json_files=cos_json_files,
                    selected_file=selected_file,
                    call_transcript=call_transcript,
                    transcript_lines=transcript_lines,
                    call_insights=call_insights,
                    call_quality=call_quality,
                    json_filename=session.get('json_filename'),
                    errors=errors,
                    customer_details=customer_details or {},
                    call_to_action=request_points,
                    recent_pushes=recent_pushes,
                    push_stats_summary=push_stats_snapshot
                )
            else:
                session['errors'] = errors
                return render_template(
                    'index.html',
                    json_files=cos_json_files,
                    selected_file=selected_file,
                    call_transcript="Error processing file",
                    transcript_lines=[],
                    call_insights=["Error processing file"],
                    call_quality={"rating": FORCED_CALL_RATING, "reasons": ["Error processing file"]},
                    call_to_action=FORCED_CALL_TO_ACTION_ITEMS.copy(),
                    errors=errors,
                    recent_pushes=recent_pushes,
                    push_stats_summary=push_stats_snapshot
                )
        else:
            session['errors'] = ["No file selected or invalid file"]
            return render_template(
                'index.html',
                json_files=cos_json_files,
                selected_file=None,
                call_transcript="Select a JSON file to get call transcript",
                transcript_lines=[],
                call_insights=["Select a JSON file to get call insights"],
                call_quality={"rating": FORCED_CALL_RATING, "reasons": ["Select a JSON file to get call quality"]},
                call_to_action=FORCED_CALL_TO_ACTION_ITEMS.copy(),
                errors=session['errors'],
                recent_pushes=recent_pushes,
                push_stats_summary=push_stats_snapshot
            )

    return render_template(
        'index.html',
        json_files=cos_json_files,
        selected_file=None,
        call_transcript=session.get('transcript_ref', "Select a JSON file to get call transcript"),
        transcript_lines=clean_transcript_lines(session.get('transcript_ref', "")),
        call_insights=ensure_list(session.get('insights_ref'), ["Select a JSON file to get call insights"]),
        call_quality=session.get('callquality_ref', {"rating": FORCED_CALL_RATING, "reasons": ["Select a JSON file to get call quality"]}),
        json_filename=session.get('json_filename'),
        errors=errors,
        customer_details=session.get('customer_details', {}),
        call_to_action=ensure_list(session.get('call_to_action'), FORCED_CALL_TO_ACTION_ITEMS.copy()),
        recent_pushes=recent_pushes,
        push_stats_summary=push_stats_snapshot
    )


@app.route('/api_process_and_push', methods=['POST'])
def api_process_and_push():
    """Process a single file and push to Salesforce in one call."""
    try:
        data     = request.get_json(silent=True) or {}
        file_key = data.get('file_key')
        if not file_key:
            return jsonify({"error": "file_key is required"}), 400

        logger.info(f"[PROCESS & PUSH] Starting for file: {file_key}")

        # ── Step 1: Process the file ──
        json_data = get_json_from_cos(COS_BUCKET, file_key)
        if not json_data:
            return jsonify({"error": "Failed to retrieve JSON data from COS"}), 400

        logger.info(f"[API_PROCESS] COS JSON keys: {list(json_data.keys())}")

        monitor_ucid = get_monitor_ucid(json_data)
        uui = get_uui(json_data)
        audio_url    = json_data.get('AudioFile', '').strip()

        if should_skip_existing_salesforce_record(file_key, json_data):
            return jsonify({
                "status": "already_pushed",
                "message": f"monitorUcid__c {monitor_ucid} already exists in Salesforce; skipped before processing",
                "push_status": "already_pushed"
            }), 200

        if not audio_url or not urllib.parse.urlparse(audio_url).scheme:
            response = build_analysis_payload(
                file_key=file_key,
                transcript="No audio URL is available",
                insights=["Not processed due to missing URL", "Unknown"],
                callquality="Not processed due to missing URL",
                separated="No audio URL is available",
                customer_details={},
                call_to_action=FORCED_CALL_TO_ACTION_ITEMS.copy(),
                json_data=json_data,
                source="api_process_and_push:no_audio"
            )
            response = enrich_response_with_lead_opportunity(response, json_data)
            logger.warning(f"[PROCESS & PUSH] {file_key} - No audio URL")
            return jsonify({
                "status": "missing_audio_url",
                "message": "No audio URL available",
                "push_status": "missing_audio_url",
                "data": response
            }), 200

        duration_seconds = parse_call_duration(json_data.get('CallDuration', '00:00:00'))
        if duration_seconds <= 15:
            response = build_analysis_payload(
                file_key=file_key,
                transcript="Call is less than 15 sec",
                insights=["Not processed due to short duration", "Unknown"],
                callquality="Not processed due to short duration",
                separated="Call is less than 15 sec",
                customer_details={},
                call_to_action=FORCED_CALL_TO_ACTION_ITEMS.copy(),
                json_data=json_data,
                source="api_process_and_push:short_duration"
            )
            response = enrich_response_with_lead_opportunity(response, json_data)
            logger.warning(f"[PROCESS & PUSH] {file_key} - Call too short ({duration_seconds}s)")
            return jsonify({
                "status": "short_duration",
                "message": f"Call duration {duration_seconds}s is less than 15 seconds",
                "data": response
            }), 200

        # Extract transcript, insights, etc.
        errors, separated, insights, callquality, transcript, customer_details, call_to_action = process_audio_from_cos(file_key)
        if errors:
            logger.error(f"[PROCESS & PUSH] {file_key} extraction failed: {errors}")
            return jsonify({
                "status": "failed",
                "error": errors[0],
                "message": "Failed to extract audio/insights"
            }), 400

        response = build_analysis_payload(
            file_key=file_key,
            transcript=transcript,
            insights=insights,
            callquality=callquality,
            separated=separated,
            customer_details=customer_details,
            call_to_action=call_to_action,
            json_data=json_data,
            source="api_process_and_push"
        )

        response = enrich_response_with_lead_opportunity(response, json_data)

        logger.info(f"[PROCESS & PUSH] Processing complete for {file_key}, proceeding to push")

        # ── Step 2: Push to Salesforce ──
        push_status = push_to_salesforce(response, file_key)

        status_messages = {
            'pushed':          {"status": "success", "message": "Successfully processed and pushed to Salesforce"},
            'validation_failed': {"status": "validation_failed", "message": "Payload validation failed - missing enrichment fields"},
            'short_duration':  {"status": "short_duration", "message": "Call too short; not pushed"},
            'missing_audio_url': {"status": "missing_audio_url", "message": "No audio URL available; not pushed"},
            'already_pushed':  {"status": "already_pushed", "message": "Already pushed to Salesforce"},
            'push_in_progress': {"status": "push_in_progress", "message": "Push already in progress"},
            'out_of_range':    {"status": "out_of_range", "message": "File outside date window"},
            'apex_limit_error': {"status": "apex_limit_error", "message": "Salesforce Apex error: 'Too many query rows: 50001' in "
                                  "CreateCallTranscriptFromJsonAPI. Salesforce-side issue; contact Salesforce admin."},
            'failed':          {"status": "failed", "message": "Push to Salesforce failed"}
        }

        result = status_messages.get(push_status, {"status": "failed", "message": "Unknown error"})
        result["data"] = response
        result["push_status"] = push_status

        return jsonify(result), (200 if push_status == 'pushed' else 400)

    except Exception as e:
        logger.error(f"Exception in api_process_and_push: {str(e)}", exc_info=True)
        return jsonify({
            "status": "error",
            "error": "Internal server error",
            "details": str(e)
        }), 500


@app.route('/api_process', methods=['POST'])
def api_process():
    try:
        data     = request.get_json(silent=True) or {}
        file_key = data.get('file_key')
        if not file_key:
            return jsonify({"error": "file_key is required"}), 400

        logger.info(f"API request to process file: {file_key}")
        json_data = get_json_from_cos(COS_BUCKET, file_key)
        if not json_data:
            return jsonify({"error": "Failed to retrieve JSON data from COS"}), 400

        # Log all keys for debugging
        logger.info(f"[API_PROCESS] COS JSON keys: {list(json_data.keys())}")

        monitor_ucid = get_monitor_ucid(json_data)
        uui = get_uui(json_data)
        audio_url    = json_data.get('AudioFile', '').strip()

        transcript = ""
        separated = ""
        insight_summary = ""
        sentiment = ""
        callquality_val = ""
        customer_details = {}
        call_to_action = []
        opportunity_id = None

        if not audio_url or not urllib.parse.urlparse(audio_url).scheme:
            transcript = "No audio URL is available"
            separated = "No audio URL is available"
            insight_summary = "Not processed due to missing URL"
            sentiment = "Unknown"
            callquality_val = "Not processed due to missing URL"
            call_to_action = FORCED_CALL_TO_ACTION_ITEMS.copy()
        elif parse_call_duration(json_data.get('CallDuration', '00:00:00')) <= 15:
            transcript = "Call is less than 15 sec"
            separated = "Call is less than 15 sec"
            insight_summary = "Not processed due to short duration"
            sentiment = "Unknown"
            callquality_val = "Not processed due to short duration"
            call_to_action = FORCED_CALL_TO_ACTION_ITEMS.copy()
        else:
            errors, separated, insights, callquality_val, transcript, customer_details, call_to_action = process_audio_from_cos(file_key)
            if errors:
                return jsonify({"error": errors[0], "status": "failed"}), 400
            try:
                insight_summary, sentiment = insights
            except (TypeError, ValueError):
                insight_summary = "Error: Could not extract summary"
                sentiment = "Error: Could not extract sentiment"

        opportunity_id, lead_id = _extract_salesforce_link_ids(json_data)

        # Parse call quality for rating and reasons
        rating_data = parse_call_quality(callquality_val)
        rating_value = rating_data.get("rating", "Unknown")
        reasons = ensure_list(rating_data.get("reasons", []), [])

        # Build call_to_action
        final_call_to_action = build_request_points(callquality_val, call_to_action)

        # Build the response in the requested structure
        response = {
            "monitorUCID": monitor_ucid,
            "call_transcription": {
                "separated_transcript": separated
            },
            "call_insight": {
                "summary": insight_summary
            },
            "call_to_action": final_call_to_action,
            "call_Rating": {
                "rating": rating_value,
                "reasons": reasons
            },
            "call_sentiment_analysis": {
                "sentiment": sentiment
            }
        }

        # Add Opportunity__c if available
        if opportunity_id:
            response["Opportunity__c"] = opportunity_id
        if lead_id:
            response["Lead__c"] = lead_id

        logger.info(f"[API_PROCESS] Final response structured as requested")
        return jsonify(response), 200

    except Exception as e:
        logger.error(f"Exception in api_process: {str(e)}", exc_info=True)
        return jsonify({"error": "Internal server error", "details": str(e)}), 500


@app.route('/download_json')
def download_json():
    try:
        json_path = _get_safe_session_json_path()
        if json_path:
            return send_file(json_path, as_attachment=True, download_name=session.get('json_filename'))
        logger.warning("No JSON file available for download")
        return redirect(url_for('index'))
    except Exception as e:
        logger.error(f"Error downloading JSON: {e}")
        return redirect(url_for('index'))


@app.route('/process_all', methods=['GET'])
def process_all():
    try:
        started = _start_batch_thread(max_workers=4, batch_size=100)
        if not started:
            return jsonify({"status": "busy", "message": "Batch processing is already running."}), 409
        logger.info("Batch processing initiated in background")
        return jsonify({"status": "success", "message": "Batch processing started. Check logs for details."}), 200
    except Exception as e:
        logger.error(f"Error initiating batch processing: {e}")
        return jsonify({"error": "Failed to start batch processing", "details": str(e)}), 500


@app.route('/pause_process', methods=['GET'])
def pause_process():
    global PAUSE_PROCESSING
    try:
        PAUSE_PROCESSING = True
        logger.info("Pause requested")
        return jsonify({"status": "success", "message": "Batch processing pause requested."}), 200
    except Exception as e:
        logger.error(f"Error pausing batch processing: {e}")
        return jsonify({"error": "Failed to pause batch processing", "details": str(e)}), 500


@app.route('/cancel_process', methods=['GET'])
def cancel_process():
    global CANCEL_PROCESSING
    try:
        CANCEL_PROCESSING = True
        logger.info("Cancel requested")
        return jsonify({"status": "success", "message": "Batch processing cancel requested."}), 200
    except Exception as e:
        logger.error(f"Error cancelling batch processing: {e}")
        return jsonify({"error": "Failed to cancel batch processing", "details": str(e)}), 500


@app.route('/resume_process', methods=['GET'])
def resume_process():
    try:
        if os.path.exists(STATE_FILE):
            started = _start_batch_thread(max_workers=4, batch_size=100)
            if not started:
                return jsonify({"status": "busy", "message": "Batch processing is already running."}), 409
            logger.info("Batch processing resumed")
            return jsonify({"status": "success", "message": "Batch processing resumed."}), 200
        else:
            return jsonify({"error": "No paused state found", "message": "Start a new batch with /process_all."}), 400
    except Exception as e:
        logger.error(f"Error resuming batch processing: {e}")
        return jsonify({"error": "Failed to resume batch processing", "details": str(e)}), 500


@app.route('/push_to_salesforce', methods=['GET'])
def push_to_salesforce_route():
    try:
        json_path = _get_safe_session_json_path()
        if not json_path:
            flash("No processed JSON file available to push.", "danger")
            return redirect(url_for('index'))
        if 'last_processed' not in session:
            flash("No file selected for pushing to Salesforce.", "danger")
            return redirect(url_for('index'))

        file_key    = session['last_processed']
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        if not (isinstance(data.get('opportunity'), dict) or isinstance(data.get('lead'), dict)):
            json_data = get_json_from_cos(COS_BUCKET, file_key)
            data = enrich_response_with_lead_opportunity(data, json_data)

        push_status = push_to_salesforce(data, file_key)
        messages = {
            'pushed':          ("Successfully pushed data to Salesforce.", "success"),
            'short_duration':  ("This call is 15 seconds or shorter; not pushed.", "info"),
            'missing_audio_url':("No audio URL available; not pushed.", "info"),
            'already_pushed':  ("Already pushed to Salesforce. Duplicate skipped.", "info"),
            'push_in_progress':("Push already in progress. Duplicate skipped.", "warning"),
            'out_of_range':    ("File is outside the allowed date window; not pushed.", "warning"),
            'apex_limit_error':("Salesforce error: Apex hit a 'Too many query rows' limit "
                                 "(CreateCallTranscriptFromJsonAPI). This is a Salesforce-side "
                                 "issue unrelated to this file's data — please contact your "
                                 "Salesforce admin to fix the Apex class.", "danger"),
        }
        msg, category = messages.get(push_status, ("Failed to push to Salesforce. Check logs.", "danger"))
        flash(msg, category)
        return redirect(url_for('index'))
    except Exception as e:
        logger.error(f"Error pushing JSON to Salesforce: {e}")
        flash(f"Error pushing to Salesforce: {str(e)}", "danger")
        return redirect(url_for('index'))


@app.route('/status', methods=['GET'])
def status():
    try:
        processed_files, failed_files, pushed_files, pushed_monitor_ucids, current_batch_start, total_files = load_batch_state(log_loaded=False)
        push_stats_snapshot, recent_pushes = _get_recent_pushes_for_ui()
        completion_message = ""
        if BATCH_PROCESSING_COMPLETED and total_files > 0 and current_batch_start >= total_files:
            completion_message = f"✓ All batch processing completed! {len(pushed_files)} files pushed."
        elif BATCH_PUSH_LIMIT_REACHED:
            completion_message = f"Batch stopped automatically after {BATCH_PUSH_LIMIT} successful pushes."
        return jsonify({
            "status":               "success",
            "processed":            len(processed_files),
            "pushed":               len(pushed_files),
            "failed":               len(failed_files),
            "total":                total_files,
            "current_batch_start":  current_batch_start,
            "is_paused":            PAUSE_PROCESSING,
            "is_canceling":         CANCEL_PROCESSING,
            "state_file_exists":    os.path.exists(STATE_FILE),
            "batch_completed":      BATCH_PROCESSING_COMPLETED and total_files > 0 and current_batch_start >= total_files,
            "push_limit_reached":   BATCH_PUSH_LIMIT_REACHED,
            "batch_push_limit":     BATCH_PUSH_LIMIT,
            "completion_message":   completion_message,
            "push_statistics": {
                "total_push_attempts": push_stats_snapshot['total_push_attempts'],
                "successful_pushes":   push_stats_snapshot['successful_pushes'],
                "failed_pushes":       push_stats_snapshot['failed_pushes'],
                "last_push_time":      push_stats_snapshot['last_push_time']
            },
            "recent_pushes": recent_pushes
        }), 200
    except Exception as e:
        logger.error(f"Error retrieving batch status: {e}")
        return jsonify({"status": "error", "message": "Failed to retrieve batch status", "details": str(e)}), 500


@app.route('/debug/push_stats', methods=['GET'])
def debug_push_stats():
    try:
        stats = _snapshot_push_stats()
        success_rate = (stats['successful_pushes'] / stats['total_push_attempts'] * 100) if stats['total_push_attempts'] > 0 else 0
        return jsonify({
            'status': 'success',
            'push_statistics': {
                'total_push_attempts':        stats['total_push_attempts'],
                'successful_pushes':          stats['successful_pushes'],
                'failed_pushes':              stats['failed_pushes'],
                'total_transcriptions_pushed': stats['total_transcriptions_pushed'],
                'transcription_errors':       stats['transcription_errors'],
                'success_rate_percent':       round(success_rate, 2),
                'start_time':                 stats['start_time'],
                'last_push_time':             stats['last_push_time']
            },
            'recent_pushes': stats['recent_pushes'][-10:] if stats['recent_pushes'] else []
        }), 200
    except Exception as e:
        logger.error(f"Error retrieving push statistics: {e}")
        return jsonify({"status": "error", "message": "Failed to retrieve push statistics", "details": str(e)}), 500


@app.route('/debug/push_stats/reset', methods=['GET'])
def reset_push_stats():
    global push_stats
    try:
        with push_stats_lock:
            push_stats = {
                'total_push_attempts': 0, 'successful_pushes': 0,
                'failed_pushes': 0, 'total_transcriptions_pushed': 0,
                'transcription_errors': 0, 'start_time': datetime.now().isoformat(),
                'last_push_time': None, 'recent_pushes': []
            }
        return jsonify({'status': 'success', 'message': 'Push statistics have been reset'}), 200
    except Exception as e:
        logger.error(f"Error resetting push statistics: {e}")
        return jsonify({"status": "error", "message": "Failed to reset push statistics", "details": str(e)}), 500


@app.route('/debug/push_stats/export', methods=['GET'])
def export_push_stats():
    try:
        timestamp   = datetime.now().strftime('%Y%m%d_%H%M%S')
        stats       = _snapshot_push_stats()
        export_data = {
            'export_timestamp': timestamp,
            'statistics': stats,
            'summary': {
                'total_attempts':    stats['total_push_attempts'],
                'success_count':     stats['successful_pushes'],
                'failure_count':     stats['failed_pushes'],
                'success_rate':      round((stats['successful_pushes'] / max(stats['total_push_attempts'], 1)) * 100, 2),
                'transcriptions_count': stats['total_transcriptions_pushed']
            }
        }
        filename = f'push_stats_export_{timestamp}.json'
        filepath = os.path.join(ensure_temp_dir(), filename)
        _atomic_write_json(filepath, export_data)
        logger.info(f"Push statistics exported to {filename}")
        return send_file(filepath, as_attachment=True, download_name=filename)
    except Exception as e:
        logger.error(f"Error exporting push statistics: {e}")
        return jsonify({"status": "error", "message": "Failed to export push statistics", "details": str(e)}), 500


@app.route('/debug/presto_lookup', methods=['GET'])
def debug_presto_lookup():
    """
    Debug endpoint to test Presto lookup for a given ID.
    Usage: /debug/presto_lookup?id=006J3000005pBrg&type=opportunity
    """
    try:
        id_value    = request.args.get('id', '').strip()
        report_type = request.args.get('type', 'opportunity').strip()

        if not id_value:
            return jsonify({"error": "id parameter is required"}), 400

        if not _presto_is_configured():
            return jsonify({"error": "Presto is not configured"}), 500

        report = PRESTO_REPORT_CONFIG.get(report_type)
        if not report:
            return jsonify({"error": f"Unknown report type: {report_type}"}), 400

        full_table = f"{report['catalog']}.{report['schema']}.{report['table']}"
        id_field   = report['id_field']

        # ── Test 1: Exact match (current behaviour) ──────────────
        query_exact = f"SELECT * FROM {full_table} WHERE {id_field} = '{_escape_presto_string(id_value)}' LIMIT 5"

        # ── Test 2: LIKE match (catches 18-char stored values) ────
        query_like  = f"SELECT * FROM {full_table} WHERE {id_field} LIKE '{_escape_presto_string(id_value)}%' LIMIT 5"

        # ── Test 3: Case-insensitive exact match ──────────────────
        query_lower = f"SELECT * FROM {full_table} WHERE LOWER({id_field}) = LOWER('{_escape_presto_string(id_value)}') LIMIT 5"

        # ── Test 4: Sample rows so we can see what IDs look like ──
        query_sample = f"SELECT {id_field} FROM {full_table} LIMIT 10"

        results = {}

        try:
            cols, rows = _presto_query(query_exact, catalog=report['catalog'], schema=report['schema'])
            results['exact_match'] = {
                'query':     query_exact,
                'row_count': len(rows),
                'rows':      [dict(zip(cols, r)) for r in rows]
            }
        except Exception as e:
            results['exact_match'] = {'error': str(e)}

        try:
            cols, rows = _presto_query(query_like, catalog=report['catalog'], schema=report['schema'])
            results['like_match'] = {
                'query':     query_like,
                'row_count': len(rows),
                'rows':      [dict(zip(cols, r)) for r in rows]
            }
        except Exception as e:
            results['like_match'] = {'error': str(e)}

        try:
            cols, rows = _presto_query(query_lower, catalog=report['catalog'], schema=report['schema'])
            results['case_insensitive_match'] = {
                'query':     query_lower,
                'row_count': len(rows),
                'rows':      [dict(zip(cols, r)) for r in rows]
            }
        except Exception as e:
            results['case_insensitive_match'] = {'error': str(e)}

        try:
            cols, rows = _presto_query(query_sample, catalog=report['catalog'], schema=report['schema'])
            results['sample_ids'] = {
                'query':      query_sample,
                'row_count':  len(rows),
                'sample_ids': [r[0] for r in rows if r]
            }
        except Exception as e:
            results['sample_ids'] = {'error': str(e)}

        return jsonify({
            "id_queried":  id_value,
            "id_length":   len(id_value),
            "report_type": report_type,
            "id_field":    id_field,
            "table":       full_table,
            "results":     results
        }), 200

    except Exception as e:
        logger.error(f"Debug presto lookup failed: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500

# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == '__main__':
    try:
        port = 8002
        logger.info(f"Starting Flask app on port {port}")
        app.run(debug=False, host='0.0.0.0', port=port)
    except Exception as e:
        logger.error(f"Failed to start Flask app: {e}")
        raise
