from flask import Flask, render_template, request, redirect, url_for, session, send_file, jsonify, has_request_context, flash
import requests
import json
import ibm_boto3
from ibm_botocore.client import Config
from ibm_botocore.config import Config as BotocoreConfig
import urllib.parse
import os
from datetime import datetime
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
        'id', 'opportunity_id_c',
        'activity_id_c', 'WhoId', 'WhatId',
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
    global batch_thread, PAUSE_PROCESSING, CANCEL_PROCESSING, BATCH_PROCESSING_COMPLETED
    with batch_state_lock:
        if batch_thread and batch_thread.is_alive():
            return False
        PAUSE_PROCESSING = False
        CANCEL_PROCESSING = False
        BATCH_PROCESSING_COMPLETED = False
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


def is_root_bucket_json_key(key):
    normalized_key = str(key or '').strip()
    return (
        bool(normalized_key)
        and normalized_key.endswith('.json')
        and '/' not in normalized_key
        and '\\' not in normalized_key
    )


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
        fetched_json_files = []
        continuation_token = None

        while True:
            list_kwargs = {'Bucket': COS_BUCKET, 'Delimiter': '/'}
            if continuation_token:
                list_kwargs['ContinuationToken'] = continuation_token

            response = cos_client.list_objects_v2(**list_kwargs)
            for item in response.get('Contents', []) or []:
                key = item.get('Key', '')
                if is_root_bucket_json_key(key):
                    fetched_json_files.append(key)

            if response.get('IsTruncated'):
                continuation_token = response.get('NextContinuationToken')
                if not continuation_token:
                    break
            else:
                break

        if not fetched_json_files:
            logger.warning("No root-level JSON objects found in COS bucket")
            return []

        json_files = fetched_json_files
        json_files_cached_at = time.time()
        logger.info(f"Found {len(json_files)} root-level JSON files in COS bucket")
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
    return data.get('monitor_ucid') or data.get('monitorUCID') or data.get('MonitorUCID') or 'Unknown'


def get_uui(json_data):
    """Extract UUI from COS JSON payload — trimmed to 15 chars if 18-char ID."""
    data = json_data or {}
    uui = (data.get('UUI') or
           data.get('uui') or
           data.get('Uui') or
           data.get('Id') or
           data.get('id') or
           data.get('SalesforceId') or
           data.get('salesforceId') or
           'Unknown')
    if isinstance(uui, str):
        uui = uui.strip()
        # Trim 18-character Salesforce ID to 15 characters (remove last 3 chars)
        if len(uui) == 18:
            uui = uui[:15]
        return uui
    return 'Unknown'


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

    reasons = []
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


# ============================================================
# TRANSCRIPT CHUNKING
# ============================================================

def validate_enriched_payload(data, file_key):
    """Check if payload has required enrichment fields before pushing.
    Works with NORMALIZED Salesforce field names."""
    required_fields = ['Transcript__c', 'Separated_Transcript__c', 'Call_Insight__c', 'Sentiment__c', 'call_Rating__c']
    logger.debug(f"[VALIDATION CHECK] {file_key} payload keys: {list(data.keys()) if data else 'None'}")

    missing = [f for f in required_fields if not data.get(f)]

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


# ============================================================
# SALESFORCE PAYLOAD HELPERS
# ============================================================

def normalize_salesforce_payload(data):
    payload = dict(data or {})
    normalized = {}

    # ── Exact list of fields you want ────────────────────────────

    # 1. monitorUcid__c
    if "monitorUCID" in payload:
        normalized["monitorUcid__c"] = payload["monitorUCID"]

    # 2. Separated_Transcript__c and Transcript__c
    transcript_value = payload.get("call_transcription")
    if transcript_value:
        if isinstance(transcript_value, str):
            normalized["Transcript__c"] = transcript_value
            normalized["Separated_Transcript__c"] = chunk_transcript(transcript_value)
        elif isinstance(transcript_value, dict):
            raw = transcript_value.get("raw_transcript") or transcript_value.get("rawTranscript")
            sep = (
                transcript_value.get("separated_transcript")
                or transcript_value.get("separatedTranscript")
                or raw
            )
            normalized["Transcript__c"] = raw or sep or "No transcription available"
            normalized["Separated_Transcript__c"] = chunk_transcript(sep or raw) if (sep or raw) else ["No transcription available"]

    # 3. Call_Insight__c
    if "call_insight" in payload:
        insight = payload["call_insight"]
        if isinstance(insight, dict) and "summary" in insight:
            normalized["Call_Insight__c"] = insight["summary"]
        elif isinstance(insight, str):
            normalized["Call_Insight__c"] = insight

    # 4. Sentiment__c
    if "call_sentiment_analysis" in payload:
        sentiment = payload["call_sentiment_analysis"]
        if isinstance(sentiment, dict) and "sentiment" in sentiment:
            normalized["Sentiment__c"] = sentiment["sentiment"]
        elif isinstance(sentiment, str):
            normalized["Sentiment__c"] = sentiment

    # 5. call_Rating__c
    rating_value = payload.get("call_rating")
    if rating_value:
        if isinstance(rating_value, str):
            normalized["call_Rating__c"] = rating_value
        elif isinstance(rating_value, dict):
            rating_parts = [f"Call Rating: {rating_value.get('rating', 'Unknown')}"]
            reasons = ensure_list(rating_value.get("reasons", []), [])
            if reasons:
                rating_parts.extend([f"Reason: {r}" for r in reasons])
            normalized["call_Rating__c"] = "\n".join(rating_parts)

    # 6. Caller_Tone_Emotion__c (from customer_details)
    if "customer_details" in payload and isinstance(payload["customer_details"], dict):
        if payload["customer_details"].get("caller_tone"):
            normalized["Caller_Tone_Emotion__c"] = payload["customer_details"]["caller_tone"]

    # 7. Opportunity__c
    if "opportunity" in payload and isinstance(payload["opportunity"], dict):
        if payload["opportunity"].get("tagged_id"):
            normalized["Opportunity__c"] = payload["opportunity"]["tagged_id"]

    # 8. Lead__c
    if "lead" in payload and isinstance(payload["lead"], dict):
        if payload["lead"].get("tagged_id"):
            normalized["Lead__c"] = payload["lead"]["tagged_id"]

    return normalized


def should_skip_salesforce_push(data):
    payload = data or {}
    transcript_block = payload.get("call_transcription") or {}
    transcript_text = ""
    if isinstance(transcript_block, dict):
        transcript_text = (
            transcript_block.get("separated_transcript")
            or transcript_block.get("separatedTranscript")
            or transcript_block.get("raw_transcript")
            or transcript_block.get("rawTranscript")
            or ""
        )
    elif isinstance(transcript_block, str):
        transcript_text = transcript_block
    return str(transcript_text).strip() == SHORT_CALL_TRANSCRIPT


# ============================================================
# JSON OUTPUT BUILDER
# ============================================================

def create_json_output(file_key, transcript, insights, callquality, separated, customer_details, call_to_action=None):
    logger.info(f"Processing insights: {insights!r}")
    logger.debug(f"[CREATE_JSON] Input check - transcript:{bool(transcript)} | insights:{bool(insights)} | quality:{bool(callquality)}")
    try:
        insight_summary, sentiment = insights
    except (TypeError, ValueError) as e:
        logger.error(f"Error unpacking insights: {e}. Insights: {insights!r}")
        insight_summary = "Error: Could not extract summary"
        sentiment = "Error: Could not extract sentiment"

    json_data     = get_json_from_cos(COS_BUCKET, file_key)
    monitor_ucid  = get_monitor_ucid(json_data)
    uui           = get_uui(json_data)
    transcript_value  = transcript if transcript else "No transcription available"
    separated_source  = separated if separated else transcript_value
    separated_value   = format_transcript_for_storage(separated_source)
    rating_data       = parse_call_quality(callquality)
    action_items      = clean_call_to_action_items(call_to_action)
    request_points    = build_request_points(callquality, action_items)

    logger.debug(f"[CREATE_JSON] Transcript condition - transcript_value:{transcript_value[:50] if isinstance(transcript_value, str) else type(transcript_value)}")

    output_data = {
        "monitorUCID": monitor_ucid,
        "UUI": uui,
        "customer_details": customer_details or {},
        "call_transcription": {
            "separated_transcript": (
                separated_value
                if transcript not in ["No audio URL is available", "Call is less than 15 sec"]
                else format_transcript_for_storage(transcript_value)
            )
        }
    }

    if transcript not in ["No audio URL is available", "Call is less than 15 sec"]:
        logger.info(f"[CREATE_JSON] {file_key} - Adding full enrichment (transcript length: {len(transcript_value) if isinstance(transcript_value, str) else 0})")
        output_data.update({
            "call_insight":            {"summary": insight_summary},
            "call_to_action":          request_points,
            "call_rating":             {"rating": rating_data["rating"], "reasons": rating_data["reasons"]},
            "call_sentiment_analysis": {"sentiment": sentiment}
        })
    else:
        logger.warning(f"[CREATE_JSON] {file_key} - SHORT CALL DETECTED: {transcript} - Using forced defaults only")
        output_data.update({
            "call_to_action":          FORCED_CALL_TO_ACTION_ITEMS.copy(),
            "call_rating":             {"rating": FORCED_CALL_RATING, "reasons": ["Call could not be analyzed."]},
            "call_sentiment_analysis": {"sentiment": "Unknown"}
        })

    try:
        # ── First: Enrich with Lead / Opportunity from Presto ──────────────────────
        logger.info(f"[CREATE_JSON] COS JSON keys for enrichment: {list((json_data or {}).keys())}")
        output_data = enrich_response_with_lead_opportunity(output_data, json_data)
        logger.info(f"[CREATE_JSON] Output keys after enrichment: {list(output_data.keys())}")
        # ── Then: Normalize to Salesforce field names ─────────────────────────────
        output_data = normalize_salesforce_payload(output_data)
        # ────────────────────────────────────────────────────────────────────────────

        json_str    = json.dumps(output_data, indent=2, ensure_ascii=False)
        temp_path   = get_output_json_path(file_key)
        filename    = os.path.basename(temp_path)
        _atomic_write_json(temp_path, output_data)

        if has_request_context():
            session['json_filename']    = filename
            session['json_path']        = temp_path
            # Don't store large data in session cookie—keep it small!
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
            "decoding_method": "greedy", "max_new_tokens": 200,
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


def push_to_salesforce(data, file_key):
    _check_batch_pause(file_key, "Salesforce push preparation")

    if not validate_enriched_payload(data, file_key):
        logger.error(f"[PUSH ABORT] {file_key} - Payload validation failed. Not pushing incomplete data.")
        return 'validation_failed'

    logger.warning(f"[PUSH DEBUG] {file_key} input data keys: {list((data or {}).keys())}")
    if data:
        for key in ['call_transcription', 'call_insight', 'call_sentiment_analysis', 'call_rating']:
            logger.debug(f"  - {key}: {bool(data.get(key))}")

    if should_skip_salesforce_push(data):
        logger.info(f"[PUSH SKIP] {file_key} is a short call; not pushing")
        return 'short_duration'

    if not is_file_key_in_current_window(file_key):
        logger.warning(f"[PUSH SKIP] {file_key} is outside the allowed date window")
        return 'out_of_range'

    monitor_ucid    = get_monitor_ucid(data)
    safety_tag      = f"monitorUCID:{monitor_ucid}" if monitor_ucid != 'Unknown' else 'monitorUCID:unknown'
    reservation     = reserve_push_attempt(file_key, monitor_ucid=monitor_ucid)

    if reservation == 'already_pushed':
        logger.info(f"[PUSH SKIP] {file_key} already pushed")
        return 'already_pushed'
    if reservation == 'push_in_progress':
        logger.info(f"[PUSH SKIP] {file_key} push already in progress")
        return 'push_in_progress'

    push_start = time.time()
    with push_stats_lock:
        push_stats['total_push_attempts'] += 1
        push_stats['last_push_time']       = datetime.now().isoformat()

    try:
        transcription_data  = data.get('Transcript__c', {}) or data.get('call_transcription', {})
        separated_transcript = transcription_data.get('separated_transcript', '') if isinstance(transcription_data, dict) else (transcription_data if isinstance(transcription_data, str) else '')
        transcript_length    = len(separated_transcript) if separated_transcript else 0

        if 'Transcript__c' in data or 'call_transcription' in data:
            with push_stats_lock:
                push_stats['total_transcriptions_pushed'] += 1
            logger.info(f"[PUSH] Pushing transcription for {file_key} — {transcript_length} chars")

        _check_batch_pause(file_key, "Salesforce access token request")
        t0           = time.time()
        token        = get_salesforce_access_token()
        access_dur   = time.time() - t0
        headers      = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

        # Check if data is already normalized (has Salesforce field names)
        is_already_normalized = 'Transcript__c' in data or 'Separated_Transcript__c' in data
        normalized_data = data if is_already_normalized else normalize_salesforce_payload(data)

        logger.debug(f"Raw data keys for {file_key}: {list(data.keys()) if data else 'None'}")
        logger.debug(f"[NORMALIZATION] Already normalized: {is_already_normalized}")
        logger.info(f"Salesforce payload for {file_key}: {json.dumps(normalized_data, ensure_ascii=False)[:1000]}")

        enforce_request_gap('salesforce_push', SALESFORCE_PUSH_INTERVAL_SECONDS)
        _check_batch_pause(file_key, "Salesforce API request")
        t1           = time.time()
        response     = requests.post(SALESFORCE_API_URL, headers=headers, json=normalized_data, timeout=(30, 300))
        request_dur  = time.time() - t1
        total_dur    = time.time() - push_start

        if response.status_code in (200, 201, 204):
            with push_stats_lock:
                push_stats['successful_pushes'] += 1
                current_pushed = push_stats['successful_pushes']
            logger.info(f"[PUSH SUCCESS] {file_key} | Total={current_pushed} | access={access_dur:.2f}s req={request_dur:.2f}s total={total_dur:.2f}s")
            _record_push_event('success', file_key=file_key, monitor_ucid=monitor_ucid, transcript_length=transcript_length,
                               status_code=response.status_code, access_time=round(access_dur, 2),
                               request_time=round(request_dur, 2), total_time=round(total_dur, 2))
            release_push_attempt(file_key, success=True, monitor_ucid=monitor_ucid)
            persist_pushed_file(file_key, monitor_ucid=monitor_ucid)
            return 'pushed'
        else:
            with push_stats_lock:
                push_stats['failed_pushes'] += 1
                push_stats['transcription_errors'] += 1
            logger.error(f"[PUSH FAILED] {file_key} | Status={response.status_code} | total={total_dur:.2f}s")
            _record_push_event('failed', file_key=file_key, monitor_ucid=monitor_ucid, transcript_length=transcript_length,
                               status_code=response.status_code, error=(response.text or '')[:200],
                               access_time=round(access_dur, 2), request_time=round(request_dur, 2), total_time=round(total_dur, 2))
            release_push_attempt(file_key, success=False, monitor_ucid=monitor_ucid)
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
        logger.warning(f"[DATA CHECKPOINT] {file_key} | transcript:{bool(output_data.get('call_transcription'))} | insight:{bool(output_data.get('call_insight'))} | sentiment:{bool(output_data.get('call_sentiment_analysis'))} | rating:{bool(output_data.get('call_rating'))}")
        if not any([output_data.get(k) for k in ['call_transcription', 'call_insight', 'call_sentiment_analysis', 'call_rating']]):
            logger.error(f"[DATA EMPTY] {file_key} - All enrichment fields missing! Raw json_str keys: {list(json.loads(json_str).keys()) if json_str else 'None'}")
        _check_batch_pause(file_key, "Salesforce push")
        push_status = push_to_salesforce(output_data, file_key)

        if push_status == 'pushed':
            logger.info(f"[PUSHED] {file_key}")
            return file_key, 'pushed', []
        elif push_status == 'short_duration':
            return file_key, 'short_duration', []
        elif push_status == 'already_pushed':
            return file_key, 'already_pushed', []
        elif push_status == 'push_in_progress':
            return file_key, 'push_in_progress', []
        elif push_status == 'out_of_range':
            return file_key, 'out_of_range', ["File is outside the allowed date window"]
        elif push_status == 'validation_failed':
            return file_key, 'validation_failed', ["Payload missing required enrichment fields"]
        else:
            return file_key, 'failed', ["Failed to push to Salesforce"]

    except BatchPauseRequested as e:
        logger.info(str(e))
        return file_key, 'paused', [str(e)]
    except Exception as e:
        logger.error(f"Error processing {file_key}: {e}")
        return file_key, 'failed', [str(e)]


def process_and_push_all_jsons(max_workers=4, batch_size=100):
    global PAUSE_PROCESSING, CANCEL_PROCESSING, BATCH_PROCESSING_COMPLETED, batch_thread
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

        for batch_start in range(current_batch_start, total_files, batch_size):
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

                while queued and len(pending) < max_workers:
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
                            elif push_status == 'short_duration':
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

                    if not CANCEL_PROCESSING and not PAUSE_PROCESSING and queued:
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

        if not audio_url or not urllib.parse.urlparse(audio_url).scheme:
            response = {
                "monitorUCID": monitor_ucid,
                "UUI": uui,
                "customer_details": {},
                "call_transcription": {"separated_transcript": "No audio URL is available"},
                "call_to_action": FORCED_CALL_TO_ACTION_ITEMS.copy(),
                "call_rating": {"rating": "Unknown", "reasons": ["Call could not be analyzed."]}
            }
            response = enrich_response_with_lead_opportunity(response, json_data)
            response = normalize_salesforce_payload(response)
            logger.warning(f"[PROCESS & PUSH] {file_key} - No audio URL")
            return jsonify({
                "status": "short_duration",
                "message": "No audio URL available",
                "data": response
            }), 200

        duration_seconds = parse_call_duration(json_data.get('CallDuration', '00:00:00'))
        if duration_seconds <= 15:
            response = {
                "monitorUCID": monitor_ucid,
                "UUI": uui,
                "customer_details": {},
                "call_transcription": {"separated_transcript": "Call is less than 15 sec"},
                "call_to_action": FORCED_CALL_TO_ACTION_ITEMS.copy(),
                "call_rating": {"rating": "Unknown", "reasons": ["Call could not be analyzed."]}
            }
            response = enrich_response_with_lead_opportunity(response, json_data)
            response = normalize_salesforce_payload(response)
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

        insight_summary, sentiment = insights
        rating_data    = parse_call_quality(callquality)
        action_items   = clean_call_to_action_items(call_to_action)
        request_points = build_request_points(callquality, action_items)

        response = {
            "monitorUCID": monitor_ucid,
            "UUI": uui,
            "customer_details": customer_details or {},
            "call_transcription": {
                "separated_transcript": format_transcript_for_storage(
                    separated if separated else (transcript if transcript else "No transcription available")
                )
            },
            "call_insight":            {"summary": insight_summary},
            "call_to_action":          request_points,
            "call_rating":             {"rating": rating_data["rating"], "reasons": rating_data["reasons"]},
            "call_sentiment_analysis": {"sentiment": sentiment}
        }

        response = enrich_response_with_lead_opportunity(response, json_data)
        response = normalize_salesforce_payload(response)

        logger.info(f"[PROCESS & PUSH] Processing complete for {file_key}, proceeding to push")

        # ── Step 2: Push to Salesforce ──
        push_status = push_to_salesforce(response, file_key)

        status_messages = {
            'pushed':          {"status": "success", "message": "Successfully processed and pushed to Salesforce"},
            'validation_failed': {"status": "validation_failed", "message": "Payload validation failed - missing enrichment fields"},
            'short_duration':  {"status": "short_duration", "message": "Call too short; not pushed"},
            'already_pushed':  {"status": "already_pushed", "message": "Already pushed to Salesforce"},
            'push_in_progress': {"status": "push_in_progress", "message": "Push already in progress"},
            'out_of_range':    {"status": "out_of_range", "message": "File outside date window"},
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

        if not audio_url or not urllib.parse.urlparse(audio_url).scheme:
            response = {
                "monitorUCID": monitor_ucid,
                "UUI": uui,
                "customer_details": {},
                "call_transcription": {"separated_transcript": "No audio URL is available"},
                "call_to_action": FORCED_CALL_TO_ACTION_ITEMS.copy(),
                "call_rating": {"rating": "Unknown", "reasons": ["Call could not be analyzed."]}
            }
            # ── First: Enrich with Lead / Opportunity from Presto ──
            response = enrich_response_with_lead_opportunity(response, json_data)
            # ── Then: Normalize to Salesforce field names ──────────
            response = normalize_salesforce_payload(response)
            return jsonify(response), 200

        duration_seconds = parse_call_duration(json_data.get('CallDuration', '00:00:00'))
        if duration_seconds <= 15:
            response = {
                "monitorUCID": monitor_ucid,
                "UUI": uui,
                "customer_details": {},
                "call_transcription": {"separated_transcript": "Call is less than 15 sec"},
                "call_to_action": FORCED_CALL_TO_ACTION_ITEMS.copy(),
                "call_rating": {"rating": "Unknown", "reasons": ["Call could not be analyzed."]}
            }
            # ── First: Enrich with Lead / Opportunity from Presto ──
            response = enrich_response_with_lead_opportunity(response, json_data)
            # ── Then: Normalize to Salesforce field names ──────────
            response = normalize_salesforce_payload(response)
            return jsonify(response), 200

        errors, separated, insights, callquality, transcript, customer_details, call_to_action = process_audio_from_cos(file_key)
        if errors:
            return jsonify({"error": errors[0], "status": "failed"}), 400

        insight_summary, sentiment = insights
        rating_data    = parse_call_quality(callquality)
        action_items   = clean_call_to_action_items(call_to_action)
        request_points = build_request_points(callquality, action_items)

        response = {
            "monitorUCID": monitor_ucid,
            "UUI": uui,
            "customer_details": customer_details or {},
            "call_transcription": {
                "separated_transcript": format_transcript_for_storage(
                    separated if separated else (transcript if transcript else "No transcription available")
                )
            },
            "call_insight":            {"summary": insight_summary},
            "call_to_action":          request_points,
            "call_rating":             {"rating": rating_data["rating"], "reasons": rating_data["reasons"]},
            "call_sentiment_analysis": {"sentiment": sentiment}
        }

        # ── First: Enrich with Lead / Opportunity from Presto ──
        response = enrich_response_with_lead_opportunity(response, json_data)
        # ── Then: Normalize to Salesforce field names ──────────
        response = normalize_salesforce_payload(response)

        logger.info(f"[API_PROCESS] Final response keys: {list(response.keys())}")
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

        push_status = push_to_salesforce(data, file_key)
        messages = {
            'pushed':          ("Successfully pushed data to Salesforce.", "success"),
            'short_duration':  ("This call is 15 seconds or shorter; not pushed.", "info"),
            'already_pushed':  ("Already pushed to Salesforce. Duplicate skipped.", "info"),
            'push_in_progress':("Push already in progress. Duplicate skipped.", "warning"),
            'out_of_range':    ("File is outside the allowed date window; not pushed.", "warning"),
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