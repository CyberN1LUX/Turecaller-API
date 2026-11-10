"""HITEK / ICMR Data Gateway.

FastAPI + DuckDB over the public HuggingFace bucket:
    CutehackX/icrm-hitek-full-db-mixed-bucket

The bucket contains prebuilt Parquet indexes sorted by phoneNumber and
aadharNumber.  The app works in two modes:

remote (default when no local files are present)
    Streams the sorted index parts straight from HuggingFace.  No 227 GB
    download is required, but the first lookup that touches a new row group
    is network-bound (roughly 30-90 seconds).  Results are cached.

local
    If idx_phone*.parquet / idx_aadhar*.parquet / part*.parquet exist in
    ICMR_DATA_DIR (or next to this file), DuckDB queries those files instead.

Endpoints
---------
GET  /                       landing page + search box
GET  /FetchData?Number=...   DB lookup + Truecaller, one merged JSON
GET  /search?q=...           unified search (phone / aadhar / text)
POST /search/parallel        batch search
GET  /callapp?Number=...     CallApp search (needs captured request spec)
GET  /health                 status
GET  /docs                   Swagger UI
"""

from __future__ import annotations

import glob
import gzip
import json
import os
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import duckdb
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response
from pydantic import BaseModel
from starlette.exceptions import HTTPException as StarletteHTTPException

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BASE = os.path.dirname(os.path.abspath(__file__))
_DATA_DIR = os.environ.get("ICMR_DATA_DIR") or (
    os.path.join(BASE, "data") if os.path.isdir(os.path.join(BASE, "data")) else BASE
)

BUCKET = os.environ.get("ICMR_BUCKET", "CutehackX/icrm-hitek-full-db-mixed-bucket")
BUCKET_BASE = f"https://huggingface.co/buckets/{BUCKET}/resolve"

PARALLELISM = int(os.environ.get("ICMR_PARALLEL", "8"))
THREADS_PER_CONN = int(os.environ.get("ICMR_THREADS_PER_CONN", "8"))
DUPLICATE_CAP = 2  # max 2 copies of the same person in results
CACHE_TTL = int(os.environ.get("ICMR_CACHE_TTL", "600"))  # seconds
# DuckDB's httpfs `http_timeout` is in SECONDS (not milliseconds).  Values
# above ~2,147,483 overflow its internal millisecond conversion, which makes
# every request fail immediately with "Timeout was reached error for HTTP
# HEAD".  Keep it in a sane range (1 hour by default).
HTTP_TIMEOUT_S = int(os.environ.get("ICMR_HTTP_TIMEOUT_S", "3600"))
HTTP_TIMEOUT_S = max(1, min(HTTP_TIMEOUT_S, 2147483))
HTTP_RETRIES = int(os.environ.get("ICMR_HTTP_RETRIES", "3"))
ALLOW_REMOTE_TEXT_SCAN = os.environ.get("ICMR_ALLOW_REMOTE_TEXT_SCAN", "0") == "1"
ALLOW_REMOTE_OTHER_SCAN = os.environ.get("ICMR_ALLOW_REMOTE_OTHER_SCAN", "0") == "1"

# ---------------------------------------------------------------------------
# Truecaller (caller-id search) - token embedded here on purpose
# ---------------------------------------------------------------------------

TRUECALLER_TOKEN = os.environ.get("TRUECALLER_TOKEN") or (
    "a2i0t-00hOQFmkjVZGi6EMnIRnYIQUQmQORbwoqUXrfMNB1rCTQZnFN6e8Pb_zn5"
)
TRUECALLER_UA = "Truecaller/26.43.5 (Android;10)"
TRUECALLER_CLIENT_SECRET = "lvc22mp3l1sfv6ujg83rd17btt"
TRUECALLER_SEARCH_URL = "https://search5-noneu.truecaller.com/v2/search"
TRUECALLER_BULK_URL = "https://search5-noneu.truecaller.com/v2/bulk"
TRUECALLER_ACCOUNT = "https://account-asia-south1.truecaller.com"
TRUECALLER_TIMEOUT = int(os.environ.get("TRUECALLER_TIMEOUT", "25"))
TRUECALLER_ENABLED = os.environ.get("TRUECALLER_ENABLED", "1") == "1"

# ---------------------------------------------------------------------------
# CallApp (needs the real search request from a capture - see /callapp)
# ---------------------------------------------------------------------------

CALLAPP_SEARCH_URL = os.environ.get("CALLAPP_SEARCH_URL", "")
CALLAPP_HEADERS = os.environ.get("CALLAPP_HEADERS", "")
CALLAPP_TIMEOUT = int(os.environ.get("CALLAPP_TIMEOUT", "25"))

# iso2 -> (dialing code, typical national length)
COUNTRY_DIAL: dict[str, tuple[str, int]] = {
    "IN": ("91", 10), "US": ("1", 10), "CA": ("1", 10), "GB": ("44", 10),
    "AE": ("971", 9), "SA": ("966", 9), "QA": ("974", 8), "KW": ("965", 8),
    "OM": ("968", 8), "BH": ("973", 8), "PK": ("92", 10), "BD": ("880", 10),
    "NP": ("977", 10), "LK": ("94", 9), "MY": ("60", 9), "SG": ("65", 8),
    "ID": ("62", 10), "PH": ("63", 10), "TH": ("66", 9), "VN": ("84", 9),
    "CN": ("86", 11), "JP": ("81", 10), "KR": ("82", 10), "HK": ("852", 8),
    "TW": ("886", 9), "AU": ("61", 9), "NZ": ("64", 9), "DE": ("49", 11),
    "FR": ("33", 9), "IT": ("39", 10), "ES": ("34", 9), "PT": ("351", 9),
    "NL": ("31", 9), "BE": ("32", 9), "CH": ("41", 9), "AT": ("43", 10),
    "SE": ("46", 9), "NO": ("47", 8), "DK": ("45", 8), "FI": ("358", 10),
    "PL": ("48", 9), "CZ": ("420", 9), "RO": ("40", 9), "HU": ("36", 9),
    "GR": ("30", 10), "TR": ("90", 10), "RU": ("7", 10), "UA": ("380", 9),
    "IL": ("972", 9), "EG": ("20", 10), "NG": ("234", 10), "KE": ("254", 9),
    "ZA": ("27", 9), "MA": ("212", 9), "BR": ("55", 11), "MX": ("52", 10),
    "AR": ("54", 10), "CL": ("56", 9), "CO": ("57", 10), "PE": ("51", 9),
    "VE": ("58", 10), "IR": ("98", 10), "IQ": ("964", 10), "AF": ("93", 9),
    "MM": ("95", 10), "KH": ("855", 9), "LA": ("856", 10), "MN": ("976", 8),
    "KZ": ("7", 10), "UZ": ("998", 9), "AZ": ("994", 9), "GE": ("995", 9),
    "AM": ("374", 8), "BY": ("375", 9), "MD": ("373", 8), "BG": ("359", 9),
    "RS": ("381", 9), "HR": ("385", 9), "SI": ("386", 8), "SK": ("421", 9),
    "LT": ("370", 8), "LV": ("371", 8), "EE": ("372", 8), "IE": ("353", 9),
    "IS": ("354", 7), "LU": ("352", 9), "MT": ("356", 8), "CY": ("357", 8),
    "JO": ("962", 9), "LB": ("961", 8), "SY": ("963", 9), "YE": ("967", 9),
    "TZ": ("255", 9), "UG": ("256", 9), "GH": ("233", 9), "ET": ("251", 9),
}


SEARCH_FIELDS = [
    "name", "fathersName", "phoneNumber", "aadharNumber", "otherNumber",
    "address", "district", "pincode", "state", "town", "source",
]
NUMBER_FIELDS = ["phoneNumber", "aadharNumber", "otherNumber"]
TEXT_FIELDS = [f for f in SEARCH_FIELDS if f not in NUMBER_FIELDS]

PHONE_PARTS = 7
AADHAR_PARTS = 7

# min/max phoneNumber per sorted part, read from Parquet row-group statistics.
# None means "unbounded on that side".
_PHONE_RANGES: list[tuple[int | None, int | None]] = [
    (None, 7310463827),
    (7310463827, 8077394733),
    (8077394734, 8780538380),
    (8780538381, 9313336069),
    (9313336069, 9703053944),
    (9703053945, 9990701360),
    (9990701361, None),
]

# aadharNumber ranges for the clean parts (0-3).  Parts 4-6 contain some
# malformed values in the published index, so values above the part-3 max are
# routed to parts 4-6 directly.
_AADHAR_RANGES: list[tuple[int | None, int | None]] = [
    (None, 351104219878),
    (351104219878, 549490723501),
    (549490723501, 746293615015),
    (746293615015, 944410175347),
]
_AADHAR_TAIL_START = 944410175348  # values >= this go to parts 4,5,6


def _bucket_url(name: str) -> str:
    return f"{BUCKET_BASE}/{name}?download=true"


_REMOTE_PHONE = [_bucket_url(f"idx_phone.{i}.parquet") for i in range(PHONE_PARTS)]
_REMOTE_AADHAR = [_bucket_url(f"idx_aadhar.{i}.parquet") for i in range(AADHAR_PARTS)]


# ---------------------------------------------------------------------------
# Local / remote discovery
# ---------------------------------------------------------------------------

def _local_parts(prefix: str) -> list[str]:
    """Return local index parts for `prefix` (e.g. idx_phone.0.parquet)."""
    single = os.path.join(_DATA_DIR, f"{prefix}.parquet")
    parts = sorted(glob.glob(os.path.join(_DATA_DIR, f"{prefix}.*.parquet")))
    parts = [p for p in parts if os.path.isfile(p) and os.path.getsize(p) > 0]
    if parts:
        return parts
    if os.path.isfile(single) and os.path.getsize(single) > 0:
        return [single]
    return []


def _local_raw() -> list[str]:
    names = ["part1.parquet", "part2a.parquet", "part2b_new.parquet"]
    out = []
    for n in names:
        p = os.path.join(_DATA_DIR, n)
        if os.path.isfile(p) and os.path.getsize(p) > 0:
            out.append(p)
    return out


_LOCAL_PHONE = _local_parts("idx_phone")
_LOCAL_AADHAR = _local_parts("idx_aadhar")
_LOCAL_RAW = _local_raw()

# A full-width local scan source (raw parquet if present, otherwise the phone
# index, which still has all 11 columns).
_LOCAL_SCAN_FILES = _LOCAL_RAW or _LOCAL_PHONE
MODE = "local" if (_LOCAL_PHONE or _LOCAL_AADHAR or _LOCAL_RAW) else "remote"

# ---------------------------------------------------------------------------
# DuckDB connection pool (one connection per worker thread)
# ---------------------------------------------------------------------------

_thread_local = threading.local()
_conns: list[duckdb.DuckDBPyConnection] = []
_conns_lock = threading.Lock()


def _new_conn() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()  # in-memory catalog
    con.execute(f"SET threads = {THREADS_PER_CONN}")
    con.execute("SET enable_progress_bar = false")
    if MODE == "remote":
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute(f"SET http_timeout = {HTTP_TIMEOUT_S}")
        con.execute(f"SET http_retries = {HTTP_RETRIES}")
    if _LOCAL_SCAN_FILES:
        files = ", ".join(_sql_literal(p) for p in _LOCAL_SCAN_FILES)
        con.execute(f"CREATE VIEW people AS SELECT * FROM read_parquet([{files}])")
    return con


def _get_conn() -> duckdb.DuckDBPyConnection:
    ident = getattr(_thread_local, "id", None)
    if ident is None:
        with _conns_lock:
            ident = len(_conns)
            _thread_local.id = ident
    with _conns_lock:
        while len(_conns) <= ident:
            _conns.append(_new_conn())
    return _conns[ident]


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------

def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _parquet_expr(paths: list[str]) -> str:
    if not paths:
        raise ValueError("No Parquet files are available")
    return "read_parquet([" + ", ".join(_sql_literal(p) for p in paths) + "])"


def _source_value(source: str | None) -> str | None:
    if source == "hitek":
        return "inddata"
    if source in ("icmr", "inddata"):
        return source
    return None


def _source_clause(source: str | None) -> str:
    src = _source_value(source)
    if src:
        return f" AND source = {_sql_literal(src)}"
    return ""


def _run(sql: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
    con = _get_conn()
    cur = con.execute(sql, params or [])
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def _person_key(row: dict[str, Any]) -> tuple[str, ...]:
    ph = (row.get("phoneNumber") or "").strip()
    ad = (row.get("aadharNumber") or "").strip()
    if ph or ad:
        return (ph, ad)
    return (row.get("name") or "").strip(), (row.get("fathersName") or "").strip()


def _cap_duplicates(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Keep at most DUPLICATE_CAP rows per person (same phone+aadhar identity)."""
    seen: dict[tuple[str, ...], int] = {}
    out: list[dict[str, Any]] = []
    for r in rows:
        k = _person_key(r)
        n = seen.get(k, 0)
        if n < DUPLICATE_CAP:
            seen[k] = n + 1
            out.append(r)
    return out


def _pretty(data: dict[str, Any], pretty: bool) -> Response:
    body = json.dumps(data, indent=2 if pretty else None, ensure_ascii=False, default=str)
    return Response(content=body, media_type="application/json")


# ---------------------------------------------------------------------------
# HTTP client + Truecaller helpers
# ---------------------------------------------------------------------------

def _http_json(url: str, method: str = "GET", headers: dict[str, str] | None = None,
               payload: Any = None, timeout: int = 25) -> tuple[int, Any]:
    """Tiny stdlib JSON client. Returns (status, parsed_body)."""
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("User-Agent", TRUECALLER_UA)
    req.add_header("Accept-Encoding", "gzip")
    for key, value in (headers or {}).items():
        req.add_header(key, value)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            encoding = (resp.headers.get("Content-Encoding") or "").lower()
            status = resp.status
    except urllib.error.HTTPError as exc:
        raw = exc.read()
        encoding = (exc.headers.get("Content-Encoding") or "").lower()
        status = exc.code
    if "gzip" in encoding:
        try:
            raw = gzip.decompress(raw)
        except Exception:
            pass
    text = raw.decode("utf-8", "replace")
    try:
        body = json.loads(text) if text else None
    except Exception:
        body = text
    return status, body


def normalize_phone(raw: str, iso2: str = "IN") -> dict[str, Any]:
    """Normalize phone input for Truecaller (accepts 0/+91/0091 prefixes)."""
    code = (iso2 or "IN").upper()
    country = COUNTRY_DIAL.get(code)
    digits = re.sub(r"\D", "", str(raw or ""))
    if not digits:
        raise ValueError("Invalid phone number: no digits found")
    national = re.sub(r"^0+", "", digits) or digits
    if country:
        dial, length = country
        if national.startswith(dial) and len(national) == len(dial) + length:
            national = national[len(dial):]
    return {
        "countryCode": code,
        "dialingCode": int(country[0]) if country else None,
        "national": national,
        "e164": "+" + (country[0] if country else "") + national,
    }


def truecaller_search(number: str, country_code: str = "IN",
                      search_type: int = 4) -> dict[str, Any]:
    """Call the Truecaller app-flow search endpoint."""
    if not TRUECALLER_ENABLED:
        return {"ok": False, "status": 0, "error": "Truecaller disabled (TRUECALLER_ENABLED=0)"}
    try:
        pn = normalize_phone(number, country_code)
    except ValueError as exc:
        return {"ok": False, "status": 400, "error": str(exc)}

    query = urllib.parse.urlencode({
        "q": pn["national"],
        "countryCode": pn["countryCode"],
        "type": str(search_type),
        "locAddr": "",
        "placement": "SEARCHRESULTS,HISTORY,DETAILS",
        "encoding": "json",
    })
    try:
        status, body = _http_json(
            f"{TRUECALLER_SEARCH_URL}?{query}",
            headers={"Authorization": f"Bearer {TRUECALLER_TOKEN}",
                     "Accept": "application/json"},
            timeout=TRUECALLER_TIMEOUT,
        )
    except Exception as exc:
        return {"ok": False, "status": 0, "error": f"Truecaller request failed: {exc}"}

    if status in (401, 403):
        return {
            "ok": False,
            "status": status,
            "error": "Truecaller token invalid/expired. Update TRUECALLER_TOKEN in main.py "
                     "(a fresh OTP login is required).",
            "data": body,
        }
    if status != 200:
        return {"ok": False, "status": status,
                "error": "Truecaller search failed", "data": body}
    return {"ok": True, "status": status, "data": body}


def truecaller_bulk(numbers: list[str], country_code: str = "IN") -> dict[str, Any]:
    """Bulk lookup (max 10 numbers per request)."""
    if not TRUECALLER_ENABLED:
        return {"ok": False, "status": 0, "error": "Truecaller disabled (TRUECALLER_ENABLED=0)"}
    clean = [n.strip() for n in numbers if n and n.strip()]
    if not clean:
        return {"ok": False, "status": 400, "error": "No numbers supplied"}
    if len(clean) > 10:
        return {"ok": False, "status": 400, "error": "Max 10 numbers per request"}
    try:
        normalized = [normalize_phone(n, country_code) for n in clean]
    except ValueError as exc:
        return {"ok": False, "status": 400, "error": str(exc)}

    query = urllib.parse.urlencode({
        "q": ",".join(p["e164"] for p in normalized),
        "countryCode": normalized[0]["countryCode"],
        "type": "14",
        "placement": "SEARCHRESULTS,HISTORY,DETAILS",
        "encoding": "json",
    })
    try:
        status, body = _http_json(
            f"{TRUECALLER_BULK_URL}?{query}",
            headers={"Authorization": f"Bearer {TRUECALLER_TOKEN}",
                     "Accept": "application/json"},
            timeout=TRUECALLER_TIMEOUT,
        )
    except Exception as exc:
        return {"ok": False, "status": 0, "error": f"Truecaller request failed: {exc}"}
    if status in (401, 403):
        return {"ok": False, "status": status,
                "error": "Truecaller token invalid/expired. Update TRUECALLER_TOKEN in main.py.",
                "data": body}
    if status != 200:
        return {"ok": False, "status": status,
                "error": "Truecaller bulk search failed", "data": body}
    return {"ok": True, "status": status, "data": body}


# ---------------------------------------------------------------------------
# CallApp helper
# ---------------------------------------------------------------------------

def callapp_search(number: str, country_code: str = "IN") -> dict[str, Any]:
    """CallApp caller lookup.

    CallApp signs its search requests inside a native library, so the request
    format cannot be guessed.  Set CALLAPP_SEARCH_URL (a URL template using
    {number} and {countryCode}) and CALLAPP_HEADERS (a JSON object) to enable
    it once the real request is captured.
    """
    if not CALLAPP_SEARCH_URL:
        return {
            "ok": False,
            "status": 501,
            "error": "CallApp search API is not configured. The supplied HAR only "
                     "contains ad-SDK traffic, not the CallApp lookup request. Capture "
                     "one search in HTTP Toolkit, then set CALLAPP_SEARCH_URL / "
                     "CALLAPP_HEADERS (or paste the request into main.py).",
        }
    try:
        pn = normalize_phone(number, country_code)
    except ValueError as exc:
        return {"ok": False, "status": 400, "error": str(exc)}
    try:
        headers = json.loads(CALLAPP_HEADERS) if CALLAPP_HEADERS else {}
    except Exception:
        headers = {}
    headers.setdefault("Accept", "application/json")
    url = (CALLAPP_SEARCH_URL
           .replace("{number}", urllib.parse.quote(pn["national"]))
           .replace("{e164}", urllib.parse.quote(pn["e164"]))
           .replace("{countryCode}", urllib.parse.quote(pn["countryCode"])))
    try:
        status, body = _http_json(url, headers=headers, timeout=CALLAPP_TIMEOUT)
    except Exception as exc:
        return {"ok": False, "status": 0, "error": f"CallApp request failed: {exc}"}
    return {"ok": status == 200, "status": status, "data": body}



# ---------------------------------------------------------------------------
# Result cache
# ---------------------------------------------------------------------------

_cache: dict[tuple[Any, ...], tuple[float, dict[str, Any]]] = {}
_cache_lock = threading.Lock()


def _cache_get(key: tuple[Any, ...]) -> dict[str, Any] | None:
    if CACHE_TTL <= 0:
        return None
    now = time.time()
    with _cache_lock:
        item = _cache.get(key)
        if item and now - item[0] < CACHE_TTL:
            return item[1]
        if item:
            _cache.pop(key, None)
    return None


def _cache_set(key: tuple[Any, ...], value: dict[str, Any]) -> None:
    if CACHE_TTL <= 0:
        return
    with _cache_lock:
        # keep the cache bounded
        if len(_cache) > 512:
            oldest = sorted(_cache.items(), key=lambda kv: kv[1][0])[:128]
            for k, _ in oldest:
                _cache.pop(k, None)
        _cache[key] = (time.time(), value)


# ---------------------------------------------------------------------------
# Index routing
# ---------------------------------------------------------------------------

def _phone_candidates(value: str) -> list[int]:
    if not value.isdigit():
        return list(range(PHONE_PARTS))
    n = int(value)
    out = []
    for i, (lo, hi) in enumerate(_PHONE_RANGES):
        if (lo is None or n >= lo) and (hi is None or n <= hi):
            out.append(i)
    return out or list(range(PHONE_PARTS))


def _aadhar_candidates(value: str) -> list[int]:
    if not value.isdigit():
        return list(range(AADHAR_PARTS))
    n = int(value)
    if n >= _AADHAR_TAIL_START:
        return [4, 5, 6]
    for i, (lo, hi) in enumerate(_AADHAR_RANGES):
        if (lo is None or n >= lo) and n <= hi:
            return [i]
    return [0, 1, 2, 3]


def _index_paths(kind: str, value: str) -> list[str]:
    if kind == "phone":
        if _LOCAL_PHONE:
            return _LOCAL_PHONE
        return [_REMOTE_PHONE[i] for i in _phone_candidates(value)]
    if kind == "aadhar":
        if _LOCAL_AADHAR:
            return _LOCAL_AADHAR
        return [_REMOTE_AADHAR[i] for i in _aadhar_candidates(value)]
    raise ValueError(f"Unknown index: {kind}")


def _exact(field: str, value: str, paths: list[str], limit: int,
           source: str | None = None) -> list[dict[str, Any]]:
    # Values are inlined (not bound as ? parameters) so DuckDB can push the
    # predicate into the Parquet scan and use row-group zone maps.  With a
    # prepared-statement parameter DuckDB scans every row group.
    sql = (f"SELECT * FROM {_parquet_expr(paths)} "
           f"WHERE {field} = {_sql_literal(value)}")
    sql += _source_clause(source)
    sql += f" LIMIT {int(limit)}"
    return _run(sql)


def _query_index(kind: str, value: str, limit: int,
                 source: str | None = None) -> dict[str, Any]:
    """Exact lookup on the phone/aadhar sorted index."""
    field = "phoneNumber" if kind == "phone" else "aadharNumber"
    paths = _index_paths(kind, value)
    fetch_limit = limit * DUPLICATE_CAP + 20
    try:
        rows = _exact(field, value, paths, fetch_limit, source)
    except Exception as exc:
        # Only retry the remaining parts when the selected part is actually
        # missing.  Do not retry on timeouts: that would multiply the wait.
        msg = str(exc).lower()
        missing_part = any(s in msg for s in ("404", "not found", "no files"))
        all_remote = _REMOTE_PHONE if kind == "phone" else _REMOTE_AADHAR
        if MODE == "remote" and missing_part and len(paths) < len(all_remote):
            rows = _exact(field, value, all_remote, fetch_limit, source)
            paths = all_remote
        else:
            raise
    capped = _cap_duplicates(rows)[:limit]
    return {
        "results": capped,
        "raw_count": len(rows),
        "parts_queried": len(paths),
        "index": f"idx_{kind}",
    }


def _query_other(value: str, limit: int,
                 source: str | None = None) -> dict[str, Any]:
    """otherNumber has no prebuilt index.  Only scan locally (or opt-in remote)."""
    if _LOCAL_SCAN_FILES:
        sql = (f"SELECT * FROM {_parquet_expr(_LOCAL_SCAN_FILES)} "
               f"WHERE otherNumber = {_sql_literal(value)}")
        sql += _source_clause(source)
        sql += f" LIMIT {int(limit * DUPLICATE_CAP + 20)}"
        rows = _run(sql)
        return {
            "results": _cap_duplicates(rows)[:limit],
            "raw_count": len(rows),
            "parts_queried": len(_LOCAL_SCAN_FILES),
            "index": "local-scan",
        }
    if ALLOW_REMOTE_OTHER_SCAN:
        rows = _exact("otherNumber", value, _REMOTE_PHONE,
                      limit * DUPLICATE_CAP + 20, source)
        return {
            "results": _cap_duplicates(rows)[:limit],
            "raw_count": len(rows),
            "parts_queried": len(_REMOTE_PHONE),
            "index": "remote-scan",
        }
    return {"results": [], "raw_count": 0, "parts_queried": 0,
            "index": None, "skipped": "otherNumber has no index in remote mode"}


def _query_text(value: str, limit: int, source: str | None = None,
                field: str | None = None, mode: str = "contains") -> dict[str, Any]:
    """Text search over local files.  Remote full scans are not viable."""
    if not _LOCAL_SCAN_FILES:
        raise HTTPException(
            503,
            "Text search needs local data. Run setup.bat/setup.sh (or point "
            "ICMR_DATA_DIR at the downloaded index parts). Phone/aadhar lookups "
            "work in remote mode.",
        )
    target = field or "name"
    if target not in SEARCH_FIELDS:
        raise HTTPException(400, f"Unknown field: {target}")
    if mode == "exact":
        sql = (f"SELECT * FROM {_parquet_expr(_LOCAL_SCAN_FILES)} "
               f"WHERE {target} = {_sql_literal(value)}")
    else:
        escaped = value.replace("\\", "\\\\").replace("%", r"\%").replace("_", r"\_")
        sql = (f"SELECT * FROM {_parquet_expr(_LOCAL_SCAN_FILES)} "
               f"WHERE {target} ILIKE {_sql_literal('%' + escaped + '%')} ESCAPE '\\'")
    sql += _source_clause(source)
    sql += f" LIMIT {int(limit * DUPLICATE_CAP + 20)}"
    rows = _run(sql)
    return {
        "results": _cap_duplicates(rows)[:limit],
        "raw_count": len(rows),
        "parts_queried": len(_LOCAL_SCAN_FILES),
        "index": "local-scan",
    }


# ---------------------------------------------------------------------------
# Unified search
# ---------------------------------------------------------------------------

def _phone_variants(value: str) -> list[str]:
    variants = [value]
    if len(value) == 12 and value.startswith("91"):
        variants.append(value[2:])
    if len(value) == 11 and value.startswith("0"):
        variants.append(value[1:])
    return list(dict.fromkeys(variants))


def _search_one(q: str, field: str | None = None, mode: str = "contains",
                limit: int = 10, source: str | None = None) -> dict[str, Any]:
    q = (q or "").strip()
    if not q:
        raise HTTPException(400, "q must not be empty")

    if field:
        if field not in SEARCH_FIELDS:
            raise HTTPException(400, f"Unknown field: {field}")
        if mode == "exact" and field in ("phoneNumber", "aadharNumber"):
            kind = "phone" if field == "phoneNumber" else "aadhar"
            data = _query_index(kind, q, limit, source)
        elif mode == "exact" and field == "otherNumber":
            data = _query_other(q, limit, source)
        else:
            data = _query_text(q, limit, source, field=field, mode=mode)
        return {
            "query": q, "field": field, "mode": mode, "dedup": DUPLICATE_CAP,
            "count": len(data["results"]), "results": data["results"],
            "raw_count": data.get("raw_count", len(data["results"])),
        }

    # Numeric input -> indexed exact lookups.  Accept +91, spaces, dashes,
    # dots and parentheses around a phone number.
    digits = re.sub(r"\D", "", q)
    numeric_like = bool(re.fullmatch(r"[\d\s+\-().]+", q))
    if digits and len(digits) >= 8 and numeric_like:
        searched: list[str] = []
        phone_data = {"results": [], "raw_count": 0}
        for v in _phone_variants(digits):
            phone_data = _query_index("phone", v, limit, source)
            if phone_data["results"]:
                break
        if phone_data["results"]:
            searched.append("phoneNumber")

        aadhar_data = {"results": [], "raw_count": 0}
        if len(digits) >= 12:
            # Do not strip a country-code prefix from a 12-digit Aadhar.
            aadhar_data = _query_index("aadhar", digits, limit, source)
            if aadhar_data["results"]:
                searched.append("aadharNumber")

        other_data = {"results": [], "raw_count": 0}
        if not phone_data["results"]:
            other_data = _query_other(digits, limit, source)
            if other_data["results"]:
                searched.append("otherNumber")

        all_rows = _cap_duplicates(
            phone_data["results"] + aadhar_data["results"] + other_data["results"]
        )[:limit]
        return {
            "query": q, "field": None, "mode": "exact", "dedup": DUPLICATE_CAP,
            "count": len(all_rows), "results": all_rows,
            "searched_fields": searched or ["phoneNumber"],
            "raw_count": phone_data["raw_count"] + aadhar_data["raw_count"]
                         + other_data["raw_count"],
        }

    # Text input.
    data = _query_text(q, limit, source, field="name", mode="contains")
    rows = data["results"]
    if not rows and _LOCAL_SCAN_FILES:
        father = _query_text(q, limit, source, field="fathersName", mode="contains")
        rows = _cap_duplicates(rows + father["results"])[:limit]
    return {
        "query": q, "field": None, "mode": "contains", "dedup": DUPLICATE_CAP,
        "count": len(rows), "results": rows,
        "searched_fields": ["name", "fathersName"],
        "raw_count": data["raw_count"],
    }


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

app = FastAPI(
    title="HITEK + ICMR Data Gateway",
    description=(
        "DuckDB-backed lookup over the ICMR + HITEK full DB (2.5B rows, 11 columns). "
        "Phone and Aadhar lookups use the prebuilt sorted indexes. "
        "Duplicates are capped at 2 per person."
    ),
)


@app.exception_handler(StarletteHTTPException)
async def custom_http_exception_handler(request: Request, exc: StarletteHTTPException):
    if exc.status_code == 404:
        return JSONResponse(
            status_code=404,
            content={
                "status": "rejected",
                "message": "Invalid endpoint. Use /FetchData?Number=XXXXXXXXXX or /search?q=...",
                "Developer": "cybern1lux",
            },
        )
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.detail, "Developer": "cybern1lux"},
    )


LANDING_PAGE_HTML = r"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>HITEK Data Gateway - LIVE</title>
    <style>
        body { margin: 0; overflow: hidden; background-color: #050505; color: #00ffcc; font-family: 'Courier New', Courier, monospace; }
        #canvas-container { position: fixed; inset: 0; z-index: -1; }
        .overlay {
            position: absolute; top: 50%; left: 50%; transform: translate(-50%, -50%);
            width: min(900px, 92vw); max-height: 90vh; overflow: auto;
            text-align: center; background: rgba(10, 10, 10, 0.9); padding: 32px;
            border: 1px solid #00ffcc; border-radius: 12px;
            box-shadow: 0 0 30px rgba(0, 255, 204, 0.3); backdrop-filter: blur(5px);
        }
        h1 { margin: 0 0 10px 0; font-size: 2.4em; text-transform: uppercase; letter-spacing: 4px; text-shadow: 0 0 15px #00ffcc; }
        p { font-size: 1em; margin: 6px 0; color: #ccc; }
        .highlight { color: #00ffcc; font-weight: bold; }
        .status-box {
            margin: 16px 0; font-weight: bold; padding: 10px; border-radius: 8px;
            background: rgba(0, 255, 204, 0.1); border: 1px solid rgba(0, 255, 204, 0.5);
            font-size: 0.95em;
        }
        .blinking { animation: blinker 1.5s linear infinite; display: inline-block; }
        @keyframes blinker { 50% { opacity: 0; } }
        form { display: flex; gap: 8px; justify-content: center; margin: 18px 0 8px; flex-wrap: wrap; }
        input {
            flex: 1 1 260px; min-width: 220px; padding: 12px 14px; border-radius: 8px;
            border: 1px solid #00ffcc; background: #07110f; color: #00ffcc;
            font: inherit; font-size: 1.05em; outline: none;
        }
        button {
            padding: 12px 22px; border-radius: 8px; border: 1px solid #00ffcc;
            background: #00ffcc; color: #04110e; font: inherit; font-weight: bold; cursor: pointer;
        }
        button:disabled { opacity: 0.5; cursor: wait; }
        pre {
            text-align: left; background: #07110f; border: 1px solid #123; border-radius: 8px;
            padding: 12px; max-height: 42vh; overflow: auto; color: #bfffe9;
            font-size: 0.82em; white-space: pre-wrap; word-break: break-word;
        }
        .hint { color: #888; font-size: 0.82em; }
        a { color: #00ffcc; }
    </style>
</head>
<body>
    <div id="canvas-container"></div>
    <div class="overlay">
        <h1>SYSTEM ONLINE</h1>
        <p>API Gateway is <span class="highlight">Active &amp; Secured</span></p>
        <p>Source: <span class="highlight" id="mode">checking...</span></p>
        <div class="status-box">
            <span class="blinking" style="color: #00ffcc;">●</span> HTTP 200 OK - LISTENING FOR QUERIES
        </div>
        <form id="search-form">
            <input id="number" name="number" inputmode="numeric" autocomplete="off"
                   placeholder="Enter phone number (e.g. 9732226783)" />
            <button id="go" type="submit">FETCH</button>
        </form>
        <p class="hint">
            Response = DB records first (<code>Data</code>), then the Truecaller
            result (<code>Truecaller</code>) — one merged JSON.<br>
            Remote mode streams the sorted HuggingFace index. The first lookup that
            touches a new row group can take 30-90s; repeated queries are cached.
        </p>
        <pre id="out">Ready. Try 9732226783</pre>
    </div>

    <script src="https://cdnjs.cloudflare.com/ajax/libs/three.js/r128/three.min.js"></script>
    <script>
        try {
            const scene = new THREE.Scene();
            const camera = new THREE.PerspectiveCamera(75, innerWidth / innerHeight, 0.1, 2000);
            const renderer = new THREE.WebGLRenderer({ antialias: true, alpha: true });
            renderer.setSize(innerWidth, innerHeight);
            document.getElementById('canvas-container').appendChild(renderer.domElement);

            const geometry = new THREE.BufferGeometry();
            const vertices = [];
            for (let i = 0; i < 8000; i++) {
                vertices.push(THREE.MathUtils.randFloatSpread(3000));
                vertices.push(THREE.MathUtils.randFloatSpread(3000));
                vertices.push(THREE.MathUtils.randFloatSpread(3000));
            }
            geometry.setAttribute('position', new THREE.Float32BufferAttribute(vertices, 3));
            const material = new THREE.PointsMaterial({ color: 0x00ffcc, size: 2.5, transparent: true, opacity: 0.8 });
            const points = new THREE.Points(geometry, material);
            scene.add(points);
            camera.position.z = 1200;

            (function animate() {
                requestAnimationFrame(animate);
                points.rotation.x += 0.0005;
                points.rotation.y += 0.001;
                renderer.render(scene, camera);
            })();

            addEventListener('resize', () => {
                camera.aspect = innerWidth / innerHeight;
                camera.updateProjectionMatrix();
                renderer.setSize(innerWidth, innerHeight);
            });
        } catch (e) {
            console.warn('three.js unavailable', e);
        }

        fetch('/health').then(r => r.json()).then(h => {
            document.getElementById('mode').textContent =
                (h.mode || 'unknown').toUpperCase() + (h.bucket ? ' · ' + h.bucket : '');
        }).catch(() => {});

        const form = document.getElementById('search-form');
        const out = document.getElementById('out');
        const go = document.getElementById('go');

        form.addEventListener('submit', async (ev) => {
            ev.preventDefault();
            const n = document.getElementById('number').value.trim();
            if (!n) return;
            go.disabled = true;
            out.textContent = 'Searching… the first remote lookup can take 30-90 seconds.';
            const t0 = performance.now();
            try {
                const r = await fetch('/FetchData?Number=' + encodeURIComponent(n));
                const j = await r.json();
                out.textContent = 'HTTP ' + r.status + ' · ' +
                    Math.round(performance.now() - t0) + ' ms\n\n' +
                    JSON.stringify(j, null, 2);
            } catch (e) {
                out.textContent = 'Request failed: ' + e;
            } finally {
                go.disabled = false;
            }
        });
    </script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
def root_landing_page():
    return HTMLResponse(content=LANDING_PAGE_HTML, status_code=200)


@app.get("/health")
def health():
    return {
        "status": "ok",
        "mode": MODE,
        "bucket": BUCKET if MODE == "remote" else None,
        "data_dir": _DATA_DIR,
        "indexes": {
            "phone": bool(_LOCAL_PHONE) if MODE == "local" else True,
            "aadhar": bool(_LOCAL_AADHAR) if MODE == "local" else True,
            "name": bool(_LOCAL_SCAN_FILES),
            "other": bool(_LOCAL_SCAN_FILES) or ALLOW_REMOTE_OTHER_SCAN,
        },
        "local_parts": {
            "phone": len(_LOCAL_PHONE),
            "aadhar": len(_LOCAL_AADHAR),
            "raw": len(_LOCAL_RAW),
        },
        "dedup_cap": DUPLICATE_CAP,
        "cache_ttl_s": CACHE_TTL,
        "cache_entries": len(_cache),
        "columns": SEARCH_FIELDS,
        "truecaller": {
            "enabled": TRUECALLER_ENABLED,
            "token_set": bool(TRUECALLER_TOKEN),
            "search_url": TRUECALLER_SEARCH_URL,
        },
        "callapp": {
            "configured": bool(CALLAPP_SEARCH_URL),
        },
    }


@app.get("/FetchData")
def fetch_data(
    Number: str = Query(None),
    truecaller: bool = Query(True, description="Also fetch the Truecaller caller id"),
    countryCode: str = Query("IN", description="ISO2 country code (IN, US, ...)"),
):
    """Exact phone lookup, one merged JSON response.

    Order is exactly: DB records first (`Data`), then the Truecaller result
    (`Truecaller`).  There is no separate endpoint/section.
    """
    raw = (Number or "").strip()
    digits = re.sub(r"\D", "", raw)
    if not digits or len(digits) < 10 or len(digits) > 15:
        return JSONResponse(
            status_code=400,
            content={
                "status": "rejected",
                "message": "Invalid parameter. STRICTLY use /FetchData?Number=XXXXXXXXXX",
                "Developer": "cybern1lux",
            },
        )

    iso2 = (countryCode or "IN").upper()
    cache_key = ("fetch", digits, bool(truecaller), iso2)
    cached = _cache_get(cache_key)
    if cached is not None:
        payload = dict(cached)
        meta = dict(payload.get("meta", {}))
        meta["cached"] = True
        payload["meta"] = meta
        return payload

    t0 = time.time()
    try:
        main_data: dict[str, Any] = {"results": [], "raw_count": 0}
        alt_data: dict[str, Any] = {"results": [], "raw_count": 0}
        aadhar_data: dict[str, Any] = {"results": [], "raw_count": 0}
        db_error: str | None = None

        try:
            for v in _phone_variants(digits):
                main_data = _query_index("phone", v, 20, None)
                if main_data["results"]:
                    break

            alt_data = _query_other(digits, 20, None)

            if not main_data["results"] and not alt_data["results"] and len(digits) >= 12:
                for v in _phone_variants(digits):
                    aadhar_data = _query_index("aadhar", v, 20, None)
                    if aadhar_data["results"]:
                        break
        except HTTPException:
            raise
        except Exception as exc:
            db_error = str(exc)

        tc_result = None
        if truecaller:
            tc_result = truecaller_search(digits, iso2)

        has_db = bool(
            main_data["results"] or alt_data["results"] or aadhar_data["results"]
        )
        has_tc = bool(tc_result and tc_result.get("ok"))

        if not has_db and not has_tc:
            body: dict[str, Any] = {
                "status": "not_found",
                "phone": digits,
                "Data": {"Main_Records": [], "Alt_Records": []},
                "Truecaller": tc_result,
                "Developer": "cybern1lux",
            }
            if db_error:
                body["status"] = "error"
                body["message"] = f"Database processing error: {db_error}"
            return JSONResponse(status_code=404 if not db_error else 500, content=body)

        data: dict[str, Any] = {
            "Main_Records": main_data["results"],
            "Alt_Records": alt_data["results"],
        }
        if aadhar_data["results"]:
            data["Aadhar_Records"] = aadhar_data["results"]

        payload = {
            "status": "success",
            "phone": digits,
            "Data": data,
            "Truecaller": tc_result,
            "meta": {
                "mode": MODE,
                "index": main_data.get("index") or aadhar_data.get("index") or alt_data.get("index"),
                "parts_queried": main_data.get("parts_queried", 0),
                "elapsed_ms": round((time.time() - t0) * 1000, 1),
                "raw_matches": {
                    "phoneNumber": main_data["raw_count"],
                    "otherNumber": alt_data["raw_count"],
                    "aadharNumber": aadhar_data["raw_count"],
                },
                "dedup_cap": DUPLICATE_CAP,
                "cached": False,
                "alt_lookup": alt_data.get("index") or alt_data.get("skipped"),
                "truecaller": {
                    "enabled": bool(truecaller),
                    "ok": bool(tc_result and tc_result.get("ok")),
                    "status": (tc_result or {}).get("status"),
                    "error": (tc_result or {}).get("error"),
                },
            },
            "Developer": "cybern1lux",
        }
        _cache_set(cache_key, payload)
        return payload

    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "message": f"Database processing error: {str(e)}",
                "Developer": "cybern1lux",
            },
        )


@app.get("/callapp")
def callapp(
    Number: str = Query(None),
    countryCode: str = Query("IN", description="ISO2 country code (IN, US, ...)"),
):
    """CallApp caller lookup.

    The supplied capture only contains ad-SDK traffic, so this stays disabled
    until CALLAPP_SEARCH_URL / CALLAPP_HEADERS are set with the real request.
    """
    digits = re.sub(r"\D", "", (Number or "").strip())
    if not digits or len(digits) < 10 or len(digits) > 15:
        return JSONResponse(
            status_code=400,
            content={
                "status": "rejected",
                "message": "STRICTLY use /callapp?Number=XXXXXXXXXX",
                "Developer": "cybern1lux",
            },
        )
    result = callapp_search(digits, (countryCode or "IN").upper())
    return JSONResponse(
        status_code=200 if result.get("ok") else 501,
        content={
            "status": "success" if result.get("ok") else "unavailable",
            "phone": digits,
            "CallApp": result,
            "Developer": "cybern1lux",
        },
    )


@app.get("/search")
def search(
    q: str = Query(..., description="Search term - phone, aadhar, name, address, etc."),
    field: str | None = Query(None, description=f"Restrict to one field: {SEARCH_FIELDS}"),
    mode: str = Query("contains", pattern="^(exact|contains)$"),
    limit: int = Query(10, ge=1, le=1000),
    source: str | None = Query(None, pattern="^(icmr|hitek|inddata)$",
                               description="Filter by source: icmr, hitek (=inddata), inddata"),
    pretty: bool = Query(True, description="Pretty-print JSON"),
):
    """Unified search across all 11 columns. Duplicates capped at 2 per person."""
    data = _search_one(q, field=field, mode=mode, limit=limit, source=source)
    return _pretty(data, pretty)


class BatchRequest(BaseModel):
    queries: list[dict[str, Any]]
    limit: int = 10


@app.post("/search/parallel")
def search_parallel(req: BatchRequest):
    """Run up to 50 searches in parallel (bounded by ICMR_PARALLEL)."""
    if not req.queries:
        raise HTTPException(400, "queries must not be empty")
    if len(req.queries) > 50:
        raise HTTPException(400, "max 50 queries per batch")

    jobs = [
        (item.get("value", ""), item.get("field"), item.get("mode", "contains"),
         int(item.get("limit", req.limit)), item.get("source"))
        for item in req.queries
    ]
    workers = max(1, min(PARALLELISM, len(jobs)))
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="batch") as pool:
        futures = [pool.submit(_search_one, *job) for job in jobs]
        results = []
        for f in futures:
            try:
                results.append(f.result())
            except Exception as e:
                results.append({"error": str(e)})
    return _pretty({"searches": len(jobs), "parallelism": workers, "results": results}, True)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="127.0.0.1", port=17712, reload=False)
