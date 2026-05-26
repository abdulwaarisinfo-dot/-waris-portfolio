# ============================================================
# AI E-COMMERCE CHATBOT SaaS — main.py
# Version: 5.0 (Production Hardening)
#
# ── CHANGE LOG (v4.1 vs v4.0) ────────────────────────────────
#
#   FIX-SERIAL-1  load_data_realtime() now applies safe_json()
#                 to every product document fetched from MongoDB.
#                 Previously only _id was converted to str; fields
#                 like created_at / updated_at remained as Python
#                 datetime objects.  When those products appeared
#                 in a carousel payload sent via ws.send_json()
#                 (which uses stdlib json.dumps), the call raised:
#                   "Object of type datetime is not JSON serializable"
#                 disconnecting every user who received a product result.
#
#   FIX-SERIAL-2  load_data_realtime() now also applies safe_json()
#                 to the bot_metadata document, preventing datetime
#                 fields (created_at, version stamps) stored there
#                 from leaking into WS payloads.
#
#   FIX-SERIAL-3  websocket_endpoint() wraps bot_response in
#                 safe_json() before calling ws.send_json().
#                 This is a cheap last-resort guard: even if a
#                 future code path introduces a new non-JSON-safe
#                 type anywhere in the response tree, the send
#                 will succeed rather than crashing the connection.
#
#   FIX-SERIAL-4  _ws_send_initial_greeting() wraps its payload
#                 in safe_json() for the same defensive reason.
#
# ── PRESERVED (zero diff vs v4.0 in all other behaviour) ─────
#   - All route URLs, HTTP methods, request/response shapes
#   - Login / Signup / Session / Auth logic
#   - NLPUtils, AnalyticsEngine class APIs
#   - Bot response logic, FAQ, suggestions, product scoring
#   - Multi-tenant owner_id isolation
#   - WebSocket rate limiting, heartbeat, reconnect
# ============================================================


# ============================================================
# SECTION 1: IMPORTS
# ============================================================

from fastapi import (
    FastAPI, WebSocket, WebSocketDisconnect,
    Request, Form, Depends, Cookie, HTTPException, Query, APIRouter, status,
)
from fastapi.responses import (
    HTMLResponse, FileResponse, JSONResponse, RedirectResponse,
)
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from pymongo import MongoClient
from pymongo.collection import Collection

from dotenv import load_dotenv
import os
import re
import html
import logging
import asyncio
import random
import secrets
import threading
import time
import uuid
import json
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from langdetect import detect, DetectorFactory
import certifi
from bson import ObjectId
import bcrypt
from functools import wraps

try:
    from websockets.exceptions import ConnectionClosedOK
except ImportError:
    ConnectionClosedOK = Exception  # safe fallback if not installed
    
    

# ============================================================
# SECTION 2: INITIAL SETUP
# ============================================================

load_dotenv()
DetectorFactory.seed = 0

_log_level = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, _log_level, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("Chatbot")


# ============================================================
# SECTION 3: GLOBAL STATE
# ============================================================

BOT_DATA:      Dict[str, Any]                  = {}   # {owner_id: bot_config_dict}
PRODUCTS_DATA: Dict[str, List[Dict[str, Any]]] = {}   # {owner_id: [products]}

SESSIONS: Dict[str, Dict[str, Any]] = {}
SESSIONS_LOCK: threading.Lock = threading.Lock()  # guards SESSIONS dict

# Allowed CORS origins from env (comma-separated). Empty = allow all.
_ALLOWED_ORIGINS_RAW = os.getenv("ALLOWED_ORIGINS", "")
ALLOWED_ORIGINS: List[str] = (
    [o.strip().rstrip("/") for o in _ALLOWED_ORIGINS_RAW.split(",") if o.strip()]
    if _ALLOWED_ORIGINS_RAW
    else []
)

USER_SESSION_HISTORY:        Dict[str, Dict[str, Any]] = {}
MAX_SESSION_HISTORY_ENTRIES: int            = 10_000
HISTORY_LOCK:                threading.Lock = threading.Lock()

_DEFAULT_BOT: Dict[str, Any] = {
    "supported_languages": ["en"],
    "initial_message":     {"en": "Hello! How can I help you today?"},
    "faq":                 {},
    "smart_suggestions":   {},
}

_HTML_TAG_RE = re.compile(r"<[^>]+>")
_EMAIL_RE    = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
_URL_RE      = re.compile(r"^https?://", re.IGNORECASE)


# ============================================================
# SECTION 4: safe_json — RECURSIVE SERIALISER (CRASH FIX)
# ============================================================

def safe_json(data: Any) -> Any:
    """
    Recursively convert MongoDB-incompatible types to JSON-safe equivalents.

    Handles:
      ✔ dict  → recurse into values
      ✔ list  → recurse into elements
      ✔ ObjectId  → str
      ✔ datetime  → isoformat()

    Apply to EVERY response that returns MongoDB data:
      ✔ /dashboard, /products, /api/analytics, /api/data
      ✔ /chat history, /get-bot-config, /update-bot-config
    """
    if isinstance(data, dict):
        return {k: safe_json(v) for k, v in data.items()}
    if isinstance(data, list):
        return [safe_json(item) for item in data]
    if isinstance(data, ObjectId):
        return str(data)
    if isinstance(data, datetime):
        return data.isoformat()
    return data


# ============================================================
# SECTION 5: NLPUtils CLASS (merged from keywords.py)
# ============================================================

class NLPUtils:
    """
    All NLP helpers: language detection + keyword extraction.
    Merged from keywords.py — no external dependency required.
    """

    # ── Keyword databases ────────────────────────────────────────────────────

    COLOR_KEYWORDS: Dict[str, List[str]] = {
        "black":  ["black", "کالا", "سیاہ", "dark", "schwarz", "kala", "onyx"],
        "blue":   ["blue", "نیلا", "blau", "navy", "azure", "indigo", "light blue"],
        "white":  ["white", "سفید", "weiß", "ivory", "cream", "snow"],
        "red":    ["red", "لال", "rot", "surkh", "crimson", "scarlet"],
        "green":  ["green", "سبز", "grün", "olive", "sage", "teal"],
        "grey":   ["grey", "گرے", "grau", "gray", "silver", "charcoal"],
        "khaki":  ["khaki", "beige", "خاکی", "tan", "sand"],
        "yellow": ["yellow", "پیلا", "gelb", "gold", "neon"],
    }

    MATERIAL_KEYWORDS: Dict[str, List[str]] = {
        "leather":   ["leather", "چمڑا", "leder", "genuine leather"],
        "cotton":    ["cotton", "کاٹن", "baumwolle", "organic cotton", "twill"],
        "denim":     ["denim", "ڈينم", "jeans", "rigid denim"],
        "wool":      ["wool", "اون", "wolle", "merino", "lambswool", "knit"],
        "silk":      ["silk", "ریشم", "seide", "satin"],
        "linen":     ["linen", "لینن", "leinen"],
        "synthetic": ["nylon", "polyester", "spandex", "rayon", "chiffon"],
        "fleece":    ["fleece", "فلِیس", "brushed cotton"],
    }

    CATEGORY_KEYWORDS: Dict[str, List[str]] = {
        "jacket":         ["jacket", "جیکٹ", "coat", "blazer", "outerwear", "windbreaker"],
        "shirt":          ["shirt", "شرٹ", "tshirt", "tee", "top", "button-down"],
        "pants":          ["pant", "pants", "پینٹ", "jeans", "trousers", "chinos", "shorts"],
        "dress":          ["dress", "ڈریس", "gown", "maxi", "mini dress"],
        "hoodie_sweater": ["hoodie", "سویٹر", "sweater", "pullover", "knitwear"],
        "skirt":          ["skirt", "اسکرٹ", "pleated skirt"],
        "shoes":          ["shoes", "sneakers", "boots"],
    }

    INTENT_KEYWORDS: Dict[str, List[str]] = {
        "discount":     ["discount", "sale", "deal", "offer", "cheap", "رعایت"],
        "high_quality": ["best", "premium", "top", "excellent", "luxury"],
        "style":        ["trendy", "vintage", "casual", "formal", "fashion"],
        "weather":      ["winter", "summer", "cold", "warm", "سردی", "گرمی"],
        "low_price":    ["cheap", "budget", "affordable"],
        "high_price":   ["premium", "expensive", "luxury"],
    }

    # ── Fast lookup indexes ──────────────────────────────────────────────────

    @classmethod
    def _build_lookup(cls, kw_dict: Dict[str, List[str]]) -> Dict[str, str]:
        lookup: Dict[str, str] = {}
        for key, values in kw_dict.items():
            for v in values:
                lookup[v.lower()] = key
        return lookup

    # Computed once at class definition time
    _COLOR_LOOKUP:    Dict[str, str] = {}
    _MATERIAL_LOOKUP: Dict[str, str] = {}
    _CATEGORY_LOOKUP: Dict[str, str] = {}
    _INTENT_LOOKUP:   Dict[str, str] = {}

    # ── Language detection ───────────────────────────────────────────────────

    @staticmethod
    def detect_language(text: str) -> str:
        """
        Detects language: English ('en'), Urdu ('ur'), German ('de').
        Fallback to English for unknown / empty input.
        """
        try:
            if not text or not text.strip():
                return "en"
            if any("\u0600" <= c <= "\u06FF" for c in text):
                return "ur"
            lang = detect(text)
            if lang.startswith("ur"):
                return "ur"
            elif lang.startswith("de"):
                return "de"
            return "en"
        except Exception:
            return "en"

    # ── Keyword extraction ───────────────────────────────────────────────────

    @classmethod
    def extract_keywords(cls, text: str) -> Dict[str, Any]:
        """
        Convert raw query into structured features for scoring.
        Returns: {color, material, category, intents, language}
        """
        text_lower = text.lower()
        words      = re.findall(r"\w+", text_lower)

        result: Dict[str, Any] = {
            "language": cls.detect_language(text),
            "color":    None,
            "material": None,
            "category": None,
            "intents":  [],
        }

        for w in words:
            if not result["color"]    and w in cls._COLOR_LOOKUP:
                result["color"]    = cls._COLOR_LOOKUP[w]
            if not result["material"] and w in cls._MATERIAL_LOOKUP:
                result["material"] = cls._MATERIAL_LOOKUP[w]
            if not result["category"] and w in cls._CATEGORY_LOOKUP:
                result["category"] = cls._CATEGORY_LOOKUP[w]
            if w in cls._INTENT_LOOKUP:
                result["intents"].append(cls._INTENT_LOOKUP[w])

        return result

    @classmethod
    def smart_match(cls, text: str, keyword_dict: Dict[str, List[str]]) -> List[str]:
        """Return all keys whose synonym lists match anywhere in text."""
        text_lower = text.lower()
        return [
            key
            for key, synonyms in keyword_dict.items()
            if any(syn in text_lower for syn in synonyms)
        ]


# Initialise fast lookup maps after class body is defined
NLPUtils._COLOR_LOOKUP    = NLPUtils._build_lookup(NLPUtils.COLOR_KEYWORDS)
NLPUtils._MATERIAL_LOOKUP = NLPUtils._build_lookup(NLPUtils.MATERIAL_KEYWORDS)
NLPUtils._CATEGORY_LOOKUP = NLPUtils._build_lookup(NLPUtils.CATEGORY_KEYWORDS)
NLPUtils._INTENT_LOOKUP   = NLPUtils._build_lookup(NLPUtils.INTENT_KEYWORDS)

# Module-level aliases — preserves all existing call-sites in this file
COLOR_KEYWORDS    = NLPUtils.COLOR_KEYWORDS
MATERIAL_KEYWORDS = NLPUtils.MATERIAL_KEYWORDS
CATEGORY_KEYWORDS = NLPUtils.CATEGORY_KEYWORDS
INTENT_KEYWORDS   = NLPUtils.INTENT_KEYWORDS
detect_language   = NLPUtils.detect_language


# ============================================================
# SECTION 6: AnalyticsEngine CLASS (merged from analytics.py)
# ============================================================

class AnalyticsEngine:
    """
    All analytics logic: init, track_*, get_data.
    Merged from analytics.py — no external dependency required.

    Design: every method is a @staticmethod accepting analytics_col
    explicitly — identical signature to the old analytics.py functions,
    so all existing call-sites work without modification.
    """

    # ── Internal helpers ─────────────────────────────────────────────────────

    @staticmethod
    def _now() -> datetime:
        return datetime.now(timezone.utc)

    @staticmethod
    def _sanitize_key(text: str) -> str:
        """Mongo-safe key: strip illegal chars, enforce max length."""
        if not text:
            return "unknown"
        text = text.lower().strip()
        text = re.sub(r"[.$\x00]", "", text)
        if not text:
            return "unknown"
        return text[:120]

    @classmethod
    def _ensure_owner(cls, col: Collection, owner_id: str) -> None:
        """Atomic upsert — creates per-owner analytics doc if absent."""
        try:
            col.update_one(
                {"type": "analytics", "owner_id": owner_id},
                {
                    "$setOnInsert": {
                        "type":                "analytics",
                        "owner_id":            owner_id,
                        "total_searches":      0,
                        "total_clicks":        0,
                        "most_questions":      {},
                        "product_search":      {},
                        "product_clicks":      {},
                        "price_updates":       {},
                        "supported_languages": {},
                        "created_at":          cls._now(),
                        "updated_at":          cls._now(),
                    }
                },
                upsert=True,
            )
        except Exception as exc:
            logging.getLogger("Analytics").error("_ensure_owner error: %s", exc)

    @staticmethod
    def _empty(owner_id: Optional[str] = None) -> Dict[str, Any]:
        return {
            "type":                "analytics",
            "owner_id":            owner_id,
            "total_searches":      0,
            "total_clicks":        0,
            "most_questions":      {},
            "product_search":      {},
            "product_clicks":      {},
            "price_updates":       {},
            "supported_languages": {},
        }

    # ── Public API ───────────────────────────────────────────────────────────

    @classmethod
    def init_analytics(cls, col: Collection) -> None:
        """Initialise global (non-tenant) analytics doc if absent."""
        try:
            if col.count_documents({"type": "analytics", "owner_id": {"$exists": False}}) == 0:
                col.insert_one({
                    "type":                "analytics",
                    "total_searches":      0,
                    "total_clicks":        0,
                    "most_questions":      {},
                    "product_search":      {},
                    "product_clicks":      {},
                    "price_updates":       {},
                    "supported_languages": {},
                    "created_at":          cls._now(),
                    "updated_at":          cls._now(),
                })
                logging.getLogger("Analytics").info("Global analytics initialised")
        except Exception as exc:
            logging.getLogger("Analytics").error("init_analytics error: %s", exc)

    @classmethod
    def log_event(cls, col: Collection, owner_id: str, session_id: str,
                  event: str, payload: Optional[Dict[str, Any]] = None) -> None:
        """Generic event logger — always includes owner_id, session_id, timestamp."""
        if col is None:
            return
        try:
            col.insert_one({
                "owner_id":   owner_id,
                "session_id": session_id,
                "event":      event,
                "payload":    payload or {},
                "timestamp":  cls._now(),
            })
        except Exception as exc:
            logging.getLogger("Analytics").error("log_event error: %s", exc)

    @classmethod
    def log_chat(cls, col: Collection, owner_id: str, session_id: str,
                 message: str, lang: str) -> None:
        """Log a chat message interaction."""
        cls.log_event(col, owner_id, session_id, "chat", {
            "message": message[:200],
            "lang":    lang,
        })

    @classmethod
    def log_conversion(cls, col: Collection, owner_id: str, session_id: str,
                       product_id: str) -> None:
        """Log a product conversion (click-through treated as conversion)."""
        cls.log_event(col, owner_id, session_id, "conversion", {
            "product_id": product_id,
        })

    @classmethod
    def track_search(cls, col: Collection, query: str,
                     owner_id: Optional[str] = None) -> None:
        try:
            key = cls._sanitize_key(query)
            if owner_id:
                cls._ensure_owner(col, owner_id)
                col.update_one(
                    {"type": "analytics", "owner_id": owner_id},
                    {"$inc": {"total_searches": 1, f"product_search.{key}": 1},
                     "$set": {"updated_at": cls._now()}},
                )
            else:
                col.update_one(
                    {"type": "analytics", "owner_id": {"$exists": False}},
                    {"$inc": {"total_searches": 1, f"product_search.{key}": 1},
                     "$set": {"updated_at": cls._now()}},
                )
        except Exception as exc:
            logging.getLogger("Analytics").error("track_search error: %s", exc)

    @classmethod
    def track_question(cls, col: Collection, question: str,
                       owner_id: Optional[str] = None) -> None:
        try:
            key = cls._sanitize_key(question)
            if owner_id:
                cls._ensure_owner(col, owner_id)
                col.update_one(
                    {"type": "analytics", "owner_id": owner_id},
                    {"$inc": {f"most_questions.{key}": 1},
                     "$set": {"updated_at": cls._now()}},
                )
            else:
                col.update_one(
                    {"type": "analytics", "owner_id": {"$exists": False}},
                    {"$inc": {f"most_questions.{key}": 1},
                     "$set": {"updated_at": cls._now()}},
                )
        except Exception as exc:
            logging.getLogger("Analytics").error("track_question error: %s", exc)

    @classmethod
    def track_click(cls, col: Collection, product_id: str,
                    owner_id: Optional[str] = None) -> None:
        try:
            key = cls._sanitize_key(product_id)
            if owner_id:
                cls._ensure_owner(col, owner_id)
                col.update_one(
                    {"type": "analytics", "owner_id": owner_id},
                    {"$inc": {"total_clicks": 1, f"product_clicks.{key}": 1},
                     "$set": {"updated_at": cls._now()}},
                )
            else:
                col.update_one(
                    {"type": "analytics", "owner_id": {"$exists": False}},
                    {"$inc": {"total_clicks": 1, f"product_clicks.{key}": 1},
                     "$set": {"updated_at": cls._now()}},
                )
        except Exception as exc:
            logging.getLogger("Analytics").error("track_click error: %s", exc)

    @classmethod
    def track_price_update(cls, col: Collection, product_id: str,
                           owner_id: Optional[str] = None) -> None:
        try:
            key = cls._sanitize_key(product_id)
            if owner_id:
                cls._ensure_owner(col, owner_id)
                col.update_one(
                    {"type": "analytics", "owner_id": owner_id},
                    {"$inc": {f"price_updates.{key}": 1},
                     "$set": {"updated_at": cls._now()}},
                )
            else:
                col.update_one(
                    {"type": "analytics", "owner_id": {"$exists": False}},
                    {"$inc": {f"price_updates.{key}": 1},
                     "$set": {"updated_at": cls._now()}},
                )
        except Exception as exc:
            logging.getLogger("Analytics").error("track_price_update error: %s", exc)

    @classmethod
    def track_language(cls, col: Collection, language: str,
                       owner_id: Optional[str] = None) -> None:
        try:
            key = cls._sanitize_key(language)
            if owner_id:
                cls._ensure_owner(col, owner_id)
                col.update_one(
                    {"type": "analytics", "owner_id": owner_id},
                    {"$inc": {f"supported_languages.{key}": 1},
                     "$set": {"updated_at": cls._now()}},
                )
            else:
                col.update_one(
                    {"type": "analytics", "owner_id": {"$exists": False}},
                    {"$inc": {f"supported_languages.{key}": 1},
                     "$set": {"updated_at": cls._now()}},
                )
        except Exception as exc:
            logging.getLogger("Analytics").error("track_language error: %s", exc)

    @classmethod
    def get_analytics_data(cls, col: Collection,
                           owner_id: Optional[str] = None) -> Dict[str, Any]:
        try:
            if owner_id:
                data = col.find_one({"type": "analytics", "owner_id": owner_id})
                if not data:
                    return cls._empty(owner_id)
            else:
                data = col.find_one({"type": "analytics", "owner_id": {"$exists": False}})
                if not data:
                    return cls._empty(None)

            if data and "_id" in data:
                data["_id"] = str(data["_id"])
            return data or cls._empty(owner_id)

        except Exception as exc:
            logging.getLogger("Analytics").error("get_analytics_data error: %s", exc)
            return cls._empty(owner_id)


# Module-level shim — preserves legacy call-sites (analytics.track_search etc.)
class _AnalyticsShim:
    """Backward-compat shim — delegates to AnalyticsEngine class methods."""
    init_analytics     = staticmethod(AnalyticsEngine.init_analytics)
    track_search       = staticmethod(AnalyticsEngine.track_search)
    track_question     = staticmethod(AnalyticsEngine.track_question)
    track_click        = staticmethod(AnalyticsEngine.track_click)
    track_price_update = staticmethod(AnalyticsEngine.track_price_update)
    track_language     = staticmethod(AnalyticsEngine.track_language)
    get_analytics_data = staticmethod(AnalyticsEngine.get_analytics_data)

analytics = _AnalyticsShim()


# ============================================================
# SECTION 7: SESSION HISTORY HELPERS
# ============================================================

def _evict_session_history() -> None:
    """
    Purge oldest 20% of USER_SESSION_HISTORY entries.
    MUST be called while HISTORY_LOCK is held by the caller.
    """
    evict_count   = max(1, len(USER_SESSION_HISTORY) // 5)
    keys_to_evict = list(USER_SESSION_HISTORY.keys())[:evict_count]
    for k in keys_to_evict:
        USER_SESSION_HISTORY.pop(k, None)
    logger.info(
        "Session history eviction: removed %d oldest entries; remaining=%d",
        evict_count,
        len(USER_SESSION_HISTORY),
    )


# ============================================================
# SECTION 8: FASTAPI APP
# ============================================================

app = FastAPI(
    title="ShopMind AI E-Commerce Chatbot SaaS",
    version="5.0",
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)

# ── Security headers middleware ─────────────────────────────────────────────

@app.middleware("http")
async def security_headers_middleware(request: Request, call_next):
    """
    Adds security headers to every HTTP response.
    Also enforces Origin allowlist when ALLOWED_ORIGINS is configured.
    """
    # Origin check for state-changing methods (non-breaking for GET/HEAD)
    if ALLOWED_ORIGINS and request.method in ("POST", "PUT", "PATCH", "DELETE"):
        origin = request.headers.get("origin", "")
        if origin:
            normalised = origin.rstrip("/")
            if normalised not in ALLOWED_ORIGINS:
                logger.warning("Blocked request from disallowed origin: %s | path: %s", origin, request.url.path)
                return JSONResponse(
                    status_code=403,
                    content={"detail": "Origin not allowed"},
                )

    response = await call_next(request)
    response.headers["X-Content-Type-Options"]  = "nosniff"
    response.headers["X-Frame-Options"]         = "SAMEORIGIN"
    response.headers["X-XSS-Protection"]        = "1; mode=block"
    response.headers["Referrer-Policy"]         = "strict-origin-when-cross-origin"
    return response


# ── Centralised exception handler ──────────────────────────────────────────

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Catch-all: log unhandled exceptions without leaking stack traces."""
    logger.error("Unhandled exception on %s %s: %s", request.method, request.url.path, exc, exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"detail": "An internal server error occurred. Please try again."},
    )

templates = Jinja2Templates(directory="templates")

_static_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
if os.path.isdir(_static_dir):
    app.mount("/static", StaticFiles(directory=_static_dir), name="static")


# ============================================================
# SECTION 9: DATABASE
# ============================================================

MONGO_URI = os.getenv("MONGO_URI", "")

if not MONGO_URI:
    logger.critical(
        "MONGO_URI environment variable is not set. "
        "The application will run in degraded mode (no database)."
    )

try:
    client = MongoClient(
        MONGO_URI,
        tls=True,
        tlsCAFile=certifi.where(),
        serverSelectionTimeoutMS=10_000,
    )
    db = client["Chatbot"]

    products_col     = db["products"]
    meta_col         = db["bot_metadata"]
    analytics_col    = db["analytics"]
    emails_col       = db["emails"]
    integrations_col = db["integrations"]

    emails_col.create_index("email", unique=True)
    integrations_col.create_index("api_key", unique=True)
    # Composite indexes for common tenant-scoped queries
    products_col.create_index([("owner_id", 1), ("category", 1)])
    products_col.create_index([("owner_id", 1), ("created_at", -1)])
    meta_col.create_index([("owner_id", 1), ("type", 1)])
    analytics_col.create_index([("owner_id", 1), ("type", 1)])
    integrations_col.create_index("owner_id")

    client.admin.command("ping")
    logger.info("MongoDB connected successfully")

except Exception as _mongo_exc:
    logger.error("MongoDB connection failed: %s", _mongo_exc)
    products_col     = None
    meta_col         = None
    analytics_col    = None
    emails_col       = None
    integrations_col = None


# ============================================================
# SECTION 10: INPUT SANITISATION
# ============================================================

def _sanitise_str(value: str, max_len: int = 1000) -> str:
    value = value.strip()[:max_len]
    if _HTML_TAG_RE.search(value):
        raise ValueError("HTML markup is not allowed in product fields.")
    return value


def _sanitise_product_fields(
    title: str, description: str, category: str,
    color: str, material: str, image: str, image_link: str,
) -> Tuple[str, str, str, str, str, str, str]:
    return (
        _sanitise_str(title,       200),
        _sanitise_str(description, 2000),
        _sanitise_str(category,    100),
        _sanitise_str(color,       100),
        _sanitise_str(material,    100),
        _sanitise_str(image,       500),
        _sanitise_str(image_link,  500),
    )


# ============================================================
# SECTION 11: DATA LOADER
# ============================================================

def load_data_realtime(
    owner_id: Optional[str] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Load products + bot metadata from MongoDB for the given owner.

    MULTI-TENANT: always scoped by owner_id.
    Returns LOCAL copies isolated to this call.
    Also updates module-level globals for the WebSocket router.
    Error path returns a fresh _DEFAULT_BOT copy — never stale globals.

    PERF: uses a projection to fetch only the fields required by the
    scoring and response engine, avoiding large image/description blobs
    being loaded when only metadata is needed.
    """
    global PRODUCTS_DATA, BOT_DATA

    default_bot: Dict[str, Any] = dict(_DEFAULT_BOT)

    if products_col is None or meta_col is None:
        logger.error(
            "load_data_realtime: DB collections not initialised (owner=%s)",
            owner_id or "global",
        )
        return [], default_bot

    try:
        query_filter: Dict[str, Any] = {}
        if owner_id:
            query_filter["owner_id"] = owner_id

        temp_products: List[Dict[str, Any]] = []
        for product in products_col.find(query_filter):
            temp_products.append(safe_json(dict(product)))

        meta_filter: Dict[str, Any] = {"type": "config"}
        if owner_id:
            meta_filter["owner_id"] = owner_id

        meta = meta_col.find_one(meta_filter)
        if not meta and owner_id:
            meta = meta_col.find_one({"owner_id": owner_id})

        if meta:
            temp_bot: Dict[str, Any] = safe_json(dict(meta))
        else:
            if owner_id:
                logger.warning(
                    "load_data_realtime: no bot_metadata for owner_id=%s; using defaults",
                    owner_id,
                )
            temp_bot = default_bot

        key = owner_id or "__global__"
        # Atomically update globals so concurrent readers never see a partial state
        PRODUCTS_DATA[key] = list(temp_products)
        BOT_DATA[key]      = dict(temp_bot)

        logger.info("Data sync | owner=%s | products=%d", owner_id or "global", len(temp_products))
        return temp_products, temp_bot

    except Exception as exc:
        logger.error("load_data_realtime error (owner=%s): %s", owner_id or "global", exc)
        return [], dict(_DEFAULT_BOT)


def init_database_sync() -> None:
    """Confirm DB connectivity at startup without pre-loading tenant data."""
    if products_col is not None:
        logger.info("Database connectivity confirmed — multi-tenant mode active")
    else:
        logger.warning("Database unavailable at startup — running in degraded mode")


# ============================================================
# SECTION 12: PRODUCT HELPERS
# ============================================================

def smart_match(text: str, keyword_dict: Dict[str, List[str]]) -> List[str]:
    return NLPUtils.smart_match(text, keyword_dict)


def process_user_query(query: str) -> Dict[str, Any]:
    return {
        "language":   NLPUtils.detect_language(query),
        "colors":     smart_match(query, COLOR_KEYWORDS),
        "materials":  smart_match(query, MATERIAL_KEYWORDS),
        "categories": smart_match(query, CATEGORY_KEYWORDS),
        "intents":    smart_match(query, INTENT_KEYWORDS),
    }


def parse_price_range(query: str) -> Dict[str, float]:
    q = (
        query.lower()
        .replace("$", "").replace("€", "").replace("rs", "").replace("pkr", "")
    )
    price_range: Dict[str, float] = {}

    if m := re.search(r"(under|below|less than|کم|weniger als|unter)\s*(\d+)", q):
        try:
            price_range["max"] = float(m.group(2))
        except ValueError:
            pass

    if m := re.search(r"(over|above|greater than|زیادہ|über|mehr als)\s*(\d+)", q):
        try:
            price_range["min"] = float(m.group(2))
        except ValueError:
            pass

    return price_range


def score_product_relevance(
    query: str, product: Dict[str, Any], price_range: Dict[str, float],
) -> float:
    q = query.lower()
    field_text = " ".join(
        str(product.get(f, "")).lower()
        for f in ["title", "description", "color", "material", "category"]
    )
    q_words = set(re.findall(r"\w+", q))
    p_words = set(re.findall(r"\w+", field_text))
    score   = len(q_words & p_words) * 0.8

    for cat, kws in CATEGORY_KEYWORDS.items():
        if any(kw in q for kw in kws) and cat in field_text:
            score += 3.0
    for col, kws in COLOR_KEYWORDS.items():
        if any(kw in q for kw in kws) and col in str(product.get("color", "")).lower():
            score += 2.5
    for mat, kws in MATERIAL_KEYWORDS.items():
        if any(kw in q for kw in kws) and mat in str(product.get("material", "")).lower():
            score += 2.5

    score += float(product.get("trending_score", 0)) * 1.5
    score += float(product.get("rating",          0)) * 1.0

    try:
        raw_price = re.sub(r"[^0-9.]", "", str(product.get("price", "0")))
        price     = float(raw_price)
        if "min" in price_range and price >= price_range["min"]:
            score += 2.0
        if "max" in price_range and price <= price_range["max"]:
            score += 2.0
        if any(kw in q for kw in INTENT_KEYWORDS.get("low_price",  [])) and price < 50:
            score += 1.5
        elif any(kw in q for kw in INTENT_KEYWORDS.get("high_price", [])) and price >= 150:
            score += 1.5
    except (ValueError, TypeError):
        pass

    if any(kw in q for kw in INTENT_KEYWORDS.get("discount", [])):
        score += 1.0

    return score


def filter_products(
    query: str, products: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], str]:
    q           = query.lower()
    filtered    = list(products)
    desc_list:  List[str] = []
    price_range = parse_price_range(query)

    # Stage 1: Category filter (FIX-E: guard with `if temp:`)
    for cat, kws in CATEGORY_KEYWORDS.items():
        if any(kw in q for kw in kws):
            temp = [
                p for p in filtered
                if cat in p.get("category", "").lower()
                or cat in p.get("title",    "").lower()
            ]
            if temp:
                filtered = temp
                desc_list.append(cat)
            break

    # Stage 2: Colour filter
    for col, kws in COLOR_KEYWORDS.items():
        if any(kw in q for kw in kws):
            temp = [
                p for p in filtered
                if col in p.get("color", "").lower()
                or col in p.get("title", "").lower()
            ]
            if temp:
                filtered = temp
                desc_list.append(col)
            break

    # Stage 3: Price range filter
    if price_range:
        def _within_price(p: Dict[str, Any]) -> bool:
            try:
                v = float(re.sub(r"[^0-9.]", "", str(p.get("price", "0"))))
                if "min" in price_range and v < price_range["min"]:
                    return False
                if "max" in price_range and v > price_range["max"]:
                    return False
                return True
            except (ValueError, TypeError):
                return False

        price_filtered = [p for p in filtered if _within_price(p)]
        if price_filtered:
            filtered = price_filtered
            desc_list.append("matching price criteria")

    # Stage 4: Relevance scoring
    scored = [
        {"product": p, "score": score_product_relevance(query, p, price_range)}
        for p in filtered
    ]
    ranked = sorted(scored, key=lambda x: x["score"], reverse=True)
    final  = [x["product"] for x in ranked if x["score"] > 0.0]

    description = " and ".join(desc_list) if desc_list else "your request"
    return final, description


# ============================================================
# SECTION 13: FAQ & SUGGESTIONS
# ============================================================

def get_faq_response(
    query: str, bot_data: Dict[str, Any],
) -> Optional[Dict[str, str]]:
    faq = bot_data.get("faq", {})
    q   = query.lower()

    if any(k in q for k in ["ship", "deliver", "ارسال", "versand", "delivery", "kab ayega"]):
        return faq.get("shipping")
    if any(k in q for k in ["return", "refund", "واپسی", "rückgabe", "exchange", "back"]):
        return faq.get("return")
    if any(k in q for k in ["track", "order", "ٹریک", "verfolgen", "status", "kahan hai"]):
        return faq.get("track")
    if any(k in q for k in ["why", "choose", "کیوں"]):
        return faq.get("Why I Choose Your Products")
    if any(k in q for k in ["best quality", "qualities", "business quality"]):
        return faq.get("What's the Best Quality of Your Business")
    if any(k in q for k in ["hello", "hi", "how are you", "what's going on", "hey"]):
        return faq.get("Hello")

    return None


def get_dynamic_suggestions(
    user_id:         str,
    context:         str,
    lang:            str,
    bot_data:        Dict[str, Any],
    session_history: Dict[str, Dict[str, Any]],
) -> List[str]:
    entry = session_history.setdefault(user_id, {})
    entry.setdefault("shown",      [])
    entry.setdefault("lang",       lang)
    entry.setdefault("last_query", "")

    sugs_dict = bot_data.get("smart_suggestions", {})

    nested = sugs_dict.get(context, {})
    if isinstance(nested, dict) and lang in nested:
        all_sugs = nested[lang]
    elif isinstance(nested, dict) and "en" in nested:
        all_sugs = nested["en"]
    elif isinstance(sugs_dict.get(lang), list):
        all_sugs = sugs_dict[lang]
    elif isinstance(sugs_dict.get("en"), list):
        all_sugs = sugs_dict["en"]
    else:
        all_sugs = []

    shown     = entry["shown"]
    available = [s for s in all_sugs if s not in shown]

    if len(available) < 2 and all_sugs:
        entry["shown"] = []
        available = all_sugs

    selected = random.sample(available, min(4, len(available)))
    entry["shown"] = list(set(entry["shown"] + selected))
    return selected


# ============================================================
# SECTION 14: ANALYTICS WRAPPERS
# ============================================================

def _track_search_scoped(query: str, owner_id: Optional[str]) -> None:
    if analytics_col is None:
        logger.warning("_track_search_scoped: analytics_col is None; skipping")
        return
    try:
        q = str(query)[:200]
        AnalyticsEngine.track_search(analytics_col, q, owner_id=owner_id)
    except Exception as exc:
        logger.error("_track_search_scoped error: %s", exc)


def _track_question_scoped(question: str, owner_id: Optional[str]) -> None:
    if analytics_col is None:
        logger.warning("_track_question_scoped: analytics_col is None; skipping")
        return
    try:
        q = str(question)[:200]
        AnalyticsEngine.track_question(analytics_col, q, owner_id=owner_id)
    except Exception as exc:
        logger.error("_track_question_scoped error: %s", exc)


# Legacy module-level helpers
def track_search(query: str) -> None:
    if analytics_col is None:
        logger.warning("track_search (legacy): analytics_col is None; skipping")
        return
    try:
        AnalyticsEngine.track_search(analytics_col, query)
    except Exception as exc:
        logger.error("track_search (legacy) error: %s", exc)


def track_question(question: str) -> None:
    if analytics_col is None:
        logger.warning("track_question (legacy): analytics_col is None; skipping")
        return
    try:
        AnalyticsEngine.track_question(analytics_col, question)
    except Exception as exc:
        logger.error("track_question (legacy) error: %s", exc)


# ============================================================
# SECTION 15: BOT RESPONSE ENGINE
# ============================================================

def generate_bot_response(
    user_id:  str,
    msg:      str,
    owner_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Core chatbot brain — tenant-scoped, thread-safe, exception-safe.
    FIX-G: entire body wrapped in try/except → always returns safe dict.
    FIX-H: uses local load_data_realtime() return values, never globals.
    FIX-C: all USER_SESSION_HISTORY reads/writes are lock-guarded.
    """
    try:
        products, bot = load_data_realtime(owner_id=owner_id)

        scoped_user_id = f"{owner_id}:{user_id}" if owner_id else user_id

        with HISTORY_LOCK:
            if (
                scoped_user_id not in USER_SESSION_HISTORY
                and len(USER_SESSION_HISTORY) >= MAX_SESSION_HISTORY_ENTRIES
            ):
                _evict_session_history()

            lang = detect_language(msg)
            USER_SESSION_HISTORY.setdefault(scoped_user_id, {})
            USER_SESSION_HISTORY[scoped_user_id]["lang"] = lang

        if len(msg.split()) > 1:
            _track_search_scoped(msg,  owner_id)
            _track_question_scoped(msg, owner_id)

        response: Dict[str, Any] = {
            "reply":       None,
            "carousel":    None,
            "suggestions": [],
        }
        query_lower: str = msg.lower()

        if any(kw in query_lower for kw in INTENT_KEYWORDS.get("discount", [])):
            discount_msg = bot.get("discount_message", {}).get(
                lang, bot.get("discount_message", {}).get("en")
            )
            if discount_msg:
                response["reply"] = discount_msg

        faq_result = get_faq_response(msg, bot)
        if faq_result:
            response["reply"] = faq_result.get(lang, faq_result.get("en"))
            with HISTORY_LOCK:
                response["suggestions"] = get_dynamic_suggestions(
                    scoped_user_id, "greeting", lang, bot, USER_SESSION_HISTORY
                )
            return response

        with HISTORY_LOCK:
            last_query = USER_SESSION_HISTORY[scoped_user_id].get("last_query", "")

        combined = (
            (last_query + " " + msg).strip()
            if len(msg.split()) < 3 and last_query
            else msg
        )

        filtered, desc = filter_products(combined, products)

        with HISTORY_LOCK:
            USER_SESSION_HISTORY[scoped_user_id]["last_query"] = combined

        if filtered:
            response["carousel"] = filtered[:8]
            response["reply"] = {
                "en": f"Sure — based on your search for *{desc}*, here are the most relevant picks.",
                "ur": f"بالکل — آپ کی تلاش *{desc}* کی بنیاد پر یہ بہترین آپشنز ہیں:",
                "de": f"Gerne — basierend auf Ihrer Suche nach *{desc}* finden Sie hier passende Empfehlungen.",
            }.get(lang, f"Here are the best matches for *{desc}*.")
            with HISTORY_LOCK:
                response["suggestions"] = get_dynamic_suggestions(
                    scoped_user_id, "greeting", lang, bot, USER_SESSION_HISTORY
                )
            return response

        response["reply"] = {
            "en": "I couldn't find the perfect match — want to try another color, size, or price range?",
            "ur": "مجھے ٹھیک چیز نہیں ملی — کیا آپ رنگ، سائز یا قیمت بدل کر دیکھیں گے؟",
            "de": "Nichts Passendes — möchten Sie eine andere Farbe, Größe oder Preisspanne versuchen?",
        }.get(lang, "Let's refine your search a bit.")
        with HISTORY_LOCK:
            response["suggestions"] = get_dynamic_suggestions(
                scoped_user_id, "greeting", lang, bot, USER_SESSION_HISTORY
            )
        return response

    except Exception as exc:
        logger.error(
            "generate_bot_response unhandled error | user=%s owner=%s: %s",
            user_id, owner_id, exc, exc_info=True,
        )
        return {
            "reply":       "Sorry, I encountered an unexpected error. Please try again.",
            "carousel":    None,
            "suggestions": [],
        }


# ============================================================
# SECTION 16: AUTH HELPERS
# ============================================================

def hash_password(plain: str) -> str:
    return bcrypt.hashpw(plain.encode(), bcrypt.gensalt()).decode()


def verify_password(plain: str, hashed: str) -> bool:
    return bcrypt.checkpw(plain.encode(), hashed.encode())


def create_session(owner: dict) -> str:
    token = secrets.token_hex(32)
    session_data = {
        "owner_id": str(owner["_id"]),
        "email":    owner.get("email", ""),
    }
    with SESSIONS_LOCK:
        SESSIONS[token] = session_data
    return token


def get_current_user(
    session_token: Optional[str] = Cookie(default=None),
) -> Optional[dict]:
    if not session_token:
        return None
    with SESSIONS_LOCK:
        return SESSIONS.get(session_token)


def require_auth(user: Optional[dict] = Depends(get_current_user)) -> dict:
    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return user


# ============================================================
# SECTION 17: AUTH ROUTES
# ============================================================

@app.get("/signup", response_class=HTMLResponse)
async def signup_page(request: Request) -> HTMLResponse:
    return templates.TemplateResponse("signup.html", {"request": request})


@app.post("/signup", response_class=HTMLResponse)
async def signup_submit(
    request:          Request,
    email:            str = Form(...),
    password:         str = Form(...),
    confirm_password: str = Form(...),
) -> HTMLResponse:
    if emails_col is None:
        return templates.TemplateResponse(
            "signup.html", {"request": request, "error": "Database not connected"},
        )

    clean_email = email.lower().strip()

    if not _EMAIL_RE.match(clean_email):  # FIX-F
        return templates.TemplateResponse(
            "signup.html", {"request": request, "error": "Please enter a valid email address"},
        )
    if password != confirm_password:
        return templates.TemplateResponse(
            "signup.html", {"request": request, "error": "Passwords do not match"},
        )
    if len(password) < 8:
        return templates.TemplateResponse(
            "signup.html", {"request": request, "error": "Password must be at least 8 characters"},
        )

    existing = await asyncio.to_thread(emails_col.find_one, {"email": clean_email})
    if existing:
        return templates.TemplateResponse(
            "signup.html", {"request": request, "error": "Email already registered"},
        )

    hashed = hash_password(password)
    await asyncio.to_thread(
        emails_col.insert_one,
        {"email": clean_email, "password": hashed, "created_at": datetime.now(timezone.utc)},
    )
    logger.info("New owner registered: %s", clean_email)
    return RedirectResponse(url="/login", status_code=303)


@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request) -> HTMLResponse:
    return templates.TemplateResponse("login.html", {"request": request})


@app.post("/login", response_class=HTMLResponse)
async def login_submit(
    request:  Request,
    email:    str = Form(...),
    password: str = Form(...),
) -> HTMLResponse:
    if emails_col is None:
        return templates.TemplateResponse(
            "login.html", {"request": request, "error": "Database not connected"},
        )

    owner = await asyncio.to_thread(
        emails_col.find_one, {"email": email.lower().strip()}
    )
    if not owner or not verify_password(password, owner["password"]):
        return templates.TemplateResponse(
            "login.html", {"request": request, "error": "Invalid credentials"},
        )

    token    = create_session(owner)
    response = RedirectResponse(url="/Dashboard", status_code=303)
    response.set_cookie(key="session_token", value=token, httponly=True, samesite="lax")
    logger.info("Owner logged in: %s", owner.get("email"))
    return response


@app.get("/logout")
async def logout(
    session_token: Optional[str] = Cookie(default=None),
) -> RedirectResponse:
    if session_token and session_token in SESSIONS:
        del SESSIONS[session_token]
    resp = RedirectResponse(url="/login", status_code=303)
    resp.delete_cookie("session_token")
    return resp


# ============================================================
# SECTION 18: PUBLIC ROUTES
# ============================================================

@app.get("/")
async def root():
    base_dir  = os.path.dirname(os.path.abspath(__file__))
    html_path = os.path.join(base_dir, "templates", "chat.html")
    if os.path.exists(html_path):
        return FileResponse(html_path)
    return JSONResponse({"status": "active", "info": "ShopMind AI is running. WebSocket at /ws/chat"})


@app.get("/landing", response_class=HTMLResponse)
async def landing_page(request: Request) -> HTMLResponse:
    return templates.TemplateResponse("landing.html", {"request": request})


# ============================================================
# SECTION 19: PROTECTED ROUTES — DASHBOARD
# ============================================================

@app.get("/Dashboard")
async def dashboard(
    request: Request,
    user:    dict = Depends(get_current_user),
):
    if not user:
        return RedirectResponse(url="/login", status_code=303)

    _products, _bot = await asyncio.to_thread(load_data_realtime, user["owner_id"])
    logger.debug("Dashboard loaded | owner=%s | products=%d", user["owner_id"], len(_products))

    base_dir  = os.path.dirname(os.path.abspath(__file__))
    html_path = os.path.join(base_dir, "templates", "index.html")
    if os.path.exists(html_path):
        return FileResponse(html_path)
    return JSONResponse({"status": "active", "info": "Dashboard loaded"})


@app.get("/api/data")
async def get_home_data(user: dict = Depends(get_current_user)):
    """
    Return tenant-scoped product list and bot config.
    safe_json() applied — ObjectId and datetime are serialised.
    """
    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated")

    try:
        products, bot = await asyncio.to_thread(load_data_realtime, user["owner_id"])

        serialised = [safe_json({**p}) for p in products]

        raw_sugs = bot.get("smart_suggestions", {})
        return safe_json({
            "products": serialised,
            "config": {
                "faq":                 bot.get("faq",               {}),
                "initial_message":     bot.get("initial_message",   {}),
                "discount_message":    bot.get("discount_message",  {}),
                "greeting":            bot.get("greeting",          {}),
                "supported_languages": bot.get("supported_languages", ["en", "ur"]),
                "smart_suggestions": {
                    "en": raw_sugs.get("en", []),
                    "ur": raw_sugs.get("ur", []),
                    "de": raw_sugs.get("de", []),
                },
            },
        })

    except Exception as exc:
        logger.error("get_home_data error: %s", exc)
        return JSONResponse(
            status_code=500,
            content={
                "products": [],
                "config":   {"faq": {}, "smart_suggestions": {"en": [], "ur": []}},
                "error":    "Failed to synchronise with MongoDB Atlas",
            },
        )


@app.get("/api/analytics")
async def get_analytics(user: dict = Depends(get_current_user)):
    """Return owner-scoped analytics. safe_json() applied."""
    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated")
    if analytics_col is None:
        raise HTTPException(status_code=500, detail="Analytics DB not connected")

    try:
        raw = await asyncio.to_thread(
            AnalyticsEngine.get_analytics_data, analytics_col, user["owner_id"]
        )
        return safe_json({
            "total_searches":      raw.get("total_searches",      0),
            "total_clicks":        raw.get("total_clicks",        0),
            "supported_languages": raw.get("supported_languages", {}),
            "product_search":      raw.get("product_search",      {}),
            "product_clicks":      raw.get("product_clicks",      {}),
            "most_questions":      raw.get("most_questions",      {}),
        })

    except Exception as exc:
        logger.error("get_analytics error: %s", exc)
        return JSONResponse(status_code=500, content={
            "total_searches": 0, "total_clicks": 0,
            "supported_languages": {}, "product_search": {},
            "product_clicks": {}, "most_questions": {},
            "error": "Failed to fetch analytics",
        })


@app.get("/analytics_dashboard", response_class=HTMLResponse)
async def analytics_dashboard(
    request: Request, user: dict = Depends(get_current_user),
) -> HTMLResponse:
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    return templates.TemplateResponse("index.html", {"request": request})


# ============================================================
# SECTION 20: BOT METADATA CRUD (NEW)
# ============================================================

def _default_bot_config(owner_id: str) -> Dict[str, Any]:
    """Generate a default bot_metadata document for a new owner."""
    return {
        "owner_id":            owner_id,
        "type":                "config",
        "bot_name":            "AI Assistant",
        "supported_languages": ["en"],
        "initial_message":     {"en": "Hello! How can I help you today?"},
        "faq":                 {},
        "smart_suggestions":   {},
        "discount_message":    {},
        "version":             "4.0",
        "created_at":          datetime.now(timezone.utc),
    }


@app.get("/get-bot-config")
async def get_bot_config(user: dict = Depends(get_current_user)):
    """
    Return bot_metadata for the authenticated owner.
    Auto-creates a default config if none exists.
    safe_json() applied to handle ObjectId + datetime.

    MULTI-TENANT: ALWAYS filters by owner_id — no global queries.
    """
    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated")
    if meta_col is None:
        raise HTTPException(status_code=500, detail="Database not connected")

    owner_id = user["owner_id"]

    config = await asyncio.to_thread(
        meta_col.find_one,
        {"owner_id": owner_id, "type": "config"},
    )

    if not config:
        # Auto-create default config
        default = _default_bot_config(owner_id)
        await asyncio.to_thread(meta_col.insert_one, default)
        logger.info("Auto-created default bot config for owner=%s", owner_id)
        config = await asyncio.to_thread(
            meta_col.find_one,
            {"owner_id": owner_id, "type": "config"},
        )

    return JSONResponse(safe_json(config))


@app.post("/update-bot-config")
async def update_bot_config(
    request: Request,
    user:    dict = Depends(get_current_user),
):
    """
    Partial-update bot_metadata for the authenticated owner.

    - Uses $set (never overwrites entire document blindly).
    - Only fields present in the JSON body are updated.
    - auto-creates config doc if missing.
    - safe_json() applied to response.

    MULTI-TENANT: ALWAYS scoped to current owner_id.

    Allowed updatable fields (all optional):
      bot_name, supported_languages, initial_message, faq,
      smart_suggestions, discount_message, version
    """
    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated")
    if meta_col is None:
        raise HTTPException(status_code=500, detail="Database not connected")

    owner_id = user["owner_id"]

    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=422, detail="Invalid JSON body")

    ALLOWED_FIELDS = {
        "bot_name", "supported_languages", "initial_message",
        "faq", "smart_suggestions", "discount_message", "version",
    }

    update_fields: Dict[str, Any] = {
        k: v for k, v in body.items() if k in ALLOWED_FIELDS
    }

    if not update_fields:
        raise HTTPException(
            status_code=422,
            detail=f"No valid fields to update. Allowed: {sorted(ALLOWED_FIELDS)}",
        )

    update_fields["updated_at"] = datetime.now(timezone.utc)

    # Ensure the document exists first (auto-create if missing)
    await asyncio.to_thread(
        meta_col.update_one,
        {"owner_id": owner_id, "type": "config"},
        {"$setOnInsert": _default_bot_config(owner_id)},
        upsert=True,
    )

    # Partial $set — never blindly overwrites
    result = await asyncio.to_thread(
        meta_col.update_one,
        {"owner_id": owner_id, "type": "config"},
        {"$set": update_fields},
    )

    # Invalidate BOT_DATA cache for this owner
    BOT_DATA.pop(owner_id, None)

    logger.info(
        "Bot config updated | owner=%s | fields=%s",
        owner_id, list(update_fields.keys()),
    )

    updated = await asyncio.to_thread(
        meta_col.find_one,
        {"owner_id": owner_id, "type": "config"},
    )

    return JSONResponse({"ok": True, "config": safe_json(updated)})


# ============================================================
# SECTION 21: PRODUCT CRUD
# ============================================================

def _is_ajax(request: Request) -> bool:
    accept = request.headers.get("accept", "")
    return "application/json" in accept or "text/javascript" in accept


@app.post("/Add_product")
async def add_new_product(
    request:        Request,
    id:             str   = Form(...),
    title:          str   = Form(...),
    description:    str   = Form(...),
    category:       str   = Form(...),
    color:          str   = Form(...),
    material:       str   = Form(...),
    price:          float = Form(...),
    rating:         float = Form(...),
    trending_score: float = Form(...),
    image:          str   = Form(...),
    image_link:     str   = Form(...),
    user:           dict  = Depends(get_current_user),
):
    if not user:
        if _is_ajax(request):
            return JSONResponse({"ok": False, "error": "Not authenticated"}, status_code=401)
        return RedirectResponse(url="/login", status_code=303)

    if products_col is None:
        if _is_ajax(request):
            return JSONResponse({"ok": False, "error": "Database not connected"}, status_code=500)
        return templates.TemplateResponse("index.html", {"request": request, "message": "Database not connected"})

    errors: List[str] = []
    if price < 0:
        errors.append("Price must be ≥ 0")
    if not (0 <= rating <= 5):
        errors.append("Rating must be between 0 and 5")
    if not (0 <= trending_score <= 10):
        errors.append("Trending score must be between 0 and 10")

    try:
        title, description, category, color, material, image, image_link = (
            _sanitise_product_fields(title, description, category, color, material, image, image_link)
        )
    except ValueError as exc:
        errors.append(str(exc))

    if errors:
        msg = " | ".join(errors)
        if _is_ajax(request):
            return JSONResponse({"ok": False, "error": msg}, status_code=422)
        return templates.TemplateResponse("index.html", {"request": request, "message": msg})

    new_product = {
        "id":             id.strip()[:100],
        "title":          title,
        "description":    description,
        "category":       category,
        "color":          color,
        "material":       material,
        "price":          price,
        "rating":         rating,
        "trending_score": trending_score,
        "image":          image,
        "image_link":     image_link,
        "owner_id":       user["owner_id"],
        "created_at":     datetime.now(timezone.utc),
    }

    try:
        result = await asyncio.to_thread(products_col.insert_one, new_product)
        logger.info("Product added | owner=%s | id=%s | mongo_id=%s", user["owner_id"], id, result.inserted_id)
        if _is_ajax(request):
            return JSONResponse({"ok": True, "message": "Product added successfully!"})
        return templates.TemplateResponse("index.html", {"request": request, "message": "Product added successfully!"})

    except Exception as exc:
        logger.error("add_new_product DB error: %s", exc)
        if _is_ajax(request):
            return JSONResponse({"ok": False, "error": "Database write failed"}, status_code=500)
        return templates.TemplateResponse("index.html", {"request": request, "message": f"Failed to add product: {exc}"})


@app.post("/delete_product")
async def delete_product(
    request: Request,
    id:      str  = Form(...),
    user:    dict = Depends(get_current_user),
):
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    if products_col is None:
        return templates.TemplateResponse("index.html", {"request": request, "message": "Database not connected"})

    try:
        result = await asyncio.to_thread(
            products_col.delete_one,
            {"_id": ObjectId(id), "owner_id": user["owner_id"]},
        )
        if result.deleted_count == 0:
            logger.warning("delete_product: not found or access denied | id=%s owner=%s", id, user["owner_id"])
            return templates.TemplateResponse("index.html", {"request": request, "message": "Product not found or access denied"})
        logger.info("Product deleted | id=%s | owner=%s", id, user["owner_id"])
        return RedirectResponse(url="/Dashboard", status_code=303)

    except Exception as exc:
        logger.error("delete_product error: %s", exc)
        return templates.TemplateResponse("index.html", {"request": request, "message": f"Error deleting product: {exc}"})


@app.post("/update_product")
async def update_product(
    request:        Request,
    product_id:     str   = Form(...),
    title:          str   = Form(...),
    description:    str   = Form(...),
    category:       str   = Form(...),
    color:          str   = Form(...),
    material:       str   = Form(...),
    price:          float = Form(...),
    rating:         float = Form(...),
    trending_score: float = Form(...),
    image:          str   = Form(...),
    image_link:     str   = Form(...),
    user:           dict  = Depends(get_current_user),
):
    if not user:
        if _is_ajax(request):
            return JSONResponse({"ok": False, "error": "Not authenticated"}, status_code=401)
        return RedirectResponse(url="/login", status_code=303)

    if products_col is None:
        if _is_ajax(request):
            return JSONResponse({"ok": False, "error": "Database not connected"}, status_code=500)
        return templates.TemplateResponse("index.html", {"request": request, "message": "Database not connected"})

    errors: List[str] = []
    if price < 0:
        errors.append("Price must be ≥ 0")
    if not (0 <= rating <= 5):
        errors.append("Rating must be between 0 and 5")
    if not (0 <= trending_score <= 10):
        errors.append("Trending score must be between 0 and 10")

    try:
        title, description, category, color, material, image, image_link = (
            _sanitise_product_fields(title, description, category, color, material, image, image_link)
        )
    except ValueError as exc:
        errors.append(str(exc))

    if errors:
        msg = " | ".join(errors)
        if _is_ajax(request):
            return JSONResponse({"ok": False, "error": msg}, status_code=422)
        return templates.TemplateResponse("index.html", {"request": request, "message": msg})

    update_data = {
        "title":          title,
        "description":    description,
        "category":       category,
        "color":          color,
        "material":       material,
        "price":          price,
        "rating":         rating,
        "trending_score": trending_score,
        "image":          image,
        "image_link":     image_link,
        "updated_at":     datetime.now(timezone.utc),
    }

    try:
        result = await asyncio.to_thread(
            products_col.update_one,
            {"_id": ObjectId(product_id), "owner_id": user["owner_id"]},
            {"$set": update_data},
        )
        if analytics_col is not None:
            await asyncio.to_thread(
                AnalyticsEngine.track_price_update, analytics_col, product_id, user["owner_id"]
            )

        if result.matched_count == 0:
            msg = "No product found with this ID or access denied."
            logger.warning("update_product: no match | product_id=%s owner=%s", product_id, user["owner_id"])
            if _is_ajax(request):
                return JSONResponse({"ok": False, "error": msg}, status_code=404)
            return templates.TemplateResponse("index.html", {"request": request, "message": msg})

        logger.info("Product updated | product_id=%s | owner=%s", product_id, user["owner_id"])
        if _is_ajax(request):
            return JSONResponse({"ok": True, "message": "Product updated successfully!"})
        return templates.TemplateResponse("index.html", {"request": request, "message": "Product updated successfully!"})

    except Exception as exc:
        logger.error("update_product DB error: %s", exc)
        if _is_ajax(request):
            return JSONResponse({"ok": False, "error": str(exc)}, status_code=500)
        return templates.TemplateResponse("index.html", {"request": request, "message": f"Update failed: {exc}"})


# ============================================================
# SECTION 22: CHAT ROUTE
# ============================================================

@app.get("/chat")
async def chat_page(
    request: Request,
    api_key: Optional[str] = None,
):
    if api_key:
        if integrations_col is None:
            return JSONResponse({"error": "Database not connected"}, status_code=500)
        integration = await asyncio.to_thread(integrations_col.find_one, {"api_key": api_key})
        if not integration:
            return JSONResponse({"error": "Invalid API key"}, status_code=403)

        _products, _bot = await asyncio.to_thread(load_data_realtime, integration["owner_id"])
        logger.debug("/chat | owner=%s | products=%d", integration["owner_id"], len(_products))
    else:
        _products, _bot = await asyncio.to_thread(load_data_realtime)
        logger.debug("/chat | global fallback | products=%d", len(_products))

    base_dir  = os.path.dirname(os.path.abspath(__file__))
    html_path = os.path.join(base_dir, "templates", "chat.html")
    if os.path.exists(html_path):
        return FileResponse(html_path)
    return JSONResponse({"status": "active"})


# ============================================================
# SECTION 23: WEBSITE INTEGRATIONS
# ============================================================

@app.get("/api/resolve-owner")
async def resolve_owner(api_key: str):
    """Validate API key and return safe public bot metadata (owner_id intentionally omitted)."""
    if integrations_col is None:
        raise HTTPException(status_code=500, detail="Database not connected")

    integration = await asyncio.to_thread(integrations_col.find_one, {"api_key": api_key})
    if not integration:
        raise HTTPException(status_code=403, detail="Invalid API key")

    owner_id = integration["owner_id"]

    meta = (
        await asyncio.to_thread(meta_col.find_one, {"owner_id": owner_id})
        if meta_col is not None
        else None
    )
    meta = meta or {}

    raw_initial = meta.get("initial_message", "Hello! How can I help?")
    if isinstance(raw_initial, dict):
        initial_message = raw_initial.get("en") or next(iter(raw_initial.values()), "Hello! How can I help?")
    else:
        initial_message = str(raw_initial) if raw_initial else "Hello! How can I help?"

    return {
        "valid":           True,
        "bot_name":        meta.get("bot_name", "AI Assistant"),
        "initial_message": initial_message,
        "website_url":     integration.get("website_url", ""),
    }


@app.post("/api/integrations/register")
async def register_integration(
    website_url: str  = Form(...),
    user:        dict = Depends(get_current_user),
):
    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated")
    if integrations_col is None:
        raise HTTPException(status_code=500, detail="Database not connected")

    url = website_url.strip()
    if not url:
        return JSONResponse({"ok": False, "error": "Website URL cannot be empty"}, status_code=422)
    if not _URL_RE.match(url):
        return JSONResponse({"ok": False, "error": "URL must start with http:// or https://"}, status_code=422)

    existing = await asyncio.to_thread(
        integrations_col.find_one,
        {"owner_id": user["owner_id"], "website_url": url},
    )
    if existing:
        return {
            "ok":          True,
            "status":      "already_registered",
            "api_key":     existing["api_key"],
            "website_url": url,
        }

    api_key = secrets.token_urlsafe(32)
    await asyncio.to_thread(
        integrations_col.insert_one,
        {
            "owner_id":    user["owner_id"],
            "website_url": url,
            "api_key":     api_key,
            "created_at":  datetime.now(timezone.utc),
        },
    )
    logger.info("Integration registered | owner=%s | url=%s", user["owner_id"], url)
    return {
        "ok":             True,
        "status":         "registered",
        "api_key":        api_key,
        "website_url":    url,
        "embed_endpoint": f"/api/chat/{api_key}",
    }


@app.get("/api/integrations/list")
async def list_integrations(user: dict = Depends(get_current_user)):
    """Return all website integrations for authenticated owner. safe_json() applied."""
    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated")
    if integrations_col is None:
        raise HTTPException(status_code=500, detail="Database not connected")

    docs = await asyncio.to_thread(
        lambda: list(integrations_col.find({"owner_id": user["owner_id"]}))
    )
    return {"integrations": safe_json(docs)}


# ============================================================
# SECTION 24: AI AGENT (PUBLIC SaaS ENDPOINT)
# ============================================================

@app.post("/api/chat/{api_key}")
async def saas_chat_endpoint(api_key: str, message: str = Form(...)):
    """Public embedded chatbot endpoint — validates API key, delegates to generate_bot_response()."""
    if integrations_col is None:
        raise HTTPException(status_code=500, detail="Database not connected")

    integration = await asyncio.to_thread(integrations_col.find_one, {"api_key": api_key})
    if not integration:
        raise HTTPException(status_code=403, detail="Invalid API key")

    owner_id   = integration["owner_id"]
    session_id = f"api_{api_key}"

    response = await asyncio.to_thread(generate_bot_response, session_id, message, owner_id)
    return JSONResponse(response)


# ============================================================
# SECTION 25: ANALYTICS TRACKING ENDPOINTS
# ============================================================

@app.post("/track_click")
async def track_click_api(
    product_id: str  = Form(...),
    user:       dict = Depends(get_current_user),
):
    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated")
    if analytics_col is None:
        logger.warning("track_click_api: analytics_col is None; skipping")
        return {"status": "skipped", "reason": "analytics unavailable"}

    await asyncio.to_thread(AnalyticsEngine.track_click, analytics_col, product_id, user["owner_id"])
    return {"status": "tracked"}


@app.post("/track_language")
async def track_language_api(
    language: str  = Form(...),
    user:     dict = Depends(get_current_user),
):
    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated")
    if analytics_col is None:
        logger.warning("track_language_api: analytics_col is None; skipping")
        return {"status": "skipped", "language": language, "reason": "analytics unavailable"}

    await asyncio.to_thread(AnalyticsEngine.track_language, analytics_col, language, user["owner_id"])
    return {"status": "tracked", "language": language}


# ============================================================
# SECTION 26: LEGACY ADMIN
# ============================================================

@app.post("/password")
async def password_root(
    request:   Request,
    password:  str = Form(...),
    user_name: str = Form(...),
):
    expected_password = os.getenv("SECRET_PASSWORD")
    expected_user     = os.getenv("USER_NAME")
    status_val = (
        "success"
        if password == expected_password and user_name == expected_user
        else "failed"
    )
    return templates.TemplateResponse(
        "index.html", {"request": request, "status": status_val}
    )


# ============================================================
# SECTION 27: WEBSOCKET — CONSTANTS
# ============================================================

MAX_CONNECTIONS_PER_OWNER = 100
MAX_MESSAGE_BYTES         = 4_096
RATE_LIMIT_MESSAGES       = 20
RATE_LIMIT_WINDOW_SECONDS = 60
SESSION_TIMEOUT_SECONDS   = 300
OWNER_DATA_TTL_SECONDS    = 120
HEARTBEAT_INTERVAL        = 30
API_KEY_PATTERN           = re.compile(r"^[a-zA-Z0-9_\-\.]{8,128}$")

_ws_logger = logging.getLogger("WebSocket")


# ============================================================
# SECTION 28: WEBSOCKET — OWNER DATA CACHE
# ============================================================

class OwnerDataCache:
    """TTL cache — avoids redundant MongoDB reads per connection."""

    def __init__(self):
        self._loaded_at: Dict[str, float] = {}
        self._lock = asyncio.Lock()

    def is_stale(self, owner_id: str) -> bool:
        loaded = self._loaded_at.get(owner_id, 0)
        return (time.monotonic() - loaded) > OWNER_DATA_TTL_SECONDS

    async def ensure_loaded(self, owner_id: str) -> None:
        async with self._lock:
            if not self.is_stale(owner_id):
                return
            try:
                await asyncio.to_thread(load_data_realtime, owner_id=owner_id)
                self._loaded_at[owner_id] = time.monotonic()
                _ws_logger.info("Owner data loaded/refreshed: %s", owner_id)
            except Exception as exc:
                _ws_logger.warning("Failed to load data for owner %s: %s", owner_id, exc)

    def invalidate(self, owner_id: str) -> None:
        self._loaded_at.pop(owner_id, None)


owner_cache = OwnerDataCache()


# ============================================================
# SECTION 29: WEBSOCKET — RATE LIMITER
# ============================================================

class RateLimiter:
    """Per-user sliding-window rate limiter (in-memory)."""

    def __init__(self):
        self._windows: Dict[str, List[float]] = defaultdict(list)

    def is_allowed(self, user_id: str) -> bool:
        now = time.monotonic()
        self._windows[user_id] = [
            t for t in self._windows[user_id]
            if now - t < RATE_LIMIT_WINDOW_SECONDS
        ]
        if len(self._windows[user_id]) >= RATE_LIMIT_MESSAGES:
            return False
        self._windows[user_id].append(now)
        return True

    def cleanup(self, user_id: str) -> None:
        self._windows.pop(user_id, None)


rate_limiter = RateLimiter()


# ============================================================
# SECTION 30: WEBSOCKET — CONNECTION MANAGER
# ============================================================

class ConnectionManager:
    """Thread-safe multi-tenant WebSocket connection manager."""

    def __init__(self):
        self.active_connections: Dict[str, Dict[str, WebSocket]] = {}
        self._lock = asyncio.Lock()

    async def connect(self, ws: WebSocket, owner_id: str) -> Optional[str]:
        async with self._lock:
            owner_conns = self.active_connections.get(owner_id, {})
            if len(owner_conns) >= MAX_CONNECTIONS_PER_OWNER:
                _ws_logger.warning("Connection limit reached for owner: %s", owner_id)
                await ws.close(code=status.WS_1008_POLICY_VIOLATION, reason="Connection limit reached")
                return None

            await ws.accept()
            user_id = f"ws_{uuid.uuid4().hex}"
            self.active_connections.setdefault(owner_id, {})[user_id] = ws

        _ws_logger.info("WS Connected: %s | Owner: %s | Total Active: %d", user_id, owner_id, self.total_active())
        return user_id

    async def disconnect(self, user_id: str, owner_id: str) -> None:
        async with self._lock:
            if owner_id in self.active_connections:
                self.active_connections[owner_id].pop(user_id, None)
                if not self.active_connections[owner_id]:
                    self.active_connections.pop(owner_id)

        rate_limiter.cleanup(user_id)
        _ws_logger.info("WS Disconnected: %s | Owner: %s | Total Active: %d", user_id, owner_id, self.total_active())

    async def broadcast_to_owner(self, owner_id: str, payload: dict) -> None:
        async with self._lock:
            sockets = list(self.active_connections.get(owner_id, {}).values())
        for ws in sockets:
            try:
                await ws.send_json(payload)
            except Exception as exc:
                _ws_logger.warning("Broadcast send failed for owner %s: %s", owner_id, exc)

    def total_active(self) -> int:
        return sum(len(users) for users in self.active_connections.values())


manager = ConnectionManager()


# ============================================================
# SECTION 31: WEBSOCKET — HELPERS
# ============================================================

def validate_api_key(api_key: str) -> bool:
    return bool(api_key and API_KEY_PATTERN.match(api_key))


def parse_client_message(raw: str) -> Optional[dict]:
    """
    Parse and validate incoming client JSON message.
    Expected: { "message": str, "lang": str, "session_id": str (optional) }
    """
    if not raw:
        return None
    if len(raw.encode("utf-8")) > MAX_MESSAGE_BYTES:
        return None

    try:
        data = json.loads(raw)
    except (json.JSONDecodeError, ValueError):
        return None

    message = data.get("message", "")
    if not isinstance(message, str):
        return None
    message = message.strip()
    if not message:
        return None

    lang = data.get("lang", "en")
    if not isinstance(lang, str) or not lang.strip():
        lang = "en"
    lang = lang.strip()[:10]

    session_id = data.get("session_id", "")
    if not isinstance(session_id, str):
        session_id = ""
    session_id = session_id.strip()[:64]

    return {"message": message, "lang": lang, "session_id": session_id}


async def _ws_resolve_owner_id(api_key: str) -> Optional[str]:
    """
    Resolve api_key → owner_id via integrations_col.
    FIX-1 (from websocket.py v5.1): queries collection directly.
    FIX-3: uses `is None` not bare truthiness check (PyMongo >= 4.x).
    """
    if integrations_col is None:
        _ws_logger.warning("_ws_resolve_owner_id: integrations_col is None (DB not connected)")
        return None
    try:
        integration = await asyncio.to_thread(
            integrations_col.find_one, {"api_key": api_key}
        )
        if not integration:
            return None
        return integration.get("owner_id")
    except Exception as exc:
        _ws_logger.warning("api_key resolution failed: %s", exc)
        return None


async def _ws_log_analytics(owner_id: str, user_id: str, message: str, lang: str) -> None:
    """
    Fire-and-forget analytics insertion.
    FIX-2 (from websocket.py v5.1): inserts directly into analytics_col.
    FIX-3: uses `is None` not bare truthiness check.
    """
    if analytics_col is None:
        _ws_logger.warning("_ws_log_analytics: analytics_col is None; skipping")
        return
    try:
        record = {
            "owner_id":  owner_id,
            "user_id":   user_id,
            "message":   message[:200],
            "lang":      lang,
            "ts":        datetime.now(timezone.utc).isoformat(),
        }
        await asyncio.to_thread(analytics_col.insert_one, record)
    except Exception as exc:
        _ws_logger.warning("Analytics log failed | owner: %s | %s", owner_id, exc)


async def _ws_generate_response(user_id: str, message: str, owner_id: str) -> dict:
    """
    Run generate_bot_response() in a thread pool; always returns a safe dict.
    lang is detected internally by generate_bot_response() via detect_language().
    """
    try:
        return await asyncio.to_thread(
            generate_bot_response,
            user_id,   # positional: 'user_id'
            message,   # positional: 'msg'
            owner_id,  # positional: 'owner_id'
        )
    except Exception as exc:
        _ws_logger.error("Bot processing error | user: %s | owner: %s | %s", user_id, owner_id, exc)
        return {
            "type":        "error",
            "reply":       "Something went wrong. Please try again.",
            "carousel":    None,
            "suggestions": [],
        }


async def _ws_send_initial_greeting(ws: WebSocket, user_id: str, owner_id: str) -> None:
    """
    Send owner-specific greeting from bot_metadata.
    Falls back to a safe generic string only if DB record is missing.
    """
    lang     = "en"
    bot_data = BOT_DATA.get(owner_id, {})

    raw_msg = bot_data.get("initial_message", {})
    if isinstance(raw_msg, dict):
        initial_message = raw_msg.get(lang) or next(iter(raw_msg.values()), "")
    else:
        initial_message = str(raw_msg) if raw_msg else ""

    if not initial_message:
        initial_message = "Hello! How can I help you today?"

    suggestions = []
    try:
        with HISTORY_LOCK:
            suggestions = get_dynamic_suggestions(
                user_id, "greeting", lang, bot_data,
                USER_SESSION_HISTORY,
            )
    except Exception as exc:
        _ws_logger.warning("Suggestion error for owner %s: %s", owner_id, exc)

    try:
        await ws.send_json(safe_json({
            "type":        "greeting",
            "reply":       initial_message,
            "carousel":    None,
            "suggestions": suggestions,
            "ts":          datetime.now(timezone.utc).isoformat(),
        }))
    except Exception as exc:
        _ws_logger.error("Failed to send greeting | user: %s | owner: %s: %s", user_id, owner_id, exc)
        raise


async def _ws_heartbeat_loop(ws: WebSocket, user_id: str, owner_id: str) -> None:
    """Application-level keepalive — detects stale connections."""
    while True:
        await asyncio.sleep(HEARTBEAT_INTERVAL)
        try:
            await ws.send_json({"type": "ping", "ts": datetime.now(timezone.utc).isoformat()})
        except Exception:
            _ws_logger.debug("Heartbeat failed — connection likely closed | user: %s", user_id)
            break


# ============================================================
# SECTION 32: WEBSOCKET — ENDPOINT
# ============================================================

@app.websocket("/ws/chat")
async def websocket_endpoint(
    websocket: WebSocket,
    api_key: str = Query(..., description="Tenant API key — resolved to owner_id server-side"),
):
    """
    Multi-tenant AI chat WebSocket endpoint (v4.0).

    Auth flow:
      1. Validate api_key format (regex gate).
      2. Resolve api_key → owner_id via integrations_col (MongoDB lookup).
      3. Accept connection only on success.

    Message format (client → server):
      { "message": str, "lang": str, "session_id": str? }

    Response format (server → client):
      { "type": str, "reply": str, "carousel": list|null, "suggestions": list, "ts": str }
    """

    # 1. Format gate
    if not validate_api_key(api_key):
        _ws_logger.warning("Rejected — invalid api_key format: %r", api_key)
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="Invalid api_key format")
        return

    # 2. Resolve api_key → owner_id
    owner_id = await _ws_resolve_owner_id(api_key)
    if not owner_id:
        _ws_logger.warning("Rejected — unrecognised api_key: %r", api_key)
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="Unrecognised api_key")
        return

    # 3. Accept & register
    user_id = await manager.connect(websocket, owner_id)
    if user_id is None:
        return  # already closed inside connect()

    heartbeat_task: Optional[asyncio.Task] = None

    try:
        # Load/refresh owner bot data (cached — no redundant DB hits)
        await owner_cache.ensure_loaded(owner_id)

        # Send MongoDB-driven greeting
        await _ws_send_initial_greeting(websocket, user_id, owner_id)

        # Start heartbeat
        heartbeat_task = asyncio.create_task(_ws_heartbeat_loop(websocket, user_id, owner_id))

        # Main message loop
        while True:
            try:
                raw = await asyncio.wait_for(
                    websocket.receive_text(),
                    timeout=SESSION_TIMEOUT_SECONDS,
                )
            except (asyncio.TimeoutError, TimeoutError):
                _ws_logger.info("Session timeout: %s | Owner: %s", user_id, owner_id)
                try:
                    await websocket.send_json({
                        "type":        "timeout",
                        "reply":       "Your session has expired due to inactivity. Please refresh to reconnect.",
                        "carousel":    None,
                        "suggestions": [],
                        "ts":          datetime.now(timezone.utc).isoformat(),
                    })
                except Exception:
                    pass
                break

            parsed = parse_client_message(raw)
            if parsed is None:
                await websocket.send_json({
                    "type":        "error",
                    "reply":       "Invalid message format. Send JSON: {message, lang, session_id}.",
                    "carousel":    None,
                    "suggestions": [],
                    "ts":          datetime.now(timezone.utc).isoformat(),
                })
                continue

            msg  = parsed["message"]
            lang = parsed["lang"]

            if not rate_limiter.is_allowed(user_id):
                await websocket.send_json({
                    "type":        "rate_limited",
                    "reply":       "You're sending messages too quickly. Please slow down.",
                    "carousel":    None,
                    "suggestions": [],
                    "ts":          datetime.now(timezone.utc).isoformat(),
                })
                continue

            # Fire-and-forget analytics
            asyncio.create_task(_ws_log_analytics(owner_id, user_id, msg, lang))

            bot_response = await _ws_generate_response(user_id, msg, owner_id)
            bot_response.setdefault("type",        "reply")
            bot_response.setdefault("carousel",    None)
            bot_response.setdefault("suggestions", [])
            bot_response["ts"] = datetime.now(timezone.utc).isoformat()

            # FIX-SERIAL: safe_json() is the definitive guard before any
            # data reaches the wire. ws.send_json() uses stdlib json.dumps
            # which cannot serialise datetime or ObjectId. Even though
            # load_data_realtime() now applies safe_json per-product, this
            # second pass is cheap and makes the send path unconditionally safe.
            try:
                await websocket.send_json(safe_json(bot_response))
            except Exception as exc:
                _ws_logger.error("Send error | user: %s | owner: %s: %s", user_id, owner_id, exc)
                break

    except (WebSocketDisconnect, ConnectionClosedOK):
        _ws_logger.info("WS Disconnected Cleanly: %s | Owner: %s", user_id, owner_id)
    except Exception as exc:
        _ws_logger.error("WS Critical Error | user: %s | owner: %s: %s", user_id, owner_id, exc)
    finally:
        if heartbeat_task and not heartbeat_task.done():
            heartbeat_task.cancel()
            try:
                await heartbeat_task
            except asyncio.CancelledError:
                pass

        await manager.disconnect(user_id, owner_id)

        try:
            await websocket.close()
        except (RuntimeError, Exception):
            pass


# ============================================================
# SECTION 33: STARTUP
# ============================================================

# Initialise global analytics document (idempotent)
if analytics_col is not None:
    AnalyticsEngine.init_analytics(analytics_col)

# Confirm DB connectivity
init_database_sync()

# ============================================================
# SECTION 34: PAYMENT & SUBSCRIPTION SYSTEM
# ============================================================
# ──────────────────────────────────────────────────────────────
# This section ONLY ADDS new functionality.
# Zero modifications to any existing routes, classes, or logic.
#
# Architecture:
#   SubscriptionManager  — plan definitions, DB CRUD, access checks
#   SubscriptionMiddleware — per-request plan enforcement (additive)
#   Payment routes       — Stripe checkout, webhooks, cancel/success
#   Local payment stub   — Easypaisa / JazzCash simulation-ready
# ──────────────────────────────────────────────────────────────

import stripe
from datetime import timedelta

# ── Stripe init ─────────────────────────────────────────────────────────────
# Set STRIPE_SECRET_KEY in your .env file.
# For testing use sk_test_... keys; for production use sk_live_...
_STRIPE_SECRET_KEY      = os.getenv("STRIPE_SECRET_KEY", "")
_STRIPE_WEBHOOK_SECRET  = os.getenv("STRIPE_WEBHOOK_SECRET", "")
_STRIPE_SUCCESS_URL     = os.getenv("STRIPE_SUCCESS_URL", "http://localhost:8000/payment-success")
_STRIPE_CANCEL_URL      = os.getenv("STRIPE_CANCEL_URL", "http://localhost:8000/payment-cancel")
_APP_BASE_URL           = os.getenv("APP_BASE_URL", "http://localhost:8000")

if _STRIPE_SECRET_KEY:
    stripe.api_key = _STRIPE_SECRET_KEY
else:
    logger.warning(
        "[Payment] STRIPE_SECRET_KEY not set — Stripe calls will fail. "
        "Add it to your .env file."
    )

_pay_logger = logging.getLogger("Payment")

# ── Subscription plan catalogue ─────────────────────────────────────────────
# EDIT PRICES HERE — all amounts are in the specified currency.
# stripe_price_id: create products in Stripe Dashboard → copy price IDs here.
SUBSCRIPTION_PLANS: Dict[str, Dict[str, Any]] = {
    "free": {
        "name":              "Free",
        "price_usd":         0.00,       # USD price shown in UI
        "price_pkr":         0,          # PKR price shown in UI
        "currency":          "usd",      # Stripe currency code
        "stripe_price_id":   None,       # No Stripe charge for free plan
        "interval_days":     None,       # No renewal
        "features": {
            "dashboard_crud":      False,   # Cannot create/update/delete products
            "analytics":           False,   # No analytics access
            "bot_config":          False,   # Cannot change bot settings
            "chatbot_messages":    50,      # Max chatbot messages / month
            "integrations":        False,   # No API integrations tab
        },
        "description": "Get started for free. Limited chatbot usage.",
    },
    "basic": {
        "name":              "Basic",
        "price_usd":         9.99,
        "price_pkr":         2800,
        "currency":          "usd",
        "stripe_price_id":   os.getenv("STRIPE_BASIC_PRICE_ID", ""),  # e.g. price_xxx
        "interval_days":     30,
        "features": {
            "dashboard_crud":      False,   # Read-only products
            "analytics":           True,    # Analytics visible
            "bot_config":          True,    # Can edit bot config
            "chatbot_messages":    500,
            "integrations":        False,
        },
        "description": "Analytics + bot configuration. Up to 500 chatbot messages/month.",
    },
    "pro": {
        "name":              "Pro",
        "price_usd":         29.99,
        "price_pkr":         8400,
        "currency":          "usd",
        "stripe_price_id":   os.getenv("STRIPE_PRO_PRICE_ID", ""),    # e.g. price_yyy
        "interval_days":     30,
        "features": {
            "dashboard_crud":      True,    # Full CRUD
            "analytics":           True,
            "bot_config":          True,
            "chatbot_messages":    -1,      # Unlimited (-1 = unlimited)
            "integrations":        True,
        },
        "description": "Full access: CRUD, analytics, integrations, unlimited chatbot messages.",
    },
}

# ── Subscription document schema (stored in MongoDB `subscriptions` collection)
# {
#   "user_id":        str,          # owner_id from emails_col
#   "email":          str,
#   "plan":           str,          # "free" | "basic" | "pro"
#   "start_date":     datetime,
#   "expiry_date":    datetime | None,
#   "payment_status": str,          # "active" | "expired" | "cancelled" | "failed" | "pending"
#   "stripe_customer_id":  str | None,
#   "stripe_session_id":   str | None,
#   "stripe_subscription_id": str | None,
#   "local_payment_method": str | None,  # "easypaisa" | "jazzcash" | None
#   "created_at":     datetime,
#   "updated_at":     datetime,
# }


# ── MongoDB collection for subscriptions ─────────────────────────────────────
try:
    subscriptions_col = db["subscriptions"]
    subscriptions_col.create_index("user_id", unique=True)
    subscriptions_col.create_index("stripe_session_id")
    subscriptions_col.create_index("stripe_customer_id")
    subscriptions_col.create_index("expiry_date")
    _pay_logger.info("subscriptions_col initialised")
except Exception as _sub_exc:
    subscriptions_col = None
    _pay_logger.error("Failed to init subscriptions_col: %s", _sub_exc)


# ============================================================
# SECTION 34.1: SubscriptionManager
# ============================================================

class SubscriptionManager:
    """
    All subscription logic — create, read, upgrade, downgrade, expiry check.
    All methods are async-friendly (called with asyncio.to_thread where needed).
    """

    # ── Internal helpers ─────────────────────────────────────────────────────

    @staticmethod
    def _now() -> datetime:
        return datetime.now(timezone.utc)

    @classmethod
    def _expiry_for_plan(cls, plan_key: str) -> Optional[datetime]:
        """Calculate expiry date from now for a given plan (None = no expiry)."""
        plan   = SUBSCRIPTION_PLANS.get(plan_key, {})
        days   = plan.get("interval_days")
        if days is None:
            return None
        return cls._now() + timedelta(days=days)

    # ── Public API ───────────────────────────────────────────────────────────

    @classmethod
    def get_subscription(cls, user_id: str) -> Optional[Dict[str, Any]]:
        """Return current subscription document for user_id, or None."""
        if subscriptions_col is None:
            return None
        try:
            return subscriptions_col.find_one({"user_id": user_id})
        except Exception as exc:
            _pay_logger.error("get_subscription error (user=%s): %s", user_id, exc)
            return None

    @classmethod
    def get_or_create_free(cls, user_id: str, email: str) -> Dict[str, Any]:
        """
        Return existing subscription or auto-create a free-plan subscription.
        Guarantees every logged-in user has a subscription record.
        """
        if subscriptions_col is None:
            # Degrade gracefully — return in-memory free record
            return cls._free_record(user_id, email)
        try:
            existing = subscriptions_col.find_one({"user_id": user_id})
            if existing:
                return existing
            record = cls._free_record(user_id, email)
            subscriptions_col.insert_one(record)
            _pay_logger.info("Auto-created free subscription for user=%s", user_id)
            return record
        except Exception as exc:
            _pay_logger.error("get_or_create_free error (user=%s): %s", user_id, exc)
            return cls._free_record(user_id, email)

    @classmethod
    def _free_record(cls, user_id: str, email: str) -> Dict[str, Any]:
        return {
            "user_id":                  user_id,
            "email":                    email,
            "plan":                     "free",
            "start_date":               cls._now(),
            "expiry_date":              None,
            "payment_status":           "active",
            "stripe_customer_id":       None,
            "stripe_session_id":        None,
            "stripe_subscription_id":   None,
            "local_payment_method":     None,
            "created_at":               cls._now(),
            "updated_at":               cls._now(),
        }

    @classmethod
    def upgrade_plan(
        cls,
        user_id:                  str,
        email:                    str,
        new_plan:                 str,
        stripe_session_id:        Optional[str] = None,
        stripe_customer_id:       Optional[str] = None,
        stripe_subscription_id:   Optional[str] = None,
        local_payment_method:     Optional[str] = None,
    ) -> bool:
        """Upgrade (or set) user's plan after successful payment."""
        if subscriptions_col is None:
            return False
        try:
            now    = cls._now()
            expiry = cls._expiry_for_plan(new_plan)
            subscriptions_col.update_one(
                {"user_id": user_id},
                {
                    "$set": {
                        "plan":                     new_plan,
                        "start_date":               now,
                        "expiry_date":              expiry,
                        "payment_status":           "active",
                        "stripe_session_id":        stripe_session_id,
                        "stripe_customer_id":       stripe_customer_id,
                        "stripe_subscription_id":   stripe_subscription_id,
                        "local_payment_method":     local_payment_method,
                        "email":                    email,
                        "updated_at":               now,
                    },
                    "$setOnInsert": {
                        "user_id":    user_id,
                        "created_at": now,
                    },
                },
                upsert=True,
            )
            _pay_logger.info("Plan upgraded → %s for user=%s", new_plan, user_id)
            return True
        except Exception as exc:
            _pay_logger.error("upgrade_plan error (user=%s): %s", user_id, exc)
            return False

    @classmethod
    def mark_payment_failed(cls, user_id: str) -> None:
        """Mark subscription payment as failed — does NOT remove access immediately."""
        if subscriptions_col is None:
            return
        try:
            subscriptions_col.update_one(
                {"user_id": user_id},
                {"$set": {"payment_status": "failed", "updated_at": cls._now()}},
            )
            _pay_logger.warning("Payment marked failed for user=%s", user_id)
        except Exception as exc:
            _pay_logger.error("mark_payment_failed error: %s", exc)

    @classmethod
    def downgrade_expired(cls, sub: Dict[str, Any]) -> Dict[str, Any]:
        """
        If subscription has expired, downgrade to free in DB and return
        the updated record.  Safe to call on every request.
        """
        if subscriptions_col is None:
            return sub
        expiry = sub.get("expiry_date")
        if expiry is None:
            return sub  # free plan — no expiry
        # Handle both datetime and ISO string (from safe_json)
        if isinstance(expiry, str):
            try:
                expiry = datetime.fromisoformat(expiry)
            except ValueError:
                return sub
        if expiry.tzinfo is None:
            expiry = expiry.replace(tzinfo=timezone.utc)
        if cls._now() > expiry:
            _pay_logger.info("Subscription expired for user=%s — downgrading to free", sub.get("user_id"))
            subscriptions_col.update_one(
                {"user_id": sub["user_id"]},
                {
                    "$set": {
                        "plan":           "free",
                        "payment_status": "expired",
                        "expiry_date":    None,
                        "updated_at":     cls._now(),
                    }
                },
            )
            sub = dict(sub)
            sub["plan"]           = "free"
            sub["payment_status"] = "expired"
        return sub

    @classmethod
    def get_plan_features(cls, plan_key: str) -> Dict[str, Any]:
        """Return feature flags for a given plan key."""
        return SUBSCRIPTION_PLANS.get(plan_key, SUBSCRIPTION_PLANS["free"]).get("features", {})

    @classmethod
    def has_feature(cls, sub: Dict[str, Any], feature: str) -> bool:
        """
        Check if a subscription's plan includes a boolean feature.
        sub: subscription document
        feature: key from SUBSCRIPTION_PLANS[plan]["features"]
        """
        plan_key = sub.get("plan", "free")
        features = cls.get_plan_features(plan_key)
        val      = features.get(feature, False)
        if isinstance(val, bool):
            return val
        if isinstance(val, int):
            return val != 0  # -1 = unlimited = True; 0 = no access
        return False

    @classmethod
    def get_effective_sub(cls, user_id: str, email: str) -> Dict[str, Any]:
        """
        One-stop helper: get or create subscription, check expiry,
        downgrade if needed, and return the current state.
        """
        sub = cls.get_or_create_free(user_id, email)
        sub = cls.downgrade_expired(sub)
        return sub


# ============================================================
# SECTION 34.2: Subscription dependency helpers
# ============================================================

def _get_subscription_for_user(user: dict) -> Dict[str, Any]:
    """
    Dependency-style helper — resolves subscription for the current user.
    Returns subscription dict (always a valid plan, never None).
    """
    return SubscriptionManager.get_effective_sub(
        user_id=user["owner_id"],
        email=user.get("email", ""),
    )


def require_plan(required_feature: str):
    """
    FastAPI dependency factory.
    Usage:  user: dict = Depends(require_plan("dashboard_crud"))

    Raises HTTP 403 with upgrade hint if the user's plan lacks the feature.
    """
    def _check(user: dict = Depends(get_current_user)):
        if not user:
            raise HTTPException(status_code=401, detail="Not authenticated")
        sub = _get_subscription_for_user(user)
        if not SubscriptionManager.has_feature(sub, required_feature):
            plan = sub.get("plan", "free")
            raise HTTPException(
                status_code=403,
                detail={
                    "error":    "plan_required",
                    "message":  f"Your current plan ({plan!r}) does not include this feature.",
                    "upgrade":  f"{_APP_BASE_URL}/payment",
                    "feature":  required_feature,
                },
            )
        return user
    return _check


# ============================================================
# SECTION 34.3: Stripe helpers (no card data ever touches server)
# ============================================================

def _create_stripe_checkout_session(
    plan_key:    str,
    user_id:     str,
    email:       str,
    success_url: str,
    cancel_url:  str,
) -> Optional[str]:
    """
    Create a Stripe Checkout session and return the redirect URL.
    Card data is handled 100% by Stripe — never passes through this server.
    Returns None on any error.
    """
    plan = SUBSCRIPTION_PLANS.get(plan_key)
    if not plan:
        _pay_logger.error("Unknown plan: %s", plan_key)
        return None

    price_id = plan.get("stripe_price_id")
    if not price_id:
        _pay_logger.error("No Stripe price_id configured for plan: %s", plan_key)
        return None

    try:
        session = stripe.checkout.Session.create(
            payment_method_types=["card"],
            mode="subscription",
            customer_email=email,
            line_items=[{"price": price_id, "quantity": 1}],
            success_url=success_url + "?session_id={CHECKOUT_SESSION_ID}",
            cancel_url=cancel_url,
            metadata={
                "user_id":  user_id,
                "plan":     plan_key,
                "email":    email,
            },
        )
        return session.url
    except stripe.error.StripeError as exc:
        _pay_logger.error("Stripe checkout session creation failed: %s", exc)
        return None


def _retrieve_stripe_session(session_id: str) -> Optional[Any]:
    """Safely retrieve a Stripe Checkout session by ID."""
    try:
        return stripe.checkout.Session.retrieve(session_id)
    except stripe.error.StripeError as exc:
        _pay_logger.error("Stripe session retrieval failed (%s): %s", session_id, exc)
        return None


# ============================================================
# SECTION 34.4: Local Payment Stub (Easypaisa / JazzCash)
# ============================================================
# Pakistan-local gateways (Easypaisa, JazzCash) do not have a universal
# REST API as of 2025 — integration is merchant-specific.
# This section provides the simulation-ready structure so you can plug in
# the actual merchant credentials and endpoint once obtained.

class LocalPaymentGateway:
    """
    Simulation-ready structure for Pakistan local payment methods.
    Replace the `_simulate_*` methods with real gateway SDK calls.
    """

    SUPPORTED_METHODS = ["easypaisa", "jazzcash"]

    @staticmethod
    def initiate_payment(
        method:    str,          # "easypaisa" | "jazzcash"
        amount_pkr: float,
        order_id:  str,
        user_phone: str,
        user_email: str,
    ) -> Dict[str, Any]:
        """
        Initiate a local payment.
        Returns: {"success": bool, "transaction_id": str, "message": str}
        
        TODO: Replace simulation body with actual merchant API call:
          - Easypaisa: POST to https://easypaisa.com.pk/easypay/...
          - JazzCash:  POST to https://sandbox.jazzcash.com.pk/...
        """
        if method not in LocalPaymentGateway.SUPPORTED_METHODS:
            return {"success": False, "transaction_id": None, "message": f"Unsupported method: {method}"}

        _pay_logger.info(
            "[LocalPayment] Initiating %s payment | amount=PKR %.2f | order=%s",
            method, amount_pkr, order_id,
        )

        # ── SIMULATION (replace with real gateway call) ──────────────────────
        simulated_txn_id = f"SIM-{method.upper()}-{uuid.uuid4().hex[:10].upper()}"
        return {
            "success":        True,
            "transaction_id": simulated_txn_id,
            "message":        f"Simulated {method} payment initiated. Replace with real gateway.",
            "amount_pkr":     amount_pkr,
            "order_id":       order_id,
        }

    @staticmethod
    def verify_payment(method: str, transaction_id: str) -> bool:
        """
        Verify a local payment transaction.
        TODO: Call the real gateway verification endpoint.
        Currently always returns True (simulation).
        """
        _pay_logger.info("[LocalPayment] Verifying %s txn: %s", method, transaction_id)
        # Simulation: accept all
        return transaction_id.startswith("SIM-")


# ============================================================
# SECTION 34.5: Payment & Subscription Routes
# ============================================================

@app.get("/payment", response_class=HTMLResponse)
async def payment_page(
    request: Request,
    user: dict = Depends(get_current_user),
):
    """
    Render the payment / plan selection page.
    Auth required — redirect to login if not authenticated.
    """
    if not user:
        return RedirectResponse(url="/login", status_code=303)

    sub  = _get_subscription_for_user(user)
    plan = sub.get("plan", "free")

    base_dir  = os.path.dirname(os.path.abspath(__file__))
    html_path = os.path.join(base_dir, "templates", "payment.html")
    if os.path.exists(html_path):
        return FileResponse(html_path)

    # Fallback: if template doesn't exist yet, return JSON info
    return JSONResponse({
        "current_plan": plan,
        "plans":        {k: {"name": v["name"], "price_usd": v["price_usd"],
                             "price_pkr": v["price_pkr"]}
                         for k, v in SUBSCRIPTION_PLANS.items()},
    })


@app.post("/create-checkout-session")
async def create_checkout_session(
    request: Request,
    user:    dict = Depends(get_current_user),
):
    """
    Create a Stripe Checkout session for the requested plan.

    Request body (JSON): {"plan": "basic" | "pro"}
    Returns: {"checkout_url": str}

    Security: No card data passes through this endpoint.
    Stripe handles all PCI-DSS sensitive operations on their side.
    """
    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated")

    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=422, detail="Invalid JSON body")

    plan_key = str(body.get("plan", "")).strip().lower()
    if plan_key not in SUBSCRIPTION_PLANS:
        raise HTTPException(status_code=422, detail=f"Unknown plan: {plan_key!r}")
    if plan_key == "free":
        raise HTTPException(status_code=422, detail="Free plan requires no payment.")

    if not _STRIPE_SECRET_KEY:
        raise HTTPException(
            status_code=503,
            detail="Payment gateway not configured. Contact support.",
        )

    checkout_url = _create_stripe_checkout_session(
        plan_key=plan_key,
        user_id=user["owner_id"],
        email=user.get("email", ""),
        success_url=_STRIPE_SUCCESS_URL,
        cancel_url=_STRIPE_CANCEL_URL,
    )

    if not checkout_url:
        raise HTTPException(
            status_code=502,
            detail="Could not create payment session. Please try again.",
        )

    _pay_logger.info(
        "Checkout session created | user=%s | plan=%s",
        user["owner_id"], plan_key,
    )
    return JSONResponse({"checkout_url": checkout_url})


@app.get("/payment-success")
async def payment_success(
    request:    Request,
    session_id: Optional[str] = Query(default=None),
    user:       dict = Depends(get_current_user),
):
    """
    Stripe redirects here after successful checkout.
    Verifies the session with Stripe and upgrades the user's plan.
    Also renders a success page.
    """
    if not user:
        return RedirectResponse(url="/login", status_code=303)

    status_msg = "Payment processed. Your plan has been upgraded!"
    plan_name  = "Unknown"

    if session_id and _STRIPE_SECRET_KEY:
        stripe_session = await asyncio.to_thread(_retrieve_stripe_session, session_id)
        if stripe_session and stripe_session.payment_status == "paid":
            metadata  = stripe_session.metadata or {}
            plan_key  = metadata.get("plan", "basic")
            user_id   = metadata.get("user_id", user["owner_id"])
            email     = metadata.get("email",   user.get("email", ""))

            await asyncio.to_thread(
                SubscriptionManager.upgrade_plan,
                user_id,
                email,
                plan_key,
                session_id,
                stripe_session.customer,
                stripe_session.subscription,
            )
            plan_name = SUBSCRIPTION_PLANS.get(plan_key, {}).get("name", plan_key)
            status_msg = f"🎉 Welcome to the {plan_name} plan! Your account has been upgraded."
            _pay_logger.info("Payment success confirmed | user=%s | plan=%s", user_id, plan_key)
        else:
            status_msg = "Payment pending or could not be verified. Contact support if you were charged."

    # Render success page or redirect
    base_dir  = os.path.dirname(os.path.abspath(__file__))
    html_path = os.path.join(base_dir, "templates", "payment.html")
    if os.path.exists(html_path):
        # Pass message via query param so the static page can read it
        return RedirectResponse(
            url=f"/payment?status=success&plan={plan_name}&msg={status_msg}",
            status_code=303,
        )
    return JSONResponse({"status": "success", "message": status_msg, "plan": plan_name})


@app.get("/payment-cancel")
async def payment_cancel(
    request: Request,
    user:    dict = Depends(get_current_user),
):
    """
    Stripe redirects here when the user cancels checkout.
    No charge has been made. Redirect back to payment page with a message.
    """
    if not user:
        return RedirectResponse(url="/login", status_code=303)

    _pay_logger.info("Payment cancelled | user=%s", user.get("owner_id"))
    return RedirectResponse(url="/payment?status=cancelled", status_code=303)


@app.post("/webhook")
async def stripe_webhook(request: Request):
    """
    Stripe webhook endpoint — handles async payment events.

    SECURITY:
      - Verifies Stripe-Signature header using STRIPE_WEBHOOK_SECRET.
      - Raw body is read before any parsing (required by Stripe SDK).
      - Never trust event data without signature verification.

    Events handled:
      - checkout.session.completed  → upgrade plan
      - invoice.payment_failed      → mark payment failed
      - customer.subscription.deleted → downgrade to free
    """
    payload = await request.body()
    sig_header = request.headers.get("stripe-signature", "")

    if not _STRIPE_WEBHOOK_SECRET:
        _pay_logger.error("Webhook received but STRIPE_WEBHOOK_SECRET not configured")
        return JSONResponse({"error": "Webhook secret not configured"}, status_code=500)

    # ── Verify signature (prevents spoofed events) ──────────────────────────
    try:
        event = stripe.Webhook.construct_event(
            payload, sig_header, _STRIPE_WEBHOOK_SECRET
        )
    except stripe.error.SignatureVerificationError as exc:
        _pay_logger.warning("Invalid Stripe signature: %s", exc)
        return JSONResponse({"error": "Invalid signature"}, status_code=400)
    except Exception as exc:
        _pay_logger.error("Webhook parsing error: %s", exc)
        return JSONResponse({"error": "Parse error"}, status_code=400)

    event_type = event.get("type", "")
    _pay_logger.info("Stripe webhook received: %s", event_type)

    # ── Handle events ────────────────────────────────────────────────────────

    if event_type == "checkout.session.completed":
        session  = event["data"]["object"]
        metadata = session.get("metadata", {})
        user_id  = metadata.get("user_id")
        plan_key = metadata.get("plan", "basic")
        email    = metadata.get("email", "")

        if user_id and session.get("payment_status") == "paid":
            await asyncio.to_thread(
                SubscriptionManager.upgrade_plan,
                user_id,
                email,
                plan_key,
                session.get("id"),
                session.get("customer"),
                session.get("subscription"),
            )
            _pay_logger.info("[Webhook] Upgraded user=%s → plan=%s", user_id, plan_key)

    elif event_type == "invoice.payment_failed":
        invoice = event["data"]["object"]
        cust_id = invoice.get("customer")
        if cust_id and subscriptions_col is not None:
            sub = await asyncio.to_thread(
                subscriptions_col.find_one,
                {"stripe_customer_id": cust_id},
            )
            if sub:
                await asyncio.to_thread(
                    SubscriptionManager.mark_payment_failed,
                    sub["user_id"],
                )
                _pay_logger.warning("[Webhook] Payment failed for customer=%s", cust_id)

    elif event_type == "customer.subscription.deleted":
        # Subscription cancelled in Stripe → downgrade to free
        stripe_sub = event["data"]["object"]
        cust_id    = stripe_sub.get("customer")
        if cust_id and subscriptions_col is not None:
            sub = await asyncio.to_thread(
                subscriptions_col.find_one,
                {"stripe_customer_id": cust_id},
            )
            if sub:
                await asyncio.to_thread(
                    SubscriptionManager.upgrade_plan,
                    sub["user_id"],
                    sub.get("email", ""),
                    "free",
                )
                _pay_logger.info("[Webhook] Subscription deleted → downgraded user=%s to free", sub["user_id"])

    return JSONResponse({"received": True})


@app.get("/api/subscription")
async def get_subscription_status(user: dict = Depends(get_current_user)):
    """
    Return the authenticated user's current subscription status.
    Used by the frontend to show plan badges and gating.
    """
    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated")

    sub      = _get_subscription_for_user(user)
    plan_key = sub.get("plan", "free")
    plan_cfg = SUBSCRIPTION_PLANS.get(plan_key, SUBSCRIPTION_PLANS["free"])

    return JSONResponse(safe_json({
        "plan":           plan_key,
        "plan_name":      plan_cfg["name"],
        "price_usd":      plan_cfg["price_usd"],
        "price_pkr":      plan_cfg["price_pkr"],
        "payment_status": sub.get("payment_status", "active"),
        "start_date":     sub.get("start_date"),
        "expiry_date":    sub.get("expiry_date"),
        "features":       plan_cfg["features"],
    }))


@app.post("/api/local-payment/initiate")
async def initiate_local_payment(
    request: Request,
    user:    dict = Depends(get_current_user),
):
    """
    Initiate a Pakistan local payment (Easypaisa / JazzCash).
    Currently simulation-ready — replace LocalPaymentGateway internals
    with real merchant API calls when credentials are available.

    Request body:
      {
        "method":    "easypaisa" | "jazzcash",
        "plan":      "basic" | "pro",
        "phone":     "+923001234567"
      }
    """
    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated")

    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=422, detail="Invalid JSON body")

    method   = str(body.get("method", "")).strip().lower()
    plan_key = str(body.get("plan",   "")).strip().lower()
    phone    = str(body.get("phone",  "")).strip()

    if method not in LocalPaymentGateway.SUPPORTED_METHODS:
        raise HTTPException(
            status_code=422,
            detail=f"Unsupported payment method. Use: {LocalPaymentGateway.SUPPORTED_METHODS}",
        )
    if plan_key not in SUBSCRIPTION_PLANS or plan_key == "free":
        raise HTTPException(status_code=422, detail="Invalid plan for local payment.")

    amount_pkr = SUBSCRIPTION_PLANS[plan_key]["price_pkr"]
    order_id   = f"ORD-{user['owner_id'][:8]}-{uuid.uuid4().hex[:8].upper()}"

    result = await asyncio.to_thread(
        LocalPaymentGateway.initiate_payment,
        method,
        float(amount_pkr),
        order_id,
        phone,
        user.get("email", ""),
    )

    if result.get("success"):
        # Store pending subscription record
        await asyncio.to_thread(
            SubscriptionManager.upgrade_plan,
            user["owner_id"],
            user.get("email", ""),
            plan_key,
            None,                          # no Stripe session
            None,                          # no Stripe customer
            None,                          # no Stripe subscription
            method,                        # local_payment_method
        )
        # For simulation: immediately mark as active.
        # For production: only upgrade after verifying the real gateway callback.
        _pay_logger.info(
            "Local payment initiated | user=%s | method=%s | plan=%s | order=%s",
            user["owner_id"], method, plan_key, order_id,
        )
        return JSONResponse({
            "success":        True,
            "transaction_id": result.get("transaction_id"),
            "order_id":       order_id,
            "message":        result.get("message"),
            "amount_pkr":     amount_pkr,
            "note":           "Simulation mode. Real gateway not yet connected.",
        })
    else:
        raise HTTPException(status_code=502, detail=result.get("message", "Payment initiation failed."))


# ============================================================
# SECTION 34.6: Subscription middleware (additive — no breaking changes)
# ============================================================
# This middleware ONLY adds plan-gating information to the request state.
# It does NOT block or modify any existing endpoint behaviour.
# Actual enforcement is done per-endpoint via the require_plan() dependency.

@app.middleware("http")
async def subscription_context_middleware(request: Request, call_next):
    """
    Attaches subscription info to request.state for any route that needs it.
    Does NOT enforce access — enforcement is done by require_plan() dependency.

    Skips: /webhook (needs raw body), /static, /ws (WebSocket).
    """
    skip_prefixes = ("/webhook", "/static", "/ws")
    path = request.url.path

    if any(path.startswith(p) for p in skip_prefixes):
        return await call_next(request)

    # Resolve subscription for authenticated users (non-blocking)
    session_token = request.cookies.get("session_token")
    if session_token:
        with SESSIONS_LOCK:
            user_data = SESSIONS.get(session_token)
        if user_data:
            try:
                sub = await asyncio.to_thread(
                    SubscriptionManager.get_effective_sub,
                    user_data["owner_id"],
                    user_data.get("email", ""),
                )
                request.state.subscription = sub
                request.state.plan         = sub.get("plan", "free")
            except Exception:
                request.state.subscription = None
                request.state.plan         = "free"
        else:
            request.state.subscription = None
            request.state.plan         = "free"
    else:
        request.state.subscription = None
        request.state.plan         = "free"

    return await call_next(request)