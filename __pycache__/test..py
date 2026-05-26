# -------------- RESTAURANT TABLE RESERVATIONS HANDLER BOT ------------------

from fastapi import FastAPI, Request, Form, HTTPException
from fastapi.responses import JSONResponse, HTMLResponse, PlainTextResponse

from fastapi.templating import Jinja2Templates
from pymongo import MongoClient, DESCENDING

from typing import List, Dict, Any, Optional
from dotenv import load_dotenv

from langdetect import detect, DetectorFactory
import logging, certifi, os, re, random, httpx, asyncio, json, time

from datetime import datetime
from bson import ObjectId

from collections import defaultdict
from difflib import SequenceMatcher

from fastapi.middleware.cors import CORSMiddleware

# ============================================================
# INITIAL SETUP 

load_dotenv()
DetectorFactory.seed = 0
logging.basicConfig(level="INFO", format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("Restaurantbot.v1.1")

BOT_RESERVATION_DATA: Dict[str, Any] = {}
RATE_LIMIT_PER_MINUTE = 15
TABLE_DATA: List[Dict[str, Any]] = []
TABLE_KEYWORD_INDEX: Dict[str, List[Dict]] = {}
_rate_store: Dict[str, list] = defaultdict(list)
USER_SESSIONS: Dict[str, Dict[str, Any]] = {}

def _is_rate_limited(user_id: str) -> bool:
    now = time.time()
    timestamps = [t for t in _rate_store[user_id] if now - t < 60]
    _rate_store[user_id] = timestamps
    if len(timestamps) >= RATE_LIMIT_PER_MINUTE:
        return True
    _rate_store[user_id].append(now)
    return False

app = FastAPI(
    title="WhatsApp AI Restaurant Bot v1.1",
    version="1.1",
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_crendentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

templates= Jinja2Templates(directory="templates")

# ============================================================
# ENVIRONMENT VARIABLES
# ============================================================

MONGO_URL   = os.getenv("MONGO_URL", "")
USERNAME    = os.getenv("USERNAME", "")
PASSWORD    = os.getenv("PASSWORD", "")

# ========================================
#  -------------- ROUTES ----------------
# ==========================================

@app.get("/login")
def login():
    return {
       "AI RES BOT":
           "WEBSITES RESTAURANT BOT IS PERFECTLY RUNNING"
    }
    
def keywords(keyword: str = Form(...)):
    keywords = {
        "I Want to booked Table",
        ""
    }
    
@app.post("/reservation")
