# index.py
# ============================================================
# STELLAR STYLE — AI E-Commerce Chatbot Backend
# ============================================================

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, Form
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from pymongo import MongoClient

from dotenv import load_dotenv
import os
import logging
import re
import random
import asyncio
import certifi
from typing import List, Dict, Any, Optional
from bson import ObjectId

import analytics
from websocket import router as websocket_router

# ============================================================
# INITIAL SETUP
# ============================================================

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s"
)
logger = logging.getLogger("StellarStyle")

# ============================================================
# GLOBAL STATE
# ============================================================

BOT_DATA: Dict[str, Any] = {}
PRODUCTS_DATA: List[Dict[str, Any]] = []
USER_SESSION_HISTORY: Dict[str, Dict[str, Any]] = {}

# ============================================================
# FASTAPI APP
# ============================================================

app = FastAPI(
    title="Stellar Style — AI E-Commerce Chatbot",
    version="2.0",
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)

templates = Jinja2Templates(directory="templates")

# ============================================================
# DATABASE CONNECTION
# ============================================================

MONGO_URI = os.getenv("MONGO_URI", "")

products_col = None
meta_col = None
analytics_col = None

try:
    client = MongoClient(
        MONGO_URI,
        tls=True,
        tlsCAFile=certifi.where(),
        serverSelectionTimeoutMS=10000,
    )
    db = client["ecommerce"]
    products_col = db["products"]
    meta_col = db["bot_metadata"]
    analytics_col = db["analytics"]

    client.admin.command("ping")
    logger.info("MongoDB connected successfully.")

except Exception as e:
    logger.error(f"MongoDB connection failed: {e}")

# ============================================================
# LANGUAGE DETECTION & KEYWORD IMPORTS
# ============================================================

from keywords import (
    detect_language,
    COLOR_KEYWORDS,
    MATERIAL_KEYWORDS,
    CATEGORY_KEYWORDS,
    INTENT_KEYWORDS,
)

# ============================================================
# REALTIME DATA LOADER
# ============================================================

def load_data_realtime() -> None:
    """
    Pulls the latest products and bot config from MongoDB
    and syncs them into global memory.
    """
    global PRODUCTS_DATA, BOT_DATA

    if products_col is None or meta_col is None:
        logger.error("DB collections not initialized — skipping sync.")
        return

    try:
        # ---- PRODUCTS ----
        temp_products = []
        for product in products_col.find({}):
            if "_id" in product:
                product["_id"] = str(product["_id"])
            temp_products.append(product)
        PRODUCTS_DATA = temp_products

        # ---- BOT METADATA ----
        meta = meta_col.find_one({"type": "config"}) or meta_col.find_one({})

        if meta:
            if "_id" in meta:
                meta["_id"] = str(meta["_id"])
            BOT_DATA = meta
        else:
            BOT_DATA = {
                "supported_languages": ["en"],
                "initial_message": {"en": "Hello! How can I help you today?"},
                "faq": {},
                "smart_suggestions": {},
            }

        logger.info(f"Data sync complete — {len(PRODUCTS_DATA)} products loaded.")

    except Exception as e:
        logger.error(f"Data sync error: {e}")


def init_database_sync() -> None:
    """Triggers initial data load on server startup."""
    load_data_realtime()


# ============================================================
# SMART KEYWORD MATCHER
# ============================================================

def smart_match(text: str, keyword_dict: Dict[str, List[str]]) -> List[str]:
    """
    Returns a list of matched categories from the keyword dict
    based on substring matching against the lowercased query.
    """
    text = text.lower()
    return [
        category
        for category, synonyms in keyword_dict.items()
        if any(syn in text for syn in synonyms)
    ]


def process_user_query(query: str) -> Dict[str, Any]:
    """
    Parses a raw user query into structured intent signals.
    Returns language, matched colors, materials, categories, and intents.
    """
    return {
        "language": detect_language(query),
        "colors": smart_match(query, COLOR_KEYWORDS),
        "materials": smart_match(query, MATERIAL_KEYWORDS),
        "categories": smart_match(query, CATEGORY_KEYWORDS),
        "intents": smart_match(query, INTENT_KEYWORDS),
    }


# ============================================================
# PRICE RANGE PARSER
# ============================================================

def parse_price_range(query: str) -> Dict[str, float]:
    """
    Extracts price constraints from natural language.
    Supports: "under 100", "below $50", "above 200", "between 50 and 150".
    Handles PKR, EUR, USD symbols and multi-language cues.
    """
    # Normalize currency symbols
    q = (
        query.lower()
        .replace("$", "")
        .replace("€", "")
        .replace("rs ", "")
        .replace("pkr", "")
        .replace(",", "")
        .strip()
    )

    price_range: Dict[str, float] = {}

    # Between X and Y
    match_between = re.search(
        r"between\s+(\d+(?:\.\d+)?)\s+(?:and|to|-)\s+(\d+(?:\.\d+)?)", q
    )
    if match_between:
        try:
            price_range["min"] = float(match_between.group(1))
            price_range["max"] = float(match_between.group(2))
            return price_range
        except ValueError:
            pass

    # Under / below / less than
    match_under = re.search(
        r"(under|below|less than|cheaper than|کم از|weniger als|unter)\s*(\d+(?:\.\d+)?)", q
    )
    if match_under:
        try:
            price_range["max"] = float(match_under.group(2))
        except ValueError:
            pass

    # Over / above / more than
    match_over = re.search(
        r"(over|above|more than|greater than|زیادہ از|über|mehr als)\s*(\d+(?:\.\d+)?)", q
    )
    if match_over:
        try:
            price_range["min"] = float(match_over.group(2))
        except ValueError:
            pass

    return price_range


# ============================================================
# PRODUCT RELEVANCE SCORING
# ============================================================

def score_product_relevance(
    query: str,
    product: Dict[str, Any],
    price_range: Dict[str, float],
) -> float:
    """
    Assigns a relevance score to a product against the user's query.
    Factors in: keyword overlap, category/color/material matches,
    trending score, rating, and price range fit.
    """
    query_lower = query.lower()
    score = 0.0

    # Aggregate searchable product text
    field_text = " ".join(
        str(product.get(f, "")).lower()
        for f in ["title", "description", "color", "material", "category"]
    )

    # Word overlap score
    query_words = set(re.findall(r"\w+", query_lower))
    product_words = set(re.findall(r"\w+", field_text))
    score += len(query_words & product_words) * 0.8

    # Category keyword match
    for category, kws in CATEGORY_KEYWORDS.items():
        if any(kw in query_lower for kw in kws):
            if category in field_text:
                score += 3.5

    # Color keyword match
    for color, kws in COLOR_KEYWORDS.items():
        if any(kw in query_lower for kw in kws):
            if color in str(product.get("color", "")).lower():
                score += 2.5

    # Material keyword match
    for mat, kws in MATERIAL_KEYWORDS.items():
        if any(kw in query_lower for kw in kws):
            if mat in str(product.get("material", "")).lower():
                score += 2.5

    # Intent: occasion
    for occasion_kw in INTENT_KEYWORDS.get("occasion", []):
        if occasion_kw in query_lower and occasion_kw in field_text:
            score += 1.5

    # Trending & rating boosts
    score += float(product.get("trending_score", 0)) * 1.5
    score += float(product.get("rating", 0)) * 1.0

    # Price scoring
    try:
        raw_price = (
            str(product.get("price", "0"))
            .replace("$", "").replace("€", "")
            .replace(",", "").replace("pkr", "").strip()
        )
        product_price = float(raw_price)

        if price_range:
            if "min" in price_range and product_price >= price_range["min"]:
                score += 2.0
            if "max" in price_range and product_price <= price_range["max"]:
                score += 2.0
            # Penalize products outside the range slightly
            if "max" in price_range and product_price > price_range["max"]:
                score -= 1.0
            if "min" in price_range and product_price < price_range["min"]:
                score -= 0.5

        # Budget / premium intent boosts
        if any(kw in query_lower for kw in INTENT_KEYWORDS["low_price"]) and product_price < 50:
            score += 1.5
        elif any(kw in query_lower for kw in INTENT_KEYWORDS["high_price"]) and product_price >= 150:
            score += 1.5

    except (ValueError, TypeError):
        pass

    # Discount intent
    if any(kw in query_lower for kw in INTENT_KEYWORDS["discount"]):
        score += 1.0

    # New arrivals intent
    if any(kw in query_lower for kw in INTENT_KEYWORDS.get("new_arrivals", [])):
        if float(product.get("trending_score", 0)) >= 4.0:
            score += 1.5

    return score


# ============================================================
# PRODUCT FILTER & RANKER
# ============================================================

def filter_products(
    query: str,
    products: List[Dict[str, Any]],
) -> tuple[List[Dict[str, Any]], str]:
    """
    Filters products by category and color cues in the query,
    then ranks by relevance score. Returns (ranked_products, description).
    """
    query_lower = query.lower()
    filtered = list(products)
    desc_list: List[str] = []
    price_range = parse_price_range(query)

    # ---- Category filter ----
    for category, kws in CATEGORY_KEYWORDS.items():
        if any(kw in query_lower for kw in kws):
            category_filtered = [
                p for p in filtered
                if (
                    category in p.get("category", "").lower()
                    or category in p.get("title", "").lower()
                )
            ]
            if category_filtered:
                filtered = category_filtered
                desc_list.append(category.replace("_", " "))
            break  # Use the first matching category

    # ---- Color filter ----
    for color, kws in COLOR_KEYWORDS.items():
        if any(kw in query_lower for kw in kws):
            color_filtered = [
                p for p in filtered
                if (
                    color in p.get("color", "").lower()
                    or color in p.get("title", "").lower()
                )
            ]
            if color_filtered:
                filtered = color_filtered
                desc_list.append(color)
            break  # Use the first matching color

    # ---- Material filter ----
    for mat, kws in MATERIAL_KEYWORDS.items():
        if any(kw in query_lower for kw in kws):
            mat_filtered = [
                p for p in filtered
                if (
                    mat in p.get("material", "").lower()
                    or mat in p.get("title", "").lower()
                )
            ]
            if mat_filtered:
                filtered = mat_filtered
                desc_list.append(mat)
            break

    # ---- Price range filter ----
    if price_range:
        price_filtered = [p for p in filtered if _price_in_range(p, price_range)]
        if price_filtered:
            filtered = price_filtered
        desc_list.append(_price_desc(price_range))

    # ---- Score & rank ----
    scored = [
        {"product": p, "score": score_product_relevance(query, p, price_range)}
        for p in filtered
    ]
    ranked = sorted(scored, key=lambda x: x["score"], reverse=True)
    final = [x["product"] for x in ranked if x["score"] > 0.0]

    desc = " and ".join(desc_list) if desc_list else "your search"
    return final, desc


def _price_in_range(product: Dict[str, Any], price_range: Dict[str, float]) -> bool:
    """Returns True if the product's price falls within the given range."""
    try:
        raw = (
            str(product.get("price", "0"))
            .replace("$", "").replace("€", "")
            .replace(",", "").replace("pkr", "").strip()
        )
        val = float(raw)
        if "min" in price_range and val < price_range["min"]:
            return False
        if "max" in price_range and val > price_range["max"]:
            return False
        return True
    except (ValueError, TypeError):
        return False


def _price_desc(price_range: Dict[str, float]) -> str:
    """Returns a human-readable string for the active price range."""
    if "min" in price_range and "max" in price_range:
        return f"${price_range['min']:.0f}–${price_range['max']:.0f}"
    if "max" in price_range:
        return f"under ${price_range['max']:.0f}"
    if "min" in price_range:
        return f"above ${price_range['min']:.0f}"
    return "matching price"


# ============================================================
# FAQ HANDLER
# ============================================================

FAQ_KEYWORD_MAP = {
    "shipping": ["ship", "deliver", "ارسال", "versand", "delivery", "kab ayega", "how long"],
    "return": ["return", "refund", "واپسی", "rückgabe", "exchange", "back", "money back"],
    "track": ["track", "order status", "ٹریک", "verfolgen", "status", "kahan hai", "where is"],
    "Why I Choose Your Products": ["why", "choose", "کیوں", "why you", "why shop"],
    "What's the Best Quality of Your Business": [
        "best quality", "qualities", "business quality", "quality guarantee"
    ],
    "Hello": ["hello", "hi", "how are you", "what's going on", "hey", "good morning", "good evening"],
}

def get_faq_response(query: str) -> Optional[Dict[str, str]]:
    """
    Matches the user query against known FAQ topics.
    Returns the full multilingual FAQ entry, or None if no match.
    """
    faq = BOT_DATA.get("faq", {})
    q = query.lower()

    for faq_key, keywords in FAQ_KEYWORD_MAP.items():
        if any(kw in q for kw in keywords):
            result = faq.get(faq_key)
            if result:
                return result

    return None


# ============================================================
# DYNAMIC SUGGESTIONS
# ============================================================

def get_dynamic_suggestions(user_id: str, context: str, lang: str) -> List[str]:
    """
    Returns personalized, non-repeating smart suggestions for the user.
    Cycles through the full list once all options have been shown.
    """
    USER_SESSION_HISTORY.setdefault(user_id, {"shown": [], "lang": lang, "last_query": ""})

    sugs_dict = BOT_DATA.get("smart_suggestions", {})
    all_sugs = (
        sugs_dict.get(context, {}).get(lang)
        or sugs_dict.get("greeting", {}).get(lang)
        or []
    )

    shown = USER_SESSION_HISTORY[user_id].get("shown", [])
    available = [s for s in all_sugs if s not in shown]

    # Reset cycle when all options are exhausted
    if len(available) < 2 and all_sugs:
        USER_SESSION_HISTORY[user_id]["shown"] = []
        available = list(all_sugs)

    selected = random.sample(available, min(4, len(available)))
    USER_SESSION_HISTORY[user_id]["shown"] = list(
        set(USER_SESSION_HISTORY[user_id]["shown"] + selected)
    )

    return selected


# ============================================================
# BOT RESPONSE GENERATOR
# ============================================================

REPLY_TEMPLATES = {
    "found": {
        "en": "Here are the best matches for *{desc}*:",
        "ur": "*{desc}* سے متعلق بہترین آپشنز یہ ہیں:",
        "de": "Hier sind passende Ergebnisse für *{desc}*:",
    },
    "not_found": {
        "en": "I couldn't find a perfect match. Try a different color, material, or price range.",
        "ur": "کوئی مناسب چیز نہیں ملی۔ رنگ، مواد یا قیمت بدل کر دیکھیں۔",
        "de": "Ich konnte nichts Passendes finden. Versuchen Sie es mit einer anderen Farbe oder Preisspanne.",
    },
}

def generate_bot_response(user_id: str, msg: str) -> Dict[str, Any]:
    """
    Core response pipeline:
    1. Reload data from DB
    2. Detect language
    3. Check discount intent
    4. Check FAQ match
    5. Search & rank products
    6. Return structured response
    """
    load_data_realtime()

    lang = detect_language(msg)
    USER_SESSION_HISTORY.setdefault(user_id, {})
    USER_SESSION_HISTORY[user_id]["lang"] = lang

    # Track for analytics (multi-word queries only)
    if len(msg.split()) > 1:
        _track_safe(msg)

    response: Dict[str, Any] = {"reply": None, "carousel": None, "suggestions": []}
    query_lower = msg.lower()

    # ---- 1. Discount intent ----
    if any(kw in query_lower for kw in INTENT_KEYWORDS["discount"]):
        discount_msg = (
            BOT_DATA.get("discount_message", {}).get(lang)
            or BOT_DATA.get("discount_message", {}).get("en")
        )
        if discount_msg:
            response["reply"] = discount_msg
            response["suggestions"] = get_dynamic_suggestions(user_id, "greeting", lang)
            return response

    # ---- 2. FAQ match ----
    faq_entry = get_faq_response(msg)
    if faq_entry:
        response["reply"] = faq_entry.get(lang) or faq_entry.get("en")
        response["suggestions"] = get_dynamic_suggestions(user_id, "greeting", lang)
        return response

    # ---- 3. Product search with context memory ----
    last_query = USER_SESSION_HISTORY[user_id].get("last_query", "")
    combined_query = (
        f"{last_query} {msg}".strip()
        if len(msg.split()) < 3 and last_query
        else msg
    )
    USER_SESSION_HISTORY[user_id]["last_query"] = combined_query

    filtered, desc = filter_products(combined_query, PRODUCTS_DATA)

    if filtered:
        response["carousel"] = filtered[:8]
        template = REPLY_TEMPLATES["found"].get(lang, REPLY_TEMPLATES["found"]["en"])
        response["reply"] = template.format(desc=desc)
        response["suggestions"] = get_dynamic_suggestions(user_id, "greeting", lang)
        return response

    # ---- 4. Fallback ----
    response["reply"] = REPLY_TEMPLATES["not_found"].get(
        lang, REPLY_TEMPLATES["not_found"]["en"]
    )
    response["suggestions"] = get_dynamic_suggestions(user_id, "greeting", lang)
    return response


def _track_safe(msg: str) -> None:
    """Tracks a message for analytics, silently swallowing errors."""
    try:
        analytics.track_search(analytics_col, msg)
        analytics.track_question(analytics_col, msg)
    except Exception as e:
        logger.warning(f"Analytics tracking skipped: {e}")


# ============================================================
# HTTP ROUTES
# ============================================================

@app.get("/")
async def root():
    load_data_realtime()
    html_path = _template_path("chat.html")
    return FileResponse(html_path) if os.path.exists(html_path) else JSONResponse(
        {"status": "active", "info": "WebSocket available at /ws/chat"}
    )


@app.get("/Dashboard")
async def dashboard():
    load_data_realtime()
    html_path = _template_path("index.html")
    return FileResponse(html_path) if os.path.exists(html_path) else JSONResponse(
        {"status": "active", "info": "Dashboard not found."}
    )


# ============================================================
# ADMIN AUTHENTICATION
# ============================================================

@app.post("/password")
async def password_root(
    request: Request,
    password: str = Form(...),
    user_name: str = Form(...),
):
    expected_password = os.getenv("SECRET_PASSWORD")
    expected_user = os.getenv("USER_NAME")

    status = (
        "success"
        if password == expected_password and user_name == expected_user
        else "failed"
    )
    return templates.TemplateResponse("index.html", {"request": request, "status": status})


# ============================================================
# DATA API
# ============================================================

@app.get("/api/data")
async def get_home_data():
    """
    Returns synced products and bot config for the admin dashboard.
    """
    try:
        load_data_realtime()

        raw_suggestions = BOT_DATA.get("smart_suggestions", {})

        serialized_products = [
            {**p, "_id": str(p["_id"])} if "_id" in p else {**p}
            for p in PRODUCTS_DATA
        ]

        return {
            "products": serialized_products,
            "config": {
                "faq": BOT_DATA.get("faq", {}),
                "initial_message": BOT_DATA.get("initial_message", {}),
                "discount_message": BOT_DATA.get("discount_message", {}),
                "greeting": BOT_DATA.get("greeting", {}),
                "supported_languages": BOT_DATA.get("supported_languages", ["en", "ur"]),
                "smart_suggestions": {
                    "en": raw_suggestions.get("en", []),
                    "ur": raw_suggestions.get("ur", []),
                    "de": raw_suggestions.get("de", []),
                },
            },
        }
    except Exception as e:
        logger.error(f"Dashboard API error: {e}")
        return {
            "products": [],
            "config": {"faq": {}, "smart_suggestions": {"en": [], "ur": []}},
            "error": "Failed to synchronize with MongoDB Atlas.",
        }


# ============================================================
# PRODUCT CRUD
# ============================================================

@app.post("/Add_product")
async def add_new_product(
    request: Request,
    id: str = Form(...),
    title: str = Form(...),
    description: str = Form(...),
    category: str = Form(...),
    color: str = Form(...),
    material: str = Form(...),
    price: float = Form(...),
    rating: float = Form(...),
    trending_score: float = Form(...),
    image: str = Form(...),
    image_link: str = Form(...),
):
    if products_col is None:
        return templates.TemplateResponse(
            "index.html", {"request": request, "message": "Database not connected."}
        )

    new_product = {
        "id": id, "title": title, "description": description,
        "category": category, "color": color, "material": material,
        "price": price, "rating": rating, "trending_score": trending_score,
        "image": image, "image_link": image_link,
    }
    products_col.insert_one(new_product)
    return templates.TemplateResponse(
        "index.html", {"request": request, "message": "Product added successfully!"}
    )


@app.post("/delete_product")
async def delete_product(request: Request, id: str = Form(...)):
    if products_col is None:
        return templates.TemplateResponse(
            "index.html", {"request": request, "message": "Database not connected."}
        )
    try:
        result = products_col.delete_one({"_id": ObjectId(id)})
        msg = "Product deleted successfully!" if result.deleted_count else "Product not found."
    except Exception as e:
        msg = f"Error: {e}"

    return templates.TemplateResponse("index.html", {"request": request, "message": msg})


@app.post("/update_product")
async def update_product(
    request: Request,
    product_id: str = Form(...),
    title: str = Form(...),
    description: str = Form(...),
    category: str = Form(...),
    color: str = Form(...),
    material: str = Form(...),
    price: float = Form(...),
    rating: float = Form(...),
    trending_score: float = Form(...),
    image: str = Form(...),
    image_link: str = Form(...),
):
    if products_col is None:
        return templates.TemplateResponse(
            "index.html", {"request": request, "message": "Database not connected."}
        )

    update_data = {
        "title": title, "description": description, "category": category,
        "color": color, "material": material, "price": price,
        "rating": rating, "trending_score": trending_score,
        "image": image, "image_link": image_link,
    }

    result = products_col.update_one(
        {"_id": ObjectId(product_id)}, {"$set": update_data}
    )
    msg = "Product updated successfully!" if result.matched_count else "No product found with this ID."

    analytics.track_price_update(analytics_col, product_id)
    return templates.TemplateResponse("index.html", {"request": request, "message": msg})


# ============================================================
# ANALYTICS ROUTES
# ============================================================

@app.post("/track_click")
async def track_click_api(product_id: str = Form(...)):
    analytics.track_click(analytics_col, product_id)
    return {"status": "tracked", "product_id": product_id}


@app.post("/track_language")
async def track_language_api(language: str = Form(...)):
    analytics.track_language(analytics_col, language)
    return {"status": "tracked", "language": language}


@app.get("/api/analytics")
async def get_analytics():
    data = analytics.get_analytics_data(analytics_col)
    return JSONResponse(data)


@app.get("/analytics_dashboard", response_class=HTMLResponse)
async def analytics_dashboard(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


# ============================================================
# PRIVATE HELPERS
# ============================================================

def _template_path(filename: str) -> str:
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates", filename)


def track_search(query: str) -> None:
    analytics.track_search(analytics_col, query)


def track_question(question: str) -> None:
    analytics.track_question(analytics_col, question)


# ============================================================
# WEBSOCKET & ANALYTICS INIT
# ============================================================

app.include_router(websocket_router)
analytics.init_analytics(analytics_col)
init_database_sync()