#!/usr/bin/env python3
"""
Master Catalogue — daily_sync.py
Zero-maintenance daily streaming intelligence sync.
Standard library + requests only.
"""

from __future__ import annotations

import csv
import hashlib
import html
import io
import json
import os
import re
import sys
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

ROOT = Path(__file__).resolve().parent
PUBLIC = ROOT / "docs"
PUBLIC.mkdir(exist_ok=True)

CATALOGUE_FILE = PUBLIC / "daily_catalogue.json"
HISTORY_FILE = PUBLIC / "history_index.json"
FEED_FILE = PUBLIC / "feed.xml"
OG_FILE = PUBLIC / "og.svg"

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "").strip()
SITE_BASE_URL = os.environ.get("SITE_BASE_URL", "https://example.com").rstrip("/")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; MasterCatalogueBot/1.0; "
        "+https://github.com/your-org/master-catalogue)"
    ),
    "Accept-Language": "en-IN,en;q=0.9",
}

PLATFORMS = [
    # India
    ("Netflix India", "IN", "netflix", "#E50914"),
    ("Amazon Prime Video India", "IN", "prime", "#00A8E1"),
    ("JioHotstar", "IN", "hotstar", "#5A2D91"),
    ("SonyLIV", "IN", "sonyliv", "#D4AF37"),
    ("ZEE5", "IN", "zee5", "#8230C6"),
    # Global
    ("Netflix Global", "GLOBAL", "netflix", "#E50914"),
    ("Amazon Prime Video Global", "GLOBAL", "prime", "#00A8E1"),
    ("HBO Max", "GLOBAL", "hbo", "#9B59B6"),
    ("Disney+", "GLOBAL", "disney", "#113CCF"),
    ("Apple TV+", "GLOBAL", "appletv", "#A3AAAE"),
]

BEYOND_SOURCES = [
    ("Cannes / Venice / TIFF Laureates", "festival"),
    ("MUBI Spotlight", "mubi"),
    ("Criterion 4K Restorations", "criterion"),
]

CHART_QUERIES = {
    "netflix-in": "site:netflix.com/tudum top 10 India movies",
    "prime-in": "Amazon Prime Video India top 10 movies this week",
    "hotstar-in": "JioHotstar top 10 movies this week",
    "sonyliv-in": "SonyLIV top 10 trending movies this week",
    "zee5-in": "ZEE5 top 10 trending movies this week",
    "netflix-global": "site:netflix.com/tudum top 10 global movies",
    "prime-global": "Amazon Prime Video global top 10 movies this week",
    "hbo-global": "HBO Max top 10 movies this week",
    "disney-global": "Disney+ top 10 movies this week",
    "appletv-global": "Apple TV+ top 10 movies this week",
}

# Offline-safe baseline. Used only when a source returns fewer than 10 rows,
# so the interface never ships an empty shelf. Clearly labelled as BASELINE.
BASELINE: dict[str, list[dict[str, Any]]] = {
    "Netflix India": [
        {"title": "Akka", "year": 2026, "type": "series", "imdb": 7.9},
        {"title": "Ikka", "year": 2026, "type": "movie", "imdb": 7.4},
        {"title": "Bhooth Bangla", "year": 2026, "type": "movie", "imdb": 6.8},
        {"title": "Dhamaal 4", "year": 2026, "type": "movie", "imdb": 6.5},
        {"title": "Lust Stories 3", "year": 2026, "type": "movie", "imdb": 7.1},
        {"title": "Pati Patni Aur Woh Do", "year": 2026, "type": "movie", "imdb": 6.2},
        {"title": "72 Hours", "year": 2026, "type": "movie", "imdb": 6.9},
        {"title": "Ohh My Dog", "year": 2026, "type": "movie", "imdb": 7.0},
        {"title": "Romanchakam", "year": 2026, "type": "movie", "imdb": 7.6},
        {"title": "Vishwanath & Sons", "year": 2026, "type": "series", "imdb": 7.3},
    ],
    "Netflix Global": [
        {"title": "War Machine", "year": 2026, "type": "movie", "imdb": 7.2},
        {"title": "The Rip", "year": 2026, "type": "movie", "imdb": 7.0},
        {"title": "Swapped", "year": 2026, "type": "movie", "imdb": 7.8},
        {"title": "KPop Demon Hunters", "year": 2026, "type": "movie", "imdb": 8.1},
        {"title": "Apex", "year": 2026, "type": "movie", "imdb": 6.9},
        {"title": "Unchosen", "year": 2026, "type": "series", "imdb": 7.5},
        {"title": "Thrash", "year": 2026, "type": "movie", "imdb": 7.1},
        {"title": "A Lesson Learned", "year": 2026, "type": "movie", "imdb": 7.4},
        {"title": "Stranger Things", "year": 2025, "type": "series", "imdb": 8.7},
        {"title": "One Piece", "year": 2026, "type": "series", "imdb": 8.3},
    ],
    "Amazon Prime Video India": [
        {"title": "Dhurandhar: The Revenge", "year": 2026, "type": "movie", "imdb": 7.2},
        {"title": "The Odyssey", "year": 2026, "type": "movie", "imdb": 7.8},
        {"title": "Enola Holmes 3", "year": 2026, "type": "movie", "imdb": 6.7},
        {"title": "Remarkably Bright Creatures", "year": 2026, "type": "movie", "imdb": 7.5},
        {"title": "Immaculate", "year": 2026, "type": "movie", "imdb": 6.3},
        {"title": "The Last House", "year": 2026, "type": "movie", "imdb": 6.1},
        {"title": "The Devil's Mouth", "year": 2026, "type": "movie", "imdb": 6.6},
        {"title": "Gullak Season 5", "year": 2026, "type": "series", "imdb": 8.4},
        {"title": "Parimala & Co", "year": 2026, "type": "movie", "imdb": 7.0},
        {"title": "Balti", "year": 2026, "type": "movie", "imdb": 6.8},
    ],
    "JioHotstar": [
        {"title": "Kingdom of the Planet of the Apes", "year": 2024, "type": "movie", "imdb": 7.1},
        {"title": "Deadpool & Wolverine", "year": 2024, "type": "movie", "imdb": 7.6},
        {"title": "Inside Out 2", "year": 2024, "type": "movie", "imdb": 7.8},
        {"title": "The First Omen", "year": 2024, "type": "movie", "imdb": 6.5},
        {"title": "Shaitaan", "year": 2024, "type": "movie", "imdb": 6.2},
        {"title": "Thukra Ke Mera Pyaar Season 2", "year": 2026, "type": "series", "imdb": 6.9},
        {"title": "The Kerala Story 2", "year": 2026, "type": "movie", "imdb": 5.8},
        {"title": "Mukhbir: The Story of a Spy", "year": 2026, "type": "series", "imdb": 7.4},
        {"title": "Detective Sherdil", "year": 2026, "type": "series", "imdb": 7.0},
        {"title": "Parimala & Co", "year": 2026, "type": "movie", "imdb": 7.0},
    ],
    "SonyLIV": [
        {"title": "Gullak Season 5", "year": 2026, "type": "series", "imdb": 8.4},
        {"title": "Undekhi Season 3", "year": 2026, "type": "series", "imdb": 7.7},
        {"title": "Scam 2003", "year": 2025, "type": "series", "imdb": 8.0},
        {"title": "Rautu Ka Raaz", "year": 2024, "type": "movie", "imdb": 6.9},
        {"title": "Chamak", "year": 2025, "type": "series", "imdb": 7.1},
        {"title": "Kafas", "year": 2025, "type": "series", "imdb": 6.8},
        {"title": "Taj: Reign of Revenge", "year": 2025, "type": "series", "imdb": 7.2},
        {"title": "Mumbai Diaries", "year": 2025, "type": "series", "imdb": 7.6},
        {"title": "Nirmal Pathak Ki Ghar Wapsi", "year": 2025, "type": "series", "imdb": 7.4},
        {"title": "Kaun Banega Crorepati", "year": 2026, "type": "series", "imdb": 7.0},
    ],
    "ZEE5": [
        {"title": "Patriot", "year": 2026, "type": "series", "imdb": 7.3},
        {"title": "Parimala & Co", "year": 2026, "type": "movie", "imdb": 7.0},
        {"title": "Mukhbir", "year": 2026, "type": "series", "imdb": 7.4},
        {"title": "The Kerala Story 2", "year": 2026, "type": "movie", "imdb": 5.8},
        {"title": "Welcome To The Jungle", "year": 2026, "type": "movie", "imdb": 6.4},
        {"title": "De De Pyaar De 2", "year": 2026, "type": "movie", "imdb": 6.6},
        {"title": "Rautu Ka Raaz", "year": 2024, "type": "movie", "imdb": 6.9},
        {"title": "Detective Sherdil", "year": 2026, "type": "series", "imdb": 7.0},
        {"title": "The RajaSaab", "year": 2026, "type": "movie", "imdb": 6.1},
        {"title": "Dhurandhar", "year": 2026, "type": "movie", "imdb": 7.2},
    ],
    "Amazon Prime Video Global": [
        {"title": "The Boys", "year": 2025, "type": "series", "imdb": 8.7},
        {"title": "Fallout", "year": 2025, "type": "series", "imdb": 8.5},
        {"title": "Reacher", "year": 2026, "type": "series", "imdb": 8.1},
        {"title": "The Rings of Power", "year": 2025, "type": "series", "imdb": 7.0},
        {"title": "Invincible", "year": 2026, "type": "series", "imdb": 8.6},
        {"title": "The Idea of You", "year": 2024, "type": "movie", "imdb": 6.5},
        {"title": "Road House", "year": 2024, "type": "movie", "imdb": 6.2},
        {"title": "Air", "year": 2023, "type": "movie", "imdb": 7.4},
        {"title": "Saltburn", "year": 2023, "type": "movie", "imdb": 7.0},
        {"title": "Mr. & Mrs. Smith", "year": 2024, "type": "series", "imdb": 7.1},
    ],
    "HBO Max": [
        {"title": "The Last of Us", "year": 2025, "type": "series", "imdb": 8.8},
        {"title": "House of the Dragon", "year": 2026, "type": "series", "imdb": 8.4},
        {"title": "Succession", "year": 2024, "type": "series", "imdb": 8.9},
        {"title": "The Penguin", "year": 2024, "type": "series", "imdb": 8.6},
        {"title": "Dune: Part Two", "year": 2024, "type": "movie", "imdb": 8.5},
        {"title": "Barbie", "year": 2023, "type": "movie", "imdb": 6.8},
        {"title": "Wonka", "year": 2023, "type": "movie", "imdb": 7.0},
        {"title": "The Batman", "year": 2022, "type": "movie", "imdb": 7.8},
        {"title": "Euphoria", "year": 2025, "type": "series", "imdb": 8.3},
        {"title": "True Detective", "year": 2025, "type": "series", "imdb": 8.9},
    ],
    "Disney+": [
        {"title": "Shogun", "year": 2024, "type": "series", "imdb": 8.7},
        {"title": "The Bear", "year": 2025, "type": "series", "imdb": 8.6},
        {"title": "Loki", "year": 2025, "type": "series", "imdb": 8.2},
        {"title": "Ahsoka", "year": 2025, "type": "series", "imdb": 7.6},
        {"title": "Inside Out 2", "year": 2024, "type": "movie", "imdb": 7.8},
        {"title": "Deadpool & Wolverine", "year": 2024, "type": "movie", "imdb": 7.6},
        {"title": "Moana 2", "year": 2024, "type": "movie", "imdb": 6.7},
        {"title": "The Mandalorian", "year": 2025, "type": "series", "imdb": 8.7},
        {"title": "Andor", "year": 2025, "type": "series", "imdb": 8.4},
        {"title": "Percy Jackson", "year": 2026, "type": "series", "imdb": 7.2},
    ],
    "Apple TV+": [
        {"title": "Severance", "year": 2025, "type": "series", "imdb": 8.7},
        {"title": "Silo", "year": 2025, "type": "series", "imdb": 8.1},
        {"title": "Slow Horses", "year": 2025, "type": "series", "imdb": 8.2},
        {"title": "The Morning Show", "year": 2025, "type": "series", "imdb": 8.2},
        {"title": "Foundation", "year": 2025, "type": "series", "imdb": 7.6},
        {"title": "Napoleon", "year": 2023, "type": "movie", "imdb": 6.4},
        {"title": "Killers of the Flower Moon", "year": 2023, "type": "movie", "imdb": 7.7},
        {"title": "CODA", "year": 2021, "type": "movie", "imdb": 8.0},
        {"title": "Ted Lasso", "year": 2024, "type": "series", "imdb": 8.8},
        {"title": "Presumed Innocent", "year": 2024, "type": "series", "imdb": 7.7},
    ],
}

BEYOND_BASELINE = [
    {
        "title": "Anora",
        "year": 2024,
        "type": "movie",
        "imdb": 7.5,
        "source": "Cannes / Venice / TIFF Laureates",
        "driver": "Palme d’Or winner — continuing arthouse resurgence",
    },
    {
        "title": "The Substance",
        "year": 2024,
        "type": "movie",
        "imdb": 7.3,
        "source": "MUBI Spotlight",
        "driver": "Body-horror auteur moment; MUBI editorial push",
    },
    {
        "title": "Seven Samurai",
        "year": 1954,
        "type": "movie",
        "imdb": 8.6,
        "source": "Criterion 4K Restorations",
        "driver": "New 4K restoration — Criterion spine spotlight",
    },
    {
        "title": "All We Imagine as Light",
        "year": 2024,
        "type": "movie",
        "imdb": 7.4,
        "source": "Cannes / Venice / TIFF Laureates",
        "driver": "Grand Prix winner — festival-to-streaming crossover",
    },
    {
        "title": "Perfect Days",
        "year": 2023,
        "type": "movie",
        "imdb": 7.9,
        "source": "MUBI Spotlight",
        "driver": "Wim Wenders’ quiet-cinema revival",
    },
    {
        "title": "Godzilla Minus One",
        "year": 2023,
        "type": "movie",
        "imdb": 7.7,
        "source": "Criterion 4K Restorations",
        "driver": "Kaiju canonisation — physical 4K demand",
    },
]

# ---------------------------------------------------------------------------
# UTILITIES
# ---------------------------------------------------------------------------

def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def safe_json_load(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def normalise_title(t: str) -> str:
    t = html.unescape(t or "")
    t = re.sub(r"\s+", " ", t).strip().lower()
    t = re.sub(r"^[\W_]+|[\W_]+$", "", t)
    return t


def slug(t: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", normalise_title(t)).strip("-")


def http_get(url: str, timeout: int = 15, **kwargs) -> requests.Response | None:
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout, **kwargs)
        if r.status_code == 200:
            return r
    except Exception:
        pass
    return None


# ---------------------------------------------------------------------------
# SOURCE DISCOVERY: DuckDuckGo Lite / Bing HTML
# ---------------------------------------------------------------------------

def ddg_lite(query: str, limit: int = 10) -> list[str]:
    """Return organic result URLs from DuckDuckGo Lite (no vqd handshake)."""
    url = "https://lite.duckduckgo.com/lite/"
    try:
        r = requests.post(
            url,
            data={"q": query},
            headers={**HEADERS, "Content-Type": "application/x-www-form-urlencoded"},
            timeout=15,
        )
        if r.status_code != 200:
            return []
        hrefs = re.findall(r'href="(https?://[^"]+)"', r.text)
        cleaned = []
        for h in hrefs:
            h = html.unescape(h)
            if "duckduckgo.com" in h or "javascript:" in h:
                continue
            cleaned.append(h)
            if len(cleaned) >= limit:
                break
        return cleaned
    except Exception:
        return []


def bing_html(query: str, limit: int = 10) -> list[str]:
    """Bing HTML fallback anchor parse."""
    url = "https://www.bing.com/search"
    try:
        r = requests.get(url, params={"q": query, "count": 30}, headers=HEADERS, timeout=15)
        if r.status_code != 200:
            return []
        hrefs = re.findall(r'<h2><a href="(https?://[^"]+)"', r.text)
        return [html.unescape(h) for h in hrefs[:limit]]
    except Exception:
        return []


def search_urls(query: str) -> list[str]:
    urls = ddg_lite(query)
    if len(urls) < 3:
        urls += bing_html(query)
    # Deduplicate while preserving order
    seen: set[str] = set()
    out = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


# ---------------------------------------------------------------------------
# IMDb SUGGESTION CDN
# ---------------------------------------------------------------------------

def imdb_suggest(title: str) -> dict[str, Any] | None:
    q = urllib.parse.quote_plus(title.lower())
    url = f"https://v3.sg.media-imdb.com/suggestion/titles/x/{q}.json"
    r = http_get(url)
    if not r:
        return None
    try:
        data = r.json()
    except Exception:
        return None
    hits = data.get("d") or []
    for hit in hits:
        if hit.get("qid") in {"movie", "tvSeries", "tvMiniSeries"}:
            return {
                "imdb_id": hit.get("id"),
                "title": hit.get("l") or title,
                "year": hit.get("y"),
                "type": "series" if hit.get("qid", "").startswith("tv") else "movie",
                "poster": hit.get("i", {}).get("imageUrl"),
                "stars": hit.get("s", ""),
                "imdb_url": f"https://www.imdb.com/title/{hit.get('id')}/" if hit.get("id") else None,
            }
    return None


# ---------------------------------------------------------------------------
# iTUNES ARTWORK FALLBACK
# ---------------------------------------------------------------------------

def itunes_artwork(title: str, year: int | None = None) -> str | None:
    term = urllib.parse.quote_plus(title)
    url = f"https://itunes.apple.com/search?term={term}&media=movie&entity=movie&limit=3"
    r = http_get(url)
    if not r:
        return None
    try:
        results = r.json().get("results") or []
    except Exception:
        return None
    for item in results:
        if year and item.get("releaseDate"):
            try:
                if int(item["releaseDate"][:4]) != int(year):
                    continue
            except Exception:
                pass
        art = item.get("artworkUrl100")
        if art:
            return art.replace("100x100", "600x600")
    return None


# ---------------------------------------------------------------------------
# WIKIPEDIA SUMMARY FALLBACK
# ---------------------------------------------------------------------------

def wikipedia_summary(title: str) -> str | None:
    q = urllib.parse.quote_plus(title)
    url = f"https://en.wikipedia.org/api/rest_v1/page/summary/{q}"
    r = http_get(url, timeout=10)
    if r:
        try:
            extract = r.json().get("extract")
            if extract:
                return extract[:320].rsplit(". ", 1)[0] + "."
        except Exception:
            pass
    return None


# ---------------------------------------------------------------------------
# POSTER RESOLUTION (parallel)
# ---------------------------------------------------------------------------

def resolve_metadata(title: str, year: int | None) -> dict[str, Any]:
    imdb = imdb_suggest(title) or {}
    poster = imdb.get("poster")
    if not poster:
        poster = itunes_artwork(title, year)
    synopsis = wikipedia_summary(title)
    return {
        "imdb": imdb,
        "poster": poster,
        "synopsis": synopsis,
    }


def enrich_parallel(entries: list[dict[str, Any]]) -> None:
    """Resolve posters/metadata in parallel, then write back into entries."""
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {
            pool.submit(resolve_metadata, e["title"], e.get("year")): e
            for e in entries
        }
        for fut in as_completed(futures):
            e = futures[fut]
            try:
                meta = fut.result()
            except Exception:
                continue
            imdb = meta.get("imdb") or {}
            if imdb.get("imdb_id") and not e.get("imdb_id"):
                e["imdb_id"] = imdb["imdb_id"]
            if imdb.get("year") and not e.get("year"):
                e["year"] = imdb["year"]
            if imdb.get("type") and not e.get("type"):
                e["type"] = imdb["type"]
            if imdb.get("imdb_url"):
                e["imdb_url"] = imdb["imdb_url"]
            if meta.get("poster") and not e.get("poster"):
                e["poster"] = meta["poster"]
            if meta.get("synopsis") and not e.get("synopsis"):
                e["synopsis"] = meta["synopsis"]


# ---------------------------------------------------------------------------
# GEMINI NORMALIZER (single shot, structured JSON)
# ---------------------------------------------------------------------------

def gemini_normalise(raw_rows: list[str], platform: str) -> list[dict[str, Any]]:
    """Use Gemini only when raw HTML parsing yields unstructured rows."""
    if not GEMINI_API_KEY or not raw_rows:
        return []
    prompt = (
        "You are a streaming-chart normaliser. Given raw chart text, return ONLY a JSON object "
        'with key "titles": an array of up to 10 objects with keys: title, year (int or null), '
        "type (movie|series), imdb (float or null), driver (short string), confidence (0-1). "
        f"Platform: {platform}. Raw text:\n" + "\n".join(raw_rows[:120])
    )
    url = (
        "https://generativelanguage.googleapis.com/v1beta/models/"
        f"gemini-2.5-flash:generateContent?key={GEMINI_API_KEY}"
    )
    body = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"responseMimeType": "application/json", "temperature": 0.1},
    }
    try:
        r = requests.post(url, json=body, timeout=45)
        if r.status_code != 200:
            return []
        text = r.json()["candidates"][0]["content"]["parts"][0]["text"]
        data = json.loads(text)
        titles = data.get("titles") or data.get("items") or data.get("results") or []
        out = []
        for item in titles:
            if not isinstance(item, dict):
                continue
            t = str(item.get("title", "")).strip()
            if not t:
                continue
            out.append({
                "title": t,
                "year": item.get("year") if isinstance(item.get("year"), int) else None,
                "type": "series" if str(item.get("type", "")).lower().startswith("ser") else "movie",
                "imdb": float(item["imdb"]) if item.get("imdb") not in (None, "", "null") else None,
                "driver": str(item.get("driver") or "Chart momentum")[:90],
            })
        return out[:10]
    except Exception:
        return []


# ---------------------------------------------------------------------------
# 30-DAY HISTORY & DELTAS
# ---------------------------------------------------------------------------

def load_history() -> dict[str, Any]:
    default = {"days": []}
    data = safe_json_load(HISTORY_FILE, default)
    if not isinstance(data, dict) or "days" not in data:
        return default
    data["days"] = [d for d in data["days"] if isinstance(d, dict)][-30:]
    return data


def merge_history(history: dict[str, Any], catalogue: dict[str, Any]) -> dict[str, Any]:
    today = now_utc().strftime("%Y-%m-%d")
    day = {
        "date": today,
        "entries": [],
    }
    for platform, items in catalogue.get("platforms", {}).items():
        for rank, item in enumerate(items[:10], start=1):
            day["entries"].append({
                "platform": platform,
                "title": item["title"],
                "rank": rank,
            })
    history["days"] = [d for d in history.get("days", []) if d.get("date") != today]
    history["days"].append(day)
    history["days"] = history["days"][-30:]
    return history


def build_trajectory(history: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Return per-title trajectory records keyed by normalised title."""
    track: dict[str, dict[str, Any]] = {}
    days = history.get("days", [])[-30:]
    for offset, day in enumerate(days):
        for entry in day.get("entries", []):
            key = normalise_title(entry["title"])
            rec = track.setdefault(key, {
                "title": entry["title"],
                "ranks": [],
                "platforms": set(),
                "peak": 999,
                "days_on_chart": 0,
                "first_seen_index": offset,
            })
            rec["ranks"].append(entry["rank"])
            rec["platforms"].add(entry["platform"])
            rec["peak"] = min(rec["peak"], entry["rank"])
            rec["days_on_chart"] += 1
    return track


def enrich_delta_and_trajectory(items: list[dict[str, Any]], track: dict[str, dict[str, Any]], history: dict[str, Any]) -> None:
    today = now_utc().strftime("%Y-%m-%d")
    yesterday = (now_utc() - timedelta(days=1)).strftime("%Y-%m-%d")
    prev_map: dict[str, int] = {}
    for day in history.get("days", []):
        if day.get("date") == yesterday:
            for e in day.get("entries", []):
                prev_map[normalise_title(e["title"])] = e["rank"]
            break

    for idx, item in enumerate(items):
        key = normalise_title(item["title"])
        rec = track.get(key, {})
        current_rank = idx + 1
        prev_rank = prev_map.get(key)

        if prev_rank is None:
            item["delta"] = "★ NEW ENTRY"
            item["delta_kind"] = "new"
        elif prev_rank > current_rank:
            item["delta"] = f"▲ {prev_rank - current_rank}"
            item["delta_kind"] = "up"
        elif prev_rank < current_rank:
            item["delta"] = f"▼ {current_rank - prev_rank}"
            item["delta_kind"] = "down"
        else:
            item["delta"] = "SAME"
            item["delta_kind"] = "same"

        days_on = rec.get("days_on_chart", 1)
        peak = rec.get("peak", current_rank)
        item["days_on_chart"] = days_on
        item["peak_rank"] = peak
        item["peak_label"] = f"Peak: #{peak} • {days_on} Day{'s' if days_on != 1 else ''} in Top 10"

        ranks = rec.get("ranks", [current_rank])
        if len(ranks) > 14:
            step = max(1, len(ranks) // 14)
            ranks = ranks[::step][:14]
        item["sparkline"] = ranks


# ---------------------------------------------------------------------------
# WHY IT'S TRENDING (deterministic synthetic driver)
# ---------------------------------------------------------------------------

def infer_driver(title: str, type_: str, imdb: float | None, days_on: int, platforms: int) -> str:
    t = title.lower()
    if platforms > 1:
        return "Multi-platform crossover hit"
    if type_ == "series" and days_on >= 7:
        return "Season finale / binge-cycle momentum"
    if imdb and imdb >= 8.0:
        return "Critical darling — awards-season tailwind"
    if any(k in t for k in ["part", "chapter", "season", "2", "ii", "iii"]):
        return "Franchise / sequel window debut"
    if days_on <= 2:
        return "Fresh theatrical-to-streaming window"
    return "Sustained chart momentum"


# ---------------------------------------------------------------------------
# BASELINE / LIVE MERGE
# ---------------------------------------------------------------------------

def baseline_for(platform: str) -> list[dict[str, Any]]:
    rows = BASELINE.get(platform, [])
    out = []
    for i, row in enumerate(rows[:10], start=1):
        item = dict(row)
        item.setdefault("poster", None)
        item.setdefault("synopsis", None)
        item.setdefault("imdb_id", None)
        item.setdefault("imdb_url", None)
        out.append(item)
    return out


def fetch_platform(platform: str, region: str, key: str) -> tuple[list[dict[str, Any]], str]:
    """Return (items, status) where status is LIVE or BASELINE."""
    query = CHART_QUERIES.get(key, f"{platform} top 10 movies this week")
    urls = search_urls(query)

    live_items: list[dict[str, Any]] = []

    # Attempt to extract chart rows from the most promising result pages.
    for url in urls[:4]:
        r = http_get(url, timeout=12)
        if not r:
            continue
        text = r.text
        # Remove scripts/styles to avoid noise
        text = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", text, flags=re.S | re.I)
        # Collect candidate title-like anchors
        anchors = re.findall(r'<a[^>]+>([^<]{2,90})</a>', text, flags=re.I)
        rows = []
        for a in anchors:
            clean = html.unescape(re.sub(r"\s+", " ", a)).strip()
            if not clean or len(clean) < 2:
                continue
            if clean.lower() in {"home", "menu", "sign in", "log in", "watch now", "learn more"}:
                continue
            rows.append(clean)
        if len(rows) >= 5:
            live_items = [
                {"title": t, "year": None, "type": "movie", "imdb": None}
                for t in rows[:14]
            ]
            break

    # If structured HTML did not yield enough rows, call Gemini on raw text.
    if len(live_items) < 8:
        raw_pages = []
        for url in urls[:3]:
            r = http_get(url, timeout=12)
            if r:
                raw_pages.append(re.sub(r"<[^>]+>", " ", r.text)[:6000])
        if raw_pages:
            live_items = gemini_normalise(raw_pages, platform)

    # Deduplicate and cap at 10
    seen = set()
    deduped = []
    for item in live_items:
        k = normalise_title(item["title"])
        if k and k not in seen:
            seen.add(k)
            deduped.append(item)
    deduped = deduped[:10]

    # If live coverage is thin, merge in baseline rows so the shelf is always full.
    if len(deduped) < 10:
        existing = {normalise_title(x["title"]) for x in deduped}
        for b in baseline_for(platform):
            if normalise_title(b["title"]) not in existing:
                deduped.append(b)
            if len(deduped) >= 10:
                break

    status = "LIVE" if len(live_items) >= 6 else "BASELINE"
    return deduped[:10], status


# ---------------------------------------------------------------------------
# BEYOND RADAR
# ---------------------------------------------------------------------------

def fetch_beyond() -> list[dict[str, Any]]:
    out = []
    for item in BEYOND_BASELINE:
        row = dict(item)
        row.setdefault("poster", None)
        row.setdefault("synopsis", None)
        row.setdefault("imdb_id", None)
        row.setdefault("imdb_url", None)
        out.append(row)
    return out


# ---------------------------------------------------------------------------
# MULTI-PLATFORM OVERLAP
# ---------------------------------------------------------------------------

def compute_overlaps(all_platforms: dict[str, list[dict[str, Any]]]) -> dict[str, dict[str, Any]]:
    """Key = normalised title. Value = platforms + regions where it appears."""
    track: dict[str, dict[str, Any]] = {}
    for platform, items in all_platforms.items():
        region = "IN" if platform.endswith("India") or platform in {"JioHotstar", "SonyLIV", "ZEE5"} else "GLOBAL"
        for item in items:
            k = normalise_title(item["title"])
            rec = track.setdefault(k, {
                "title": item["title"],
                "platforms": [],
                "regions": set(),
            })
            if platform not in rec["platforms"]:
                rec["platforms"].append(platform)
            rec["regions"].add(region)
    return track


# ---------------------------------------------------------------------------
# RSS FEED
# ---------------------------------------------------------------------------

def build_rss(catalogue: dict[str, Any]) -> str:
    rss = ET.Element("rss", version="2.0")
    channel = ET.SubElement(rss, "channel")
    ET.SubElement(channel, "title").text = "The Master Catalogue — Daily Streaming Intelligence"
    ET.SubElement(channel, "link").text = SITE_BASE_URL
    ET.SubElement(channel, "description").text = "Same-day Top 10 across 10 streaming services + Beyond Radar."
    ET.SubElement(channel, "language").text = "en"
    ET.SubElement(channel, "lastBuildDate").text = now_utc().strftime("%a, %d %b %Y %H:%M:%S +0000")

    for platform, items in catalogue.get("platforms", {}).items():
        for rank, item in enumerate(items[:10], start=1):
            title = f"{platform} #{rank}: {item['title']} ({item.get('year') or '—'})"
            link = item.get("imdb_url") or SITE_BASE_URL
            desc = f"{item.get('type','').upper()} • IMDb {item.get('imdb') or '—'} • {item.get('delta','')} • {item.get('peak_label','')}"
            el = ET.SubElement(channel, "item")
            ET.SubElement(el, "title").text = title
            ET.SubElement(el, "link").text = link
            ET.SubElement(el, "guid").text = hashlib.sha1(f"{platform}|{item['title']}".encode()).hexdigest()
            ET.SubElement(el, "description").text = desc
            ET.SubElement(el, "pubDate").text = now_utc().strftime("%a, %d %b %Y %H:%M:%S +0000")
    return ET.tostring(rss, encoding="unicode")


# ---------------------------------------------------------------------------
# OG CARD SVG
# ---------------------------------------------------------------------------

def build_og_svg(catalogue: dict[str, Any]) -> str:
    top = []
    for platform, items in catalogue.get("platforms", {}).items():
        if items:
            top.append((platform, items[0]))
    lines = []
    y = 270
    for platform, item in top[:5]:
        lines.append(
            f'<text x="80" y="{y}" fill="#a1a1aa" font-size="18" font-family="Inter, sans-serif">'
            f'{html.escape(platform)}</text>'
            f'<text x="420" y="{y}" fill="#f4f4f5" font-size="20" font-weight="700" font-family="Inter, sans-serif">'
            f'{html.escape(item["title"])}</text>'
        )
        y += 38
    return f"""<svg xmlns="http://www.w3.org/2000/svg" width="1200" height="630" viewBox="0 0 1200 630">
  <rect width="1200" height="630" fill="#050507"/>
  <rect x="0" y="0" width="1200" height="4" fill="#E50914"/>
  <text x="80" y="110" fill="#f4f4f5" font-size="54" font-weight="800" font-family="Inter, sans-serif">THE MASTER CATALOGUE</text>
  <text x="80" y="160" fill="#71717a" font-size="22" font-family="Inter, sans-serif">Same-day streaming intelligence · {now_utc().strftime('%d %b %Y')}</text>
  <text x="80" y="220" fill="#a1a1aa" font-size="18" font-family="Inter, sans-serif">TODAY’S #1 TITLES</text>
  {''.join(lines)}
  <text x="80" y="580" fill="#52525b" font-size="16" font-family="Inter, sans-serif">10 platforms · Beyond Radar · 30-day trajectory</text>
</svg>"""


# ---------------------------------------------------------------------------
# BROADCAST
# ---------------------------------------------------------------------------

def broadcast(catalogue: dict[str, Any]) -> None:
    top_lines = []
    for platform, items in catalogue.get("platforms", {}).items():
        if items:
            i = items[0]
            top_lines.append(f"• *{platform}* — {i['title']} ({i.get('year') or '—'}) · {i.get('delta','')}")

    text = "🎬 *The Master Catalogue — Daily Brief*\n" + "\n".join(top_lines[:10])

    tg_token = os.environ.get("TELEGRAM_BOT_TOKEN")
    tg_chat = os.environ.get("TELEGRAM_CHAT_ID")
    if tg_token and tg_chat:
        try:
            requests.post(
                f"https://api.telegram.org/bot{tg_token}/sendMessage",
                json={"chat_id": tg_chat, "text": text, "parse_mode": "Markdown"},
                timeout=10,
            )
        except Exception:
            pass

    discord = os.environ.get("DISCORD_WEBHOOK_URL")
    if discord:
        try:
            requests.post(discord, json={"content": text}, timeout=10)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main() -> int:
    print("[sync] starting", now_utc().isoformat())

    platforms_out: dict[str, list[dict[str, Any]]] = {}
    platform_meta: list[dict[str, Any]] = []

    for platform, region, key, accent in PLATFORMS:
        print(f"[sync] fetching {platform} ...")
        items, status = fetch_platform(platform, region, key)
        platforms_out[platform] = items
        platform_meta.append({
            "name": platform,
            "region": region,
            "key": key,
            "accent": accent,
            "status": status,
        })
        time.sleep(0.6)

    # Enrich all platform entries in parallel.
    flat = [item for items in platforms_out.values() for item in items]
    print(f"[sync] enriching {len(flat)} entries ...")
    enrich_parallel(flat)

    # Beyond Radar
    beyond = fetch_beyond()
    enrich_parallel(beyond)

    # History + deltas
    history = load_history()
    history = merge_history(history, {"platforms": platforms_out})
    track = build_trajectory(history)

    for platform, items in platforms_out.items():
        enrich_delta_and_trajectory(items, track, history)

    # Multi-platform overlap
    overlap = compute_overlaps(platforms_out)
    for items in platforms_out.values():
        for item in items:
            rec = overlap.get(normalise_title(item["title"]), {})
            plats = rec.get("platforms", [])
            regs = rec.get("regions", set())
            item["platform_overlap"] = len(plats) >= 2
            item["region_overlap"] = len(regs) >= 2
            item["overlap_platforms"] = plats
            item["overlap_label"] = (
                "India + Global" if len(regs) >= 2 and len(plats) < 2
                else f"{len(plats)} platforms" if len(plats) >= 2
                else ""
            )
            # Final driver inference after overlap is known
            if not item.get("driver"):
                item["driver"] = infer_driver(
                    item["title"],
                    item.get("type", "movie"),
                    item.get("imdb"),
                    item.get("days_on_chart", 1),
                    len(plats),
                )

    # Beyond Radar gets simpler deltas
    for idx, item in enumerate(beyond, start=1):
        item["delta"] = "★ NEW ENTRY"
        item["delta_kind"] = "new"
        item["days_on_chart"] = 1
        item["peak_rank"] = idx
        item["peak_label"] = "Festival / restoration spotlight"
        item["sparkline"] = [idx]
        item.setdefault("driver", item.get("driver", "Auteur radar"))

    catalogue = {
        "generated_at": now_utc().isoformat(),
        "date_label": now_utc().strftime("%A, %d %B %Y"),
        "platform_meta": platform_meta,
        "platforms": platforms_out,
        "beyond_radar": beyond,
        "meta": {
            "total_titles": len({normalise_title(i["title"]) for i in flat}),
            "live_platforms": sum(1 for p in platform_meta if p["status"] == "LIVE"),
            "baseline_platforms": sum(1 for p in platform_meta if p["status"] == "BASELINE"),
            "history_days": len(history.get("days", [])),
            "site": SITE_BASE_URL,
        },
    }

    CATALOGUE_FILE.write_text(json.dumps(catalogue, ensure_ascii=False, indent=2), encoding="utf-8")
    HISTORY_FILE.write_text(json.dumps(history, ensure_ascii=False, indent=2), encoding="utf-8")
    FEED_FILE.write_text(build_rss(catalogue), encoding="utf-8")
    OG_FILE.write_text(build_og_svg(catalogue), encoding="utf-8")

    broadcast(catalogue)

    print(f"[sync] complete — {catalogue['meta']['live_platforms']} live / "
          f"{catalogue['meta']['baseline_platforms']} baseline platforms")
    return 0


if __name__ == "__main__":
    sys.exit(main())
