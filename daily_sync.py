#!/usr/bin/env python3
"""
Master Catalogue — daily_sync.py
Live daily charts fetcher via FlixPatrol + Fallback system.
"""

from __future__ import annotations

import html
import json
import os
import re
import sys
import time
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT / "docs"
OUTPUT_DIR.mkdir(exist_ok=True)
CATALOGUE_FILE = OUTPUT_DIR / "daily_catalogue.json"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}

# Mapping our internal keys to FlixPatrol's URL slugs
FLIXPATROL_SLUGS = {
    "netflix": "netflix",
    "prime": "amazon-prime",
    "hotstar": "hotstar",
    "sonyliv": "sonyliv",
    "zee5": "zee5",
    "hbo": "max",
    "disney": "disney-plus",
    "appletv": "apple-tv-plus"
}

PLATFORMS = [
    # India
    {"name": "Netflix India", "key": "netflix", "region": "IN", "display": "Netflix"},
    {"name": "Amazon Prime Video India", "key": "prime", "region": "IN", "display": "Prime Video"},
    {"name": "JioHotstar", "key": "hotstar", "region": "IN", "display": "JioHotstar"},
    {"name": "SonyLIV", "key": "sonyliv", "region": "IN", "display": "SonyLIV"},
    {"name": "ZEE5", "key": "zee5", "region": "IN", "display": "ZEE5"},
    # Global
    {"name": "Netflix Global", "key": "netflix", "region": "GLOBAL", "display": "Netflix"},
    {"name": "Amazon Prime Video Global", "key": "prime", "region": "GLOBAL", "display": "Prime Video"},
    {"name": "HBO Max", "key": "hbo", "region": "GLOBAL", "display": "HBO Max"},
    {"name": "Disney+", "key": "disney", "region": "GLOBAL", "display": "Disney+"},
    {"name": "Apple TV+", "key": "appletv", "region": "GLOBAL", "display": "Apple TV+"},
]

def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def http_get(url: str, timeout: int = 15) -> requests.Response | None:
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout)
        if r.status_code == 200:
            return r
    except Exception:
        pass
    return None

def normalise_title(t: str) -> str:
    t = html.unescape(t or "")
    t = re.sub(r"\s+", " ", t).strip().lower()
    return re.sub(r"^[\W_]+|[\W_]+$", "", t)

def fetch_flixpatrol_chart(platform_key: str, region: str) -> list[dict[str, Any]]:
    """Fetch daily Top 10 from FlixPatrol for a specific platform and region."""
    slug = FLIXPATROL_SLUGS.get(platform_key)
    if not slug:
        return []

    # FlixPatrol URL structure: /top10/{platform}/{country}/daily/
    country_code = "india" if region == "IN" else "world"
    url = f"https://flixpatrol.com/top10/{slug}/{country_code}/daily/"
    
    r = http_get(url)
    if not r:
        return []

    # Parse the HTML to find the chart rows
    items = []
    
    # Find the main table containing the ranks
    # FlixPatrol uses a clean structure with numbers and titles
    title_matches = re.findall(r'<div class="[^"]*title[^"]*">\s*([^<]+)\s*</div>', r.text)
    
    # Fallback extractor for rows
    if not title_matches:
        # Generic extraction of table rows
        rows = re.findall(r'<tr[^>]*>.*?</tr>', r.text, re.DOTALL)
        for row in rows:
            # Find the rank number
            rank_match = re.search(r'<td[^>]*class="[^"]*rank[^"]*"[^>]*>\s*(\d+)\s*</td>', row)
            # Find the title anchor
            title_match = re.search(r'<a[^>]*class="[^"]*title[^"]*"[^>]*>\s*([^<]+)\s*</a>', row)
            
            if rank_match and title_match:
                rank = int(rank_match.group(1))
                title = html.unescape(title_match.group(1).strip())
                if rank <= 10 and title:
                    items.append({
                        "rank": rank,
                        "title": title,
                        "year": None,
                        "type": "movie", # Default, FlixPatrol doesn't always differentiate easily here
                        "imdb": None
                    })
    
    # Sort by rank and cap at 10
    items.sort(key=lambda x: x["rank"])
    return items[:10]

def main():
    print("[sync] starting live daily chart sync...")
    
    catalogue = {
        "generated_at": now_utc().isoformat(),
        "date_label": now_utc().strftime("%A, %d %B %Y"),
        "platforms": {},
        "meta": {"total_titles": 0, "live_platforms": 0, "baseline_platforms": 0}
    }

    for p in PLATFORMS:
        print(f"[sync] fetching {p['name']}...")
        items = fetch_flixpatrol_chart(p["key"], p["region"])
        
        # If live fetching fails, we fall back to a safe shell so the app doesn't break
        if not items:
            print(f"[sync] -> WARNING: Live fetch failed for {p['name']}. Using fallback.")
            items = [{"rank": i, "title": f"Awaiting Sync #{i}", "year": None, "type": "movie", "imdb": None} for i in range(1, 11)]
            status = "BASELINE"
        else:
            status = "LIVE"
            catalogue["meta"]["live_platforms"] += 1
            
        # Attach the status to each item for the UI badge
        for item in items:
            item["status"] = status
            
        catalogue["platforms"][p["name"]] = items
        catalogue["meta"]["total_titles"] += len(items)
        
        # Respect rate limits
        time.sleep(1.5)

    CATALOGUE_FILE.write_text(json.dumps(catalogue, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[sync] complete — {catalogue['meta']['live_platforms']} live platforms.")

if __name__ == "__main__":
    main()
