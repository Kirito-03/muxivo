#!/usr/bin/env python3
"""
worker.py — TikTok Photo Gallery Extractor (Termux)
Flask worker that extracts real gallery images from TikTok /photo/ posts.

Usage:
    pip install flask requests
    python worker.py

Endpoint:
    POST /extract  {"url": "https://vt.tiktok.com/..."}
"""
from __future__ import annotations

import json
import re
import sys
import time
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse, urljoin

import requests
from flask import Flask, jsonify, request

app = Flask(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
LISTEN_HOST = "0.0.0.0"
LISTEN_PORT = 5001
REQUEST_TIMEOUT = 18  # seconds for outgoing HTTP
MAX_ITEMS = 48

_UA = (
    "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36"
)

_DESKTOP_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

_HEADERS_MOBILE = {
    "User-Agent": _UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.tiktok.com/",
}

_HEADERS_DESKTOP = {
    "User-Agent": _DESKTOP_UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.tiktok.com/",
}

# ---------------------------------------------------------------------------
# URL helpers
# ---------------------------------------------------------------------------
_SHORTLINK_HOSTS = {"vt.tiktok.com", "vm.tiktok.com", "t.tiktok.com"}


def _is_shortlink(url: str) -> bool:
    try:
        host = urlparse(url).netloc.lower()
    except Exception:
        return False
    return host in _SHORTLINK_HOSTS


def _resolve_shortlink(url: str) -> str:
    """Follow redirects to get the canonical TikTok URL."""
    try:
        resp = requests.get(
            url,
            headers=_HEADERS_MOBILE,
            allow_redirects=True,
            timeout=REQUEST_TIMEOUT,
        )
        final = resp.url or url
        print(f"[WORKER] resolved {url} -> {final}", flush=True)
        return final
    except Exception as exc:
        print(f"[WORKER] resolve failed: {exc}", flush=True)
        return url


def _fetch_html(url: str, *, mobile: bool = True) -> Tuple[str, str]:
    """Download page HTML. Returns (html, final_url)."""
    hdrs = dict(_HEADERS_MOBILE if mobile else _HEADERS_DESKTOP)
    try:
        resp = requests.get(url, headers=hdrs, timeout=REQUEST_TIMEOUT, allow_redirects=True)
        resp.raise_for_status()
        return resp.text, resp.url
    except Exception as exc:
        print(f"[WORKER] fetch_html failed: {exc}", flush=True)
        return "", url


# ---------------------------------------------------------------------------
# Image URL filters
# ---------------------------------------------------------------------------
_CDN_DOMAINS = (
    "tiktokcdn.com",
    "tiktokcdn-us.com",
    "tiktokcdn-eu.com",
    "tiktokcdn-in.com",
    "p16-sign-sg.tiktokcdn.com",
    "p16-sign-va.tiktokcdn.com",
    "p16-sign.tiktokcdn-us.com",
    "p77-sign.tiktokcdn-us.com",
    "p19-sign.tiktokcdn-us.com",
)

_REJECT_PATTERNS = (
    "avatar",
    "icon",
    "emoji",
    "sticker",
    "placeholder",
    "default_",
    "100x100",
    "168x168",
    "720x720",  # typical avatar size
    "musically",
    "/obj/musically",
    "watermark",
    "/tos-alisg-i-",  # avatar path segments
)

_IMAGE_URL_RE = re.compile(
    r'https?://[a-z0-9\-_.]+(?:tiktokcdn(?:-[a-z]{2,4})?\.com)[^\s"\'<>\\}{)]*',
    re.IGNORECASE,
)


def _is_valid_photo_url(url: str) -> bool:
    """Return True if url looks like a real gallery photo (not avatar/icon)."""
    if not url:
        return False
    low = url.lower()
    # Must come from a CDN domain
    if not any(d in low for d in ("tiktokcdn.com",)):
        return False
    # Reject avatars, icons, etc
    if any(p in low for p in _REJECT_PATTERNS):
        return False
    # Must look like an image path (jpeg/webp/png or image-related path)
    is_image_ext = any(
        ext in low for ext in (".jpeg", ".jpg", ".png", ".webp", ".avif")
    )
    is_image_path = any(
        seg in low
        for seg in (
            "/photo/",
            "/image/",
            "image_post",
            "/img/",
            "photomode",
            "photo-mode",
            "tos-maliva",
            "tos-useast",
            "tos-alisg",
            "tos-",
            "/obj/",
        )
    )
    return is_image_ext or is_image_path


def _clean_image_url(url: str) -> str:
    """Unescape and clean URL."""
    u = url.replace("\\u002F", "/").replace("\\/", "/").replace("\\u0026", "&")
    u = u.replace("&amp;", "&")
    # Remove trailing junk
    for ch in ('"', "'", "}", ")", "]", ";", ",", " ", "\n", "\r", "\t"):
        u = u.rstrip(ch)
    return u.strip()


def _normalize_for_dedup(url: str) -> str:
    """Strip query params for dedup comparison."""
    try:
        p = urlparse(url)
        return f"{p.scheme}://{p.netloc}{p.path}"
    except Exception:
        return url


# ---------------------------------------------------------------------------
# Extraction strategies
# ---------------------------------------------------------------------------


def _extract_from_rehydration(html: str) -> List[str]:
    """Parse __UNIVERSAL_DATA_FOR_REHYDRATION__ script block."""
    marker = "__UNIVERSAL_DATA_FOR_REHYDRATION__"
    idx = html.find(marker)
    if idx < 0:
        return []

    # Find the JSON object after the marker
    start = html.find("{", idx)
    if start < 0:
        return []

    # Brace-match to find end of JSON
    depth = 0
    end = start
    for i in range(start, min(start + 500_000, len(html))):
        ch = html[i]
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                end = i + 1
                break
    if end <= start:
        return []

    raw = html[start:end]
    try:
        data = json.loads(raw)
    except Exception:
        return []

    return _walk_json_for_images(data)


def _extract_from_sigi_state(html: str) -> List[str]:
    """Parse SIGI_STATE script block."""
    marker = "SIGI_STATE"
    idx = html.find(marker)
    if idx < 0:
        return []

    start = html.find("{", idx)
    if start < 0:
        return []

    depth = 0
    end = start
    for i in range(start, min(start + 500_000, len(html))):
        ch = html[i]
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                end = i + 1
                break
    if end <= start:
        return []

    raw = html[start:end]
    try:
        data = json.loads(raw)
    except Exception:
        return []

    return _walk_json_for_images(data)


def _extract_from_next_data(html: str) -> List[str]:
    """Parse __NEXT_DATA__ script block (older TikTok pages)."""
    marker = "__NEXT_DATA__"
    idx = html.find(marker)
    if idx < 0:
        return []

    start = html.find("{", idx)
    if start < 0:
        return []

    depth = 0
    end = start
    for i in range(start, min(start + 500_000, len(html))):
        ch = html[i]
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                end = i + 1
                break
    if end <= start:
        return []

    raw = html[start:end]
    try:
        data = json.loads(raw)
    except Exception:
        return []

    return _walk_json_for_images(data)


def _walk_json_for_images(obj: Any, _depth: int = 0) -> List[str]:
    """Recursively walk JSON and collect image URLs from known keys."""
    if _depth > 30:
        return []
    results: List[str] = []

    if isinstance(obj, str):
        cleaned = _clean_image_url(obj)
        if _is_valid_photo_url(cleaned):
            results.append(cleaned)
        return results

    if isinstance(obj, list):
        for item in obj:
            results.extend(_walk_json_for_images(item, _depth + 1))
        return results

    if isinstance(obj, dict):
        # Prioritized keys for photo gallery images
        priority_keys = (
            "imagePost",
            "images",
            "imageList",
            "photo",
            "photoImages",
            "slides",
            "carousel",
            "coverImage",
            "originCover",
            "dynamicCover",
        )
        for pk in priority_keys:
            if pk in obj:
                results.extend(_walk_json_for_images(obj[pk], _depth + 1))

        # Also check all string values that look like image URLs
        for key, val in obj.items():
            low_key = str(key).lower()
            # Skip avatar keys
            if any(skip in low_key for skip in ("avatar", "icon", "logo", "nickname", "uniqueid")):
                continue
            if isinstance(val, str):
                cleaned = _clean_image_url(val)
                if _is_valid_photo_url(cleaned):
                    results.append(cleaned)
            elif isinstance(val, (dict, list)):
                # Don't re-walk priority keys
                if key not in priority_keys:
                    results.extend(_walk_json_for_images(val, _depth + 1))

    return results


def _extract_from_regex(html: str) -> List[str]:
    """Fallback: regex scan for tiktokcdn image URLs."""
    results: List[str] = []
    for m in _IMAGE_URL_RE.finditer(html):
        raw = _clean_image_url(m.group(0))
        if _is_valid_photo_url(raw):
            results.append(raw)
    return results


def _extract_og_image(html: str) -> List[str]:
    """Extract og:image meta tag as last resort."""
    pattern = re.compile(
        r'<meta\s+(?:[^>]*?)property=["\']og:image["\']\s+content=["\']([^"\']+)["\']',
        re.IGNORECASE,
    )
    results: List[str] = []
    for m in pattern.finditer(html):
        url = _clean_image_url(m.group(1))
        if url and url.startswith("http"):
            results.append(url)
    # Also try reversed attribute order
    pattern2 = re.compile(
        r'<meta\s+(?:[^>]*?)content=["\']([^"\']+)["\']\s+property=["\']og:image["\']',
        re.IGNORECASE,
    )
    for m in pattern2.finditer(html):
        url = _clean_image_url(m.group(1))
        if url and url.startswith("http"):
            results.append(url)
    return results


# ---------------------------------------------------------------------------
# Dedup & rank
# ---------------------------------------------------------------------------


def _dedup_images(urls: List[str]) -> List[str]:
    """Deduplicate URLs, preserving order."""
    seen: set = set()
    out: List[str] = []
    for u in urls:
        key = _normalize_for_dedup(u)
        if key in seen:
            continue
        seen.add(key)
        out.append(u)
    return out


def _score_image_url(url: str) -> int:
    """Higher score = more likely to be a real gallery image."""
    score = 0
    low = url.lower()
    # Prefer high-resolution indicators
    if "origin" in low or "original" in low:
        score += 10
    if "1080" in low or "high" in low:
        score += 5
    if "jpeg" in low or "webp" in low:
        score += 3
    # Prefer image-post paths
    if "image_post" in low or "photomode" in low or "photo-mode" in low:
        score += 8
    # Penalize small/thumbnail indicators
    if "100w" in low or "200w" in low or "thumb" in low:
        score -= 5
    if "720x720" in low:
        score -= 10
    return score


# ---------------------------------------------------------------------------
# Main extraction pipeline
# ---------------------------------------------------------------------------


def extract_gallery(input_url: str) -> List[Dict[str, str]]:
    """Full pipeline: resolve -> fetch -> extract -> dedup -> return items."""
    t0 = time.time()
    print(f"[WORKER] input_url={input_url}", flush=True)

    # 1. Resolve shortlink
    url = input_url
    if _is_shortlink(url):
        url = _resolve_shortlink(url)
    print(f"[WORKER] final_url={url}", flush=True)

    # 2. Fetch HTML (try mobile first, then desktop)
    html, final_url = _fetch_html(url, mobile=True)
    if not html or len(html) < 500:
        html, final_url = _fetch_html(url, mobile=False)
    if not html:
        print("[WORKER] empty HTML, aborting", flush=True)
        return []

    print(f"[WORKER] html_size={len(html)}", flush=True)

    # 3. Extract from all strategies
    all_urls: List[str] = []

    # Strategy 1: __UNIVERSAL_DATA_FOR_REHYDRATION__
    rehydration = _extract_from_rehydration(html)
    print(f"[WORKER] rehydration_candidates={len(rehydration)}", flush=True)
    all_urls.extend(rehydration)

    # Strategy 2: SIGI_STATE
    sigi = _extract_from_sigi_state(html)
    print(f"[WORKER] sigi_candidates={len(sigi)}", flush=True)
    all_urls.extend(sigi)

    # Strategy 3: __NEXT_DATA__
    next_data = _extract_from_next_data(html)
    print(f"[WORKER] next_data_candidates={len(next_data)}", flush=True)
    all_urls.extend(next_data)

    # Strategy 4: Regex fallback
    regex = _extract_from_regex(html)
    print(f"[WORKER] regex_candidates={len(regex)}", flush=True)
    all_urls.extend(regex)

    # Strategy 5: og:image (only if nothing else found)
    if not all_urls:
        og = _extract_og_image(html)
        print(f"[WORKER] og_image_candidates={len(og)}", flush=True)
        all_urls.extend(og)

    # If mobile got nothing, try desktop
    if not all_urls and final_url:
        print("[WORKER] retrying with desktop UA", flush=True)
        html2, _ = _fetch_html(url, mobile=False)
        if html2 and len(html2) > 500:
            all_urls.extend(_extract_from_rehydration(html2))
            all_urls.extend(_extract_from_sigi_state(html2))
            all_urls.extend(_extract_from_next_data(html2))
            all_urls.extend(_extract_from_regex(html2))
            if not all_urls:
                all_urls.extend(_extract_og_image(html2))

    # 4. Dedup
    unique = _dedup_images(all_urls)
    print(f"[WORKER] candidates={len(unique)}", flush=True)

    # 5. Sort by quality score
    unique.sort(key=_score_image_url, reverse=True)

    # 6. Build items
    items: List[Dict[str, str]] = []
    for idx, img_url in enumerate(unique[:MAX_ITEMS], start=1):
        items.append(
            {
                "url": img_url,
                "thumb": img_url,
                "label": f"IMAGE {idx}",
                "kind": "image",
            }
        )

    elapsed = time.time() - t0
    print(f"[WORKER] returning={len(items)} elapsed={elapsed:.1f}s", flush=True)
    return items


# ---------------------------------------------------------------------------
# Flask routes
# ---------------------------------------------------------------------------


@app.route("/extract", methods=["POST"])
def handle_extract():
    try:
        data = request.get_json(force=True, silent=True) or {}
    except Exception:
        data = {}

    url = str(data.get("url") or "").strip()
    if not url:
        return jsonify({"ok": False, "items": [], "message": "Missing 'url' parameter"}), 400

    try:
        items = extract_gallery(url)
    except Exception as exc:
        print(f"[WORKER] ERROR: {exc}", flush=True)
        import traceback
        traceback.print_exc()
        return jsonify(
            {
                "ok": True,
                "items": [],
                "message": f"Extraction failed: {exc}",
                "url": url,
            }
        )

    return jsonify(
        {
            "ok": True,
            "items": items,
            "message": f"Worker Termux: {len(items)} images extracted",
            "url": url,
        }
    )


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"ok": True, "service": "tiktok-photo-worker"})


@app.route("/", methods=["GET"])
def index():
    return jsonify(
        {
            "ok": True,
            "service": "tiktok-photo-worker",
            "endpoints": {
                "POST /extract": "Extract gallery images from TikTok /photo/ URL",
                "GET /health": "Health check",
            },
        }
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print(f"[WORKER] Starting on {LISTEN_HOST}:{LISTEN_PORT}", flush=True)
    app.run(host=LISTEN_HOST, port=LISTEN_PORT, debug=False)
