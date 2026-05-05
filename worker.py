#!/usr/bin/env python3
"""
worker.py — Muxivo Media Worker (Termux)
Flask worker that extracts gallery images and downloads media via yt-dlp.

Usage:
    pip install flask requests yt-dlp
    python worker.py

Endpoints:
    POST /extract   Extract gallery images from TikTok /photo/ URL
    POST /download  Download video/audio via yt-dlp (YouTube, Instagram, etc.)
    GET  /files/<f>  Serve downloaded files
    GET  /health     Health check
"""
from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
import time
import traceback
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse, urljoin, parse_qs, urlencode, urlunparse

import requests
from flask import Flask, jsonify, request, send_from_directory, abort

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
# Flask routes — /extract (TikTok photos)
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


# ---------------------------------------------------------------------------
# Download config
# ---------------------------------------------------------------------------
WORKER_DOWNLOADS_DIR = Path("worker_downloads")
WORKER_DOWNLOADS_DIR.mkdir(exist_ok=True)
DOWNLOAD_TIMEOUT = 120  # max seconds for a single yt-dlp download
MAX_DOWNLOAD_FILES = 50  # max files kept before cleanup

# YouTube query params to strip for normalization
_YT_STRIP_PARAMS = {
    "list", "start_radio", "rv", "index", "si",
    "feature", "pp", "ab_channel",
}


def _normalize_youtube_url(url: str) -> str:
    """Strip playlist/radio params from YouTube URLs to get a clean single-video URL."""
    try:
        p = urlparse(url)
        host = (p.netloc or "").lower()
    except Exception:
        return url

    # Only touch YouTube domains
    if "youtube.com" not in host and "youtu.be" not in host:
        return url

    # youtu.be/VIDEO_ID -> https://www.youtube.com/watch?v=VIDEO_ID
    if "youtu.be" in host:
        vid = (p.path or "").strip("/").split("/")[0]
        if vid:
            return f"https://www.youtube.com/watch?v={vid}"
        return url

    # youtube.com/watch?v=... -> strip junk params
    qs = parse_qs(p.query, keep_blank_values=False)
    video_id = qs.get("v", [None])[0]
    if not video_id:
        return url

    cleaned = {k: v[0] for k, v in qs.items() if k not in _YT_STRIP_PARAMS and v}
    cleaned["v"] = video_id
    new_query = urlencode(cleaned)
    normalized = urlunparse((p.scheme, p.netloc, p.path, "", new_query, ""))

    return normalized


def _sanitize_filename(name: str) -> str:
    """Remove dangerous characters from filenames."""
    name = str(name or "download").strip()
    # Remove path separators and null bytes
    name = name.replace("/", "_").replace("\\", "_").replace("\x00", "")
    name = re.sub(r'[<>:"|?*]', "_", name)
    # Collapse whitespace
    name = re.sub(r"\s+", " ", name).strip()
    # Limit length
    if len(name) > 200:
        stem, _, ext = name.rpartition(".")
        if ext and len(ext) <= 5:
            name = stem[:190] + "." + ext
        else:
            name = name[:200]
    return name or "download"


def _cleanup_old_downloads(keep: int = MAX_DOWNLOAD_FILES) -> None:
    """Remove oldest files if download dir exceeds limit."""
    try:
        files = sorted(
            [f for f in WORKER_DOWNLOADS_DIR.iterdir() if f.is_file()],
            key=lambda f: f.stat().st_mtime,
        )
        if len(files) > keep:
            for f in files[: len(files) - keep]:
                try:
                    f.unlink()
                    print(f"[WORKER] cleanup: removed {f.name}", flush=True)
                except Exception:
                    pass
    except Exception:
        pass


def _detect_kind_from_path(p: Path) -> str:
    """Detect file kind from extension."""
    ext = (p.suffix or "").lower().lstrip(".")
    if ext in ("mp4", "mkv", "webm", "avi", "mov", "flv", "ts"):
        return "video"
    if ext in ("mp3", "m4a", "ogg", "opus", "wav", "flac", "aac", "wma"):
        return "audio"
    if ext in ("jpg", "jpeg", "png", "webp", "gif", "bmp", "avif"):
        return "image"
    return "file"


def _get_worker_file_url(filename: str) -> str:
    """Build the full URL for a worker file.
    Uses request.host to auto-detect the correct IP/port."""
    try:
        # Use the actual host from the incoming request
        host = request.host  # e.g. "100.70.78.80:5001"
        scheme = request.scheme or "http"
        return f"{scheme}://{host}/files/{filename}"
    except Exception:
        return f"http://100.70.78.80:{LISTEN_PORT}/files/{filename}"


def _download_with_ytdlp(
    url: str,
    kind: str = "video",
    fmt: str = "mp4",
    quality: str = "720",
) -> List[Dict[str, str]]:
    """Download media with yt-dlp and return list of file info dicts."""

    _cleanup_old_downloads()

    # Normalize YouTube URLs
    original_url = url
    if "youtube.com" in url.lower() or "youtu.be" in url.lower():
        url = _normalize_youtube_url(url)
        if url != original_url:
            print(f"[WORKER] download normalized={url}", flush=True)

    # Build unique output filename template
    ts = int(time.time())
    rand = os.urandom(3).hex()
    outtmpl = str(WORKER_DOWNLOADS_DIR / f"dl_{ts}_{rand}_%(title).80s.%(ext)s")

    # Format selection based on kind
    if kind == "audio":
        format_str = "bestaudio[ext=m4a]/bestaudio/best"
    else:
        # Video with height constraint
        h = 720
        try:
            h = int(quality) if quality and quality.lower() != "best" else 9999
        except (ValueError, TypeError):
            h = 720
        format_str = (
            f"bv*[height<={h}][ext=mp4]+ba[ext=m4a]/"
            f"bv*[height<={h}]+ba/"
            f"b[height<={h}][ext=mp4]/"
            f"best[height<={h}]/"
            f"best"
        )

    # Build yt-dlp command
    cmd: List[str] = [
        sys.executable, "-m", "yt_dlp",
        "--no-playlist",
        "--no-warnings",
        "--socket-timeout", "30",
        "--retries", "3",
        "-f", format_str,
        "-o", outtmpl,
    ]

    # Merge to mp4 for video
    if kind == "video":
        cmd.extend(["--merge-output-format", fmt or "mp4"])

    # Audio post-processing
    if kind == "audio":
        target_codec = fmt if fmt in ("mp3", "m4a", "ogg", "opus", "wav", "flac") else "mp3"
        cmd.extend([
            "--extract-audio",
            "--audio-format", target_codec,
            "--audio-quality", "192K",
        ])

    cmd.append(url)

    print(f"[WORKER] download cmd={' '.join(cmd[:6])}... {url}", flush=True)

    # Track files before download
    before = set(WORKER_DOWNLOADS_DIR.iterdir()) if WORKER_DOWNLOADS_DIR.exists() else set()

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=DOWNLOAD_TIMEOUT,
            cwd=str(WORKER_DOWNLOADS_DIR),
        )
        if result.returncode != 0:
            stderr = (result.stderr or "").strip()
            stdout = (result.stdout or "").strip()
            error_text = stderr or stdout or "Unknown yt-dlp error"
            # Log last 3 lines of error
            error_lines = error_text.splitlines()[-3:]
            for line in error_lines:
                print(f"[WORKER] yt-dlp error: {line}", flush=True)
            return []
    except subprocess.TimeoutExpired:
        print(f"[WORKER] download timeout after {DOWNLOAD_TIMEOUT}s", flush=True)
        return []
    except FileNotFoundError:
        print("[WORKER] yt-dlp not found! Install with: pip install yt-dlp", flush=True)
        return []
    except Exception as e:
        print(f"[WORKER] download exception: {type(e).__name__}: {e}", flush=True)
        return []

    # Find new files
    after = set(WORKER_DOWNLOADS_DIR.iterdir()) if WORKER_DOWNLOADS_DIR.exists() else set()
    new_files = sorted(
        [f for f in (after - before) if f.is_file() and f.stat().st_size > 100],
        key=lambda f: f.stat().st_mtime,
        reverse=True,
    )

    if not new_files:
        print("[WORKER] download produced no files", flush=True)
        return []

    # Build response
    files_out: List[Dict[str, str]] = []
    for f in new_files:
        safe_name = _sanitize_filename(f.name)
        # Rename if needed
        if safe_name != f.name:
            new_path = f.parent / safe_name
            try:
                f.rename(new_path)
                f = new_path
            except Exception:
                safe_name = f.name

        file_kind = _detect_kind_from_path(f)
        file_url = _get_worker_file_url(safe_name)

        files_out.append({
            "name": safe_name,
            "url": file_url,
            "kind": file_kind,
            "size": f.stat().st_size,
        })
        print(f"[WORKER] download file: {safe_name} ({f.stat().st_size} bytes) kind={file_kind}", flush=True)

    return files_out


# ---------------------------------------------------------------------------
# Flask routes — /download (yt-dlp)
# ---------------------------------------------------------------------------


@app.route("/download", methods=["POST"])
def handle_download():
    try:
        data = request.get_json(force=True, silent=True) or {}
    except Exception:
        data = {}

    url = str(data.get("url") or "").strip()
    if not url:
        return jsonify({"ok": False, "files": [], "message": "Missing 'url' parameter"}), 400

    kind = str(data.get("kind") or "video").strip().lower()
    if kind not in ("video", "audio"):
        kind = "video"

    fmt = str(data.get("format") or ("mp4" if kind == "video" else "mp3")).strip().lower()
    quality = str(data.get("quality") or "720").strip()

    print(f"[WORKER] download input={url}", flush=True)
    print(f"[WORKER] download kind={kind} format={fmt} quality={quality}", flush=True)

    try:
        files = _download_with_ytdlp(url, kind=kind, fmt=fmt, quality=quality)
    except Exception as exc:
        print(f"[WORKER] download ERROR: {exc}", flush=True)
        traceback.print_exc()
        return jsonify({
            "ok": False,
            "files": [],
            "message": f"Download failed: {type(exc).__name__}: {exc}",
            "url": url,
        })

    if not files:
        return jsonify({
            "ok": False,
            "files": [],
            "message": "yt-dlp produced no output files.",
            "url": url,
        })

    print(f"[WORKER] download ok files={len(files)}", flush=True)
    return jsonify({
        "ok": True,
        "files": files,
        "message": f"Worker Termux: {len(files)} file(s) downloaded",
        "url": url,
    })


# ---------------------------------------------------------------------------
# Flask routes — /files/<filename> (serve downloads)
# ---------------------------------------------------------------------------


@app.route("/files/<path:filename>", methods=["GET"])
def serve_file(filename: str):
    # Security: prevent path traversal
    safe = str(filename or "").strip()
    if not safe or ".." in safe or "/" in safe or "\\" in safe or "\x00" in safe:
        print(f"[WORKER] file BLOCKED (traversal): {repr(filename)}", flush=True)
        abort(400)

    safe = _sanitize_filename(safe)
    filepath = (WORKER_DOWNLOADS_DIR / safe).resolve()

    # Ensure it stays within downloads dir
    if not str(filepath).startswith(str(WORKER_DOWNLOADS_DIR.resolve())):
        print(f"[WORKER] file BLOCKED (escape): {repr(filename)}", flush=True)
        abort(403)

    if not filepath.exists() or not filepath.is_file():
        print(f"[WORKER] file NOT FOUND: {safe}", flush=True)
        abort(404)

    print(f"[WORKER] file served={safe} ({filepath.stat().st_size} bytes)", flush=True)
    return send_from_directory(
        str(WORKER_DOWNLOADS_DIR.resolve()),
        safe,
        as_attachment=False,
    )


# ---------------------------------------------------------------------------
# Flask routes — health & index
# ---------------------------------------------------------------------------


@app.route("/health", methods=["GET"])
def health():
    # Quick yt-dlp availability check
    ytdlp_ok = False
    try:
        result = subprocess.run(
            [sys.executable, "-m", "yt_dlp", "--version"],
            capture_output=True, text=True, timeout=5,
        )
        ytdlp_ok = result.returncode == 0
        ytdlp_version = (result.stdout or "").strip() if ytdlp_ok else None
    except Exception:
        ytdlp_ok = False
        ytdlp_version = None

    dl_count = len(list(WORKER_DOWNLOADS_DIR.iterdir())) if WORKER_DOWNLOADS_DIR.exists() else 0

    return jsonify({
        "ok": True,
        "service": "muxivo-media-worker",
        "yt_dlp": ytdlp_ok,
        "yt_dlp_version": ytdlp_version,
        "downloads_count": dl_count,
    })


@app.route("/", methods=["GET"])
def index():
    return jsonify(
        {
            "ok": True,
            "service": "muxivo-media-worker",
            "endpoints": {
                "POST /extract": "Extract gallery images from TikTok /photo/ URL",
                "POST /download": "Download video/audio via yt-dlp",
                "GET /files/<name>": "Serve downloaded files",
                "GET /health": "Health check with yt-dlp status",
            },
        }
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    WORKER_DOWNLOADS_DIR.mkdir(exist_ok=True)
    print(f"[WORKER] Starting Muxivo Media Worker on {LISTEN_HOST}:{LISTEN_PORT}", flush=True)
    print(f"[WORKER] Downloads dir: {WORKER_DOWNLOADS_DIR.resolve()}", flush=True)

    # Check yt-dlp on startup
    try:
        r = subprocess.run(
            [sys.executable, "-m", "yt_dlp", "--version"],
            capture_output=True, text=True, timeout=5,
        )
        if r.returncode == 0:
            print(f"[WORKER] yt-dlp version: {(r.stdout or '').strip()}", flush=True)
        else:
            print("[WORKER] WARNING: yt-dlp not working!", flush=True)
    except Exception:
        print("[WORKER] WARNING: yt-dlp not found! pip install yt-dlp", flush=True)

    app.run(host=LISTEN_HOST, port=LISTEN_PORT, debug=False)
