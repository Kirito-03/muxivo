#!/usr/bin/env python3
"""
worker_universal.py — Universal Media Worker (Termux)
======================================================
Sirve a:
  - Vibe no Sekai: búsqueda + descarga audio (fallback cuando Convert falla)
  - Sistema descargador: descarga video/audio/imagen, extracción de galerías TikTok/Instagram

Endpoints:
  GET  /                      Info del servicio
  GET  /health                Estado detallado con capabilities
  POST /search                Búsqueda de música en YouTube (sin descargar)
  POST /download              Descarga video / audio / imagen
  POST /extract               Extrae imágenes de TikTok gallery / Instagram
  POST /cleanup               Limpieza manual de archivos viejos
  GET  /files/<filename>      Sirve archivos descargados

Puerto: 0.0.0.0:5001

Uso en Termux:
  pip install flask requests yt-dlp
  python worker_universal.py
"""
from __future__ import annotations

import hashlib
import ipaddress
import json
import os
import re
import shutil
import subprocess
import sys
import threading
import time
import traceback
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, quote, urlparse, urljoin, urlunparse

import requests
from flask import Flask, jsonify, request, send_from_directory, abort

# ---------------------------------------------------------------------------
# Versión y configuración global
# ---------------------------------------------------------------------------
VERSION = "2.1.0"
LISTEN_HOST = os.getenv("LISTEN_HOST", "0.0.0.0")
LISTEN_PORT = int(os.getenv("LISTEN_PORT", "5001"))

WORKER_TOKEN = os.getenv("WORKER_TOKEN", "").strip()

# Modo del worker: "vibe" = solo audio | "muxivo" o vacío = todo (audio+video+imagen)
# Ejemplo: WORKER_MODE=vibe python worker.py
WORKER_MODE = os.getenv("WORKER_MODE", "").strip().lower()

# Cookies para YouTube/Instagram/TikTok (evita detección de bots y permite posts con login)
# Opción A: ruta a un archivo cookies.txt exportado desde el navegador (o ~/cookies.txt en Termux)
# Opción B: nombre del navegador para usar --cookies-from-browser (ej: "chrome", "firefox")
WORKER_COOKIES_FILE = os.getenv("WORKER_COOKIES_FILE", "").strip()
WORKER_COOKIES_FROM_BROWSER = os.getenv("WORKER_COOKIES_FROM_BROWSER", "").strip()


def _resolve_cookies_path() -> Optional[Path]:
    """Busca cookies.txt en WORKER_COOKIES_FILE o ubicaciones estándar de Termux/proyecto."""
    candidates: List[Path] = []
    if WORKER_COOKIES_FILE:
        candidates.append(Path(WORKER_COOKIES_FILE).expanduser())
    candidates.extend([
        Path.home() / "cookies.txt",
        Path("cookies.txt"),
        Path("data/cookies.txt"),
        Path("www.instagram.com_cookies.txt"),
        Path("cookies/instagram.txt"),
        Path("www.youtube.com_cookies.txt"),
        Path("cookies/youtube.txt"),
        Path("www.tiktok.com_cookies.txt"),
        Path("cookies/tiktok.txt"),
    ])
    for p in candidates:
        try:
            if p.exists() and p.is_file() and p.stat().st_size > 10:
                return p.resolve()
        except Exception:
            continue
    return None


def _cookies_args() -> List[str]:
    """Devuelve los argumentos de cookies para yt-dlp, si están configurados."""
    cp = _resolve_cookies_path()
    if cp:
        return ["--cookies", str(cp)]
    if WORKER_COOKIES_FROM_BROWSER:
        return ["--cookies-from-browser", WORKER_COOKIES_FROM_BROWSER]
    return []


def _load_cookie_jar():
    """Carga MozillaCookieJar desde cookies.txt para peticiones requests (Instagram/TikTok/Pinterest)."""
    cp = _resolve_cookies_path()
    if not cp:
        return None
    try:
        import http.cookiejar
        jar = http.cookiejar.MozillaCookieJar(str(cp))
        jar.load(ignore_discard=True, ignore_expires=True)
        return jar
    except Exception:
        return None


DOWNLOADS_DIR = Path(os.getenv("WORKER_DOWNLOADS_DIR", "worker_downloads")).resolve()
INDEX_PATH = (DOWNLOADS_DIR / "index.json").resolve()

SEARCH_CACHE_TTL_SEC = 600  # 10 minutos
SEARCH_CACHE: Dict[str, Tuple[float, Any]] = {}

PENDING_LOCK = threading.Lock()
PENDING: Dict[str, Dict[str, Any]] = {}
RESULT_TTL_SEC = 30
RESULTS: Dict[str, Tuple[float, Any]] = {}

LAST_CLEANUP_TS = 0.0
CLEANUP_INTERVAL_SEC = 180  # cada 3 minutos
MAX_FILE_AGE_SEC = 1800       # 30 minutos
MAX_TOTAL_BYTES = 2 * 1024 * 1024 * 1024  # 2 GB
MIN_FILE_AGE_PROTECT_SEC = 300  # 5 minutos

DOWNLOAD_TIMEOUT_SEC = 180

# TikTok shortlinks
_TIKTOK_SHORTLINK_HOSTS = {"vt.tiktok.com", "vm.tiktok.com", "t.tiktok.com"}

# User-agents
_UA_MOBILE = (
    "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36"
)
_UA_DESKTOP = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
_HEADERS_MOBILE = {
    "User-Agent": _UA_MOBILE,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.tiktok.com/",
}
_HEADERS_DESKTOP = {
    "User-Agent": _UA_DESKTOP,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.tiktok.com/",
}

# Extensions por tipo
_AUDIO_EXTS = {"mp3", "m4a", "opus", "ogg", "wav", "flac", "aac"}
_VIDEO_EXTS = {"mp4", "webm", "mkv", "mov", "avi", "flv", "ts"}
_IMAGE_EXTS = {"jpg", "jpeg", "png", "webp", "avif"}

# Plataformas con soporte nativo verificado (detección extra precisa)
_KNOWN_PLATFORMS = (
    "youtube.com", "youtu.be",
    "tiktok.com", "vt.tiktok.com", "vm.tiktok.com", "t.tiktok.com",
    "instagram.com",
    "soundcloud.com", "sndcdn.com",
    "spotify.com", "open.spotify.com",
    "twitter.com", "x.com", "t.co",
    "facebook.com", "fb.watch",
    "twitch.tv",
    "dailymotion.com",
    "vimeo.com",
    "reddit.com", "v.redd.it",
    "bilibili.com",
    "pinterest.com", "pin.it",
    "threads.net", "threads.com",
)

# Modo de URL: True = aceptar CUALQUIER URL pública (recomendado para uso personal)
# False = solo plataformas en _KNOWN_PLATFORMS
_OPEN_URL_POLICY = os.getenv("WORKER_OPEN_URL_POLICY", "1").strip().lower() not in ("0", "false", "no", "off")

app = Flask(__name__)

from flask import g

@app.before_request
def before_request_hook():
    g._req_t0 = time.time()
    
    # 1. Logging inicio request (estilo Muxivo)
    try:
        qs = request.query_string.decode("utf-8", errors="ignore") if request.query_string else ""
        start_line = f">> {request.method} {request.path}"
        if qs:
            start_line += f"?{qs}"
        body_preview = ""
        if request.method in ("POST", "PUT", "PATCH"):
            ct = (request.headers.get("Content-Type") or "").lower()
            if "application/json" in ct:
                data = request.get_json(silent=True)
                if isinstance(data, dict):
                    redacted = {}
                    for k, v in data.items():
                        s = str(v)
                        redacted[str(k)] = (s[:150] + "...") if len(s) > 150 else s
                    body_preview = f" body={redacted}"
            else:
                body_preview = " body=[non-json]"
        print(start_line + body_preview, flush=True)
    except Exception:
        pass

    # 2. Validación de Token
    if request.path in ("/", "/health") or request.path.startswith("/files/"):
        return None  # Rutas públicas permitidas sin token
    
    if WORKER_TOKEN:
        auth_header = request.headers.get("Authorization", "")
        token_header = request.headers.get("X-Worker-Token", "")
        
        req_token = ""
        if auth_header.startswith("Bearer "):
            req_token = auth_header[7:].strip()
        if not req_token and token_header:
            req_token = token_header.strip()
            
        if req_token != WORKER_TOKEN:
            print(f"[WORKER] Acceso denegado. Token inválido en {request.path}", flush=True)
            return jsonify({"ok": False, "message": "Unauthorized"}), 401

@app.after_request
def after_request_hook(resp):
    try:
        start = getattr(g, "_req_t0", None)
        ms = int((time.time() - start) * 1000) if start else None
        if ms is None:
            line = f"{request.method} {request.path} -> {resp.status_code}"
        else:
            line = f"{request.method} {request.path} -> {resp.status_code} ({ms}ms)"
        print(line, flush=True)
    except Exception:
        pass
    return resp

# ---------------------------------------------------------------------------
# Utilidades básicas
# ---------------------------------------------------------------------------

def _now() -> float:
    return time.time()


def _safe_json() -> Dict[str, Any]:
    try:
        data = request.get_json(force=True, silent=True) or {}
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _normalize_query(q: str) -> str:
    return " ".join(str(q or "").strip().lower().split())


# ---------------------------------------------------------------------------
# Seguridad de URLs
# ---------------------------------------------------------------------------

def _is_private_host(host: str) -> bool:
    h = (host or "").strip().lower()
    if not h:
        return True
    if h in ("localhost", "0.0.0.0", "127.0.0.1"):
        return True
    try:
        ip = ipaddress.ip_address(h)
        return bool(ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_multicast)
    except Exception:
        return False


def is_allowed_media_url(raw: str) -> bool:
    """Verifica que la URL sea pública y no sea de una red interna."""
    try:
        u = urlparse(raw)
    except Exception:
        return False
    if u.scheme not in ("http", "https"):
        return False
    # Bloquear null bytes y path traversal
    if "\x00" in raw or ".." in raw:
        return False
    host = (u.hostname or "").lower()
    if not host:
        return False
    if _is_private_host(host):
        return False
    if _OPEN_URL_POLICY:
        return True  # Acepta cualquier URL pública
    # Modo restringido: solo plataformas conocidas
    clean_host = host.replace("www.", "").replace("m.", "")
    return any(clean_host == a or clean_host.endswith(f".{a}") for a in _KNOWN_PLATFORMS)


def detect_platform(raw: str) -> str:
    """Detecta la plataforma: youtube | tiktok | instagram | soundcloud | spotify | twitter | twitch | vimeo | reddit | bilibili | facebook | unknown."""
    try:
        u = urlparse(raw)
        host = (u.hostname or "").lower().replace("www.", "").replace("m.", "")
    except Exception:
        return "unknown"
    if host in ("youtu.be",) or host.endswith("youtube.com"):
        return "youtube"
    if "tiktok" in host:
        return "tiktok"
    if "instagram" in host:
        return "instagram"
    if "soundcloud" in host or "sndcdn" in host:
        return "soundcloud"
    if "spotify" in host:
        return "spotify"
    if "twitter" in host or host in ("x.com", "t.co"):
        return "twitter"
    if "twitch" in host:
        return "twitch"
    if "vimeo" in host:
        return "vimeo"
    if "reddit" in host or host == "v.redd.it":
        return "reddit"
    if "bilibili" in host:
        return "bilibili"
    if "facebook" in host or host == "fb.watch":
        return "facebook"
    if "dailymotion" in host or host == "dai.ly":
        return "dailymotion"
    if "pinterest" in host or host == "pin.it":
        return "pinterest"
    if "threads.net" in host or "threads.com" in host:
        return "threads"
    return "other"  # yt-dlp intentará descargarlo de todos modos


def extract_youtube_id(raw: str) -> Optional[str]:
    try:
        u = urlparse(raw)
        host = (u.hostname or "").lower().replace("www.", "").replace("m.", "")
        if host == "youtu.be":
            vid = u.path.strip("/").split("/")[0]
            return vid or None
        if host.endswith("youtube.com"):
            if u.path.startswith("/watch"):
                vid = (parse_qs(u.query).get("v") or [None])[0]
                return vid or None
            if u.path.startswith("/shorts/"):
                parts = u.path.strip("/").split("/")
                return parts[1] if len(parts) > 1 else None
            if u.path.startswith("/embed/"):
                parts = u.path.strip("/").split("/")
                return parts[1] if len(parts) > 1 else None
    except Exception:
        return None
    return None


def normalize_youtube_url(raw: str) -> Optional[str]:
    vid = extract_youtube_id(raw)
    if not vid:
        return None
    return f"https://www.youtube.com/watch?v={vid}"


def is_direct_image_url(url: str) -> bool:
    try:
        path = (urlparse(url).path or "").lower()
    except Exception:
        return False
    return any(path.endswith(f".{ext}") for ext in _IMAGE_EXTS)


def sanitize_filename(name: str, fallback: str = "download") -> str:
    s = str(name or "").strip()
    s = s.replace("\x00", "")
    s = re.sub(r"[^a-zA-Z0-9._\-]+", "_", s)
    s = s.strip("._-")
    if not s:
        return fallback
    if len(s) > 180:
        stem, _, ext = s.rpartition(".")
        if ext and len(ext) <= 5:
            s = stem[:170] + "." + ext
        else:
            s = s[:180]
    return s


def detect_kind_from_path(p: Path) -> str:
    ext = (p.suffix or "").lower().lstrip(".")
    if ext in _AUDIO_EXTS:
        return "audio"
    if ext in _VIDEO_EXTS:
        return "video"
    if ext in _IMAGE_EXTS:
        return "image"
    return "file"


def _sha1(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8", errors="ignore")).hexdigest()


def _has_ffmpeg() -> bool:
    return shutil.which("ffmpeg") is not None


# ---------------------------------------------------------------------------
# Herramientas (yt-dlp + ffmpeg) — llamada rápida para /health
# ---------------------------------------------------------------------------

_tools_cache: Dict[str, Any] = {}
_tools_cache_ts = 0.0
_TOOLS_CACHE_TTL = 30.0  # 30s


def _detect_tools() -> Tuple[bool, Optional[str], bool]:
    """Detecta yt-dlp y ffmpeg con cache de 30s para no hacer llamadas pesadas en /health."""
    global _tools_cache, _tools_cache_ts
    now = _now()
    if now - _tools_cache_ts < _TOOLS_CACHE_TTL and _tools_cache:
        return _tools_cache["ytdlp_ok"], _tools_cache.get("ytdlp_version"), _tools_cache["ffmpeg_ok"]

    ytdlp_ok = False
    ytdlp_version = None
    ffmpeg_ok = False
    try:
        r = subprocess.run(
            [sys.executable, "-m", "yt_dlp", "--version"],
            capture_output=True, text=True, timeout=10
        )
        ytdlp_ok = r.returncode == 0
        ytdlp_version = (r.stdout or "").strip() if ytdlp_ok else None
    except Exception:
        ytdlp_ok = False
        ytdlp_version = None
    try:
        r2 = subprocess.run(["ffmpeg", "-version"], capture_output=True, text=True, timeout=5)
        ffmpeg_ok = r2.returncode == 0
    except Exception:
        ffmpeg_ok = False

    _tools_cache = {"ytdlp_ok": ytdlp_ok, "ytdlp_version": ytdlp_version, "ffmpeg_ok": ffmpeg_ok}
    _tools_cache_ts = now
    return ytdlp_ok, ytdlp_version, ffmpeg_ok


# ---------------------------------------------------------------------------
# Cache de búsqueda
# ---------------------------------------------------------------------------

def _cache_get(key: str) -> Optional[Any]:
    v = SEARCH_CACHE.get(key)
    if not v:
        return None
    ts, payload = v
    if (_now() - ts) > SEARCH_CACHE_TTL_SEC:
        SEARCH_CACHE.pop(key, None)
        return None
    return payload


def _cache_set(key: str, payload: Any) -> None:
    SEARCH_CACHE[key] = (_now(), payload)
    if len(SEARCH_CACHE) > 250:
        try:
            oldest = sorted(SEARCH_CACHE.items(), key=lambda kv: kv[1][0])[0][0]
            SEARCH_CACHE.pop(oldest, None)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Cache de resultados de descarga (para join-pending)
# ---------------------------------------------------------------------------

def _result_get(key: str) -> Optional[Any]:
    v = RESULTS.get(key)
    if not v:
        return None
    ts, payload = v
    if (_now() - ts) > RESULT_TTL_SEC:
        RESULTS.pop(key, None)
        return None
    return payload


def _result_set(key: str, payload: Any) -> None:
    RESULTS[key] = (_now(), payload)
    if len(RESULTS) > 200:
        try:
            oldest = sorted(RESULTS.items(), key=lambda kv: kv[1][0])[0][0]
            RESULTS.pop(oldest, None)
        except Exception:
            pass


def _pending_done(key: str, payload: Any) -> None:
    _result_set(key, payload)
    with PENDING_LOCK:
        entry = PENDING.get(key)
        if entry and isinstance(entry.get("event"), threading.Event):
            entry["event"].set()
        PENDING.pop(key, None)


# ---------------------------------------------------------------------------
# Índice de descargas (index.json)
# ---------------------------------------------------------------------------

def _load_index() -> Dict[str, Any]:
    try:
        if not INDEX_PATH.exists():
            return {}
        data = json.loads(INDEX_PATH.read_text("utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _save_index(index: Dict[str, Any]) -> None:
    try:
        DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)
        tmp = INDEX_PATH.with_suffix(".tmp")
        tmp.write_text(json.dumps(index, ensure_ascii=False), "utf-8")
        tmp.replace(INDEX_PATH)
    except Exception:
        return


_stats_cache = (0, 0)
_stats_cache_ts = 0

def _downloads_stats() -> Tuple[int, int]:
    global _stats_cache, _stats_cache_ts
    now = _now()
    if now - _stats_cache_ts < 60:
        return _stats_cache

    try:
        if not DOWNLOADS_DIR.exists():
            return 0, 0
        count = 0
        size = 0
        for p in DOWNLOADS_DIR.iterdir():
            if p.is_file() and p.name != "index.json":
                count += 1
                try:
                    size += int(p.stat().st_size)
                except Exception:
                    pass
        _stats_cache = (count, size)
        _stats_cache_ts = now
        return count, size
    except Exception:
        return _stats_cache


# ---------------------------------------------------------------------------
# Limpieza
# ---------------------------------------------------------------------------

def _cleanup_impl() -> Dict[str, Any]:
    DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)
    index = _load_index()
    now = _now()

    files: List[Tuple[Path, float, int]] = []
    total = 0
    for p in DOWNLOADS_DIR.iterdir():
        if not p.is_file() or p.name == "index.json":
            continue
        try:
            st = p.stat()
            total += int(st.st_size)
            files.append((p, st.st_mtime, int(st.st_size)))
        except Exception:
            continue

    deleted: List[str] = []

    # Fase 1: borrar archivos viejos (>30 min) que no estén protegidos (<5 min)
    for p, mtime, size in sorted(files, key=lambda x: x[1]):
        age = now - mtime
        if age < MIN_FILE_AGE_PROTECT_SEC:
            continue
        if age >= MAX_FILE_AGE_SEC:
            try:
                p.unlink()
                total -= size
                deleted.append(p.name)
            except Exception:
                pass

    # Fase 2: si supera 2GB, borrar los más viejos
    if total > MAX_TOTAL_BYTES:
        for p, mtime, size in sorted(files, key=lambda x: x[1]):
            if total <= MAX_TOTAL_BYTES:
                break
            if p.name in deleted or not p.exists():
                continue
            age = now - mtime
            if age < MIN_FILE_AGE_PROTECT_SEC:
                continue
            try:
                p.unlink()
                total -= size
                deleted.append(p.name)
            except Exception:
                pass

    # Actualizar index
    if deleted:
        deleted_set = set(deleted)
        for k, v in list(index.items()):
            if not isinstance(v, dict):
                continue
            # Formato nuevo: {"files": [...]}
            f_list = v.get("files")
            if isinstance(f_list, list):
                if any(isinstance(it, dict) and str(it.get("name") or "") in deleted_set for it in f_list):
                    index.pop(k, None)
                    continue
            # Formato viejo: {"filename": "..."}
            if str(v.get("filename") or "") in deleted_set:
                index.pop(k, None)
        _save_index(index)

    print(
        f"[WORKER] cleanup deleted={len(deleted)} total_mb={total / (1024*1024):.1f}",
        flush=True
    )
    return {"deletedCount": len(deleted), "deleted": deleted, "totalBytes": total}


def _maybe_cleanup_throttled() -> None:
    global LAST_CLEANUP_TS
    now = _now()
    if (now - LAST_CLEANUP_TS) < CLEANUP_INTERVAL_SEC:
        return
    LAST_CLEANUP_TS = now
    try:
        _cleanup_impl()
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Extracción de imágenes TikTok (lógica del worker.py Muxivo, mejorada)
# ---------------------------------------------------------------------------

_IMAGE_CDN_RE = re.compile(
    r'https?://[a-z0-9\-_.]+(?:tiktokcdn(?:-[a-z]{2,4})?\.com)[^\s"\'<>\\}{)]*',
    re.IGNORECASE,
)

_IMG_REJECT_PATTERNS = (
    "avatar", "icon", "emoji", "sticker", "placeholder",
    "default_", "100x100", "150x150", "168x168", "720x720",
    "72x72", "120x120", "200x200", "300x300", "p16-profile", "profile_pic",
    "musically", "/obj/musically", "watermark",
    # NOTA: "/tos-alisg-i-" removido — es un path CDN legítimo para fotos de posts TikTok en servidores asiáticos
    "profile", "logo", "badge", "s150x150"
)


def _clean_image_url(url: str) -> str:
    u = str(url or "").strip().strip('"').strip("'")
    u = (u
         .replace("\\u002F", "/")
         .replace("\\/", "/")
         .replace("\\u0026", "&")
         .replace("&amp;", "&"))
    for ch in ('"', "'", "}", ")", "]", ";", ",", " ", "\n", "\r", "\t"):
        u = u.rstrip(ch)
    return u.strip()


def _is_valid_photo_url(url: str) -> bool:
    if not url:
        return False
    low = url.lower()
    if "tiktokcdn.com" not in low:
        return False
    if any(p in low for p in _IMG_REJECT_PATTERNS):
        return False
    is_image_ext = any(ext in low for ext in (".jpeg", ".jpg", ".png", ".webp", ".avif"))
    is_image_path = any(
        seg in low for seg in (
            "/photo/", "/image/", "image_post", "/img/",
            "photomode", "photo-mode", "tos-maliva", "tos-useast",
            "tos-alisg", "tos-", "/obj/",
        )
    )
    return is_image_ext or is_image_path


def _normalize_for_dedup(url: str) -> str:
    try:
        p = urlparse(url)
        return f"{p.scheme}://{p.netloc}{p.path}"
    except Exception:
        return url


def _dedup_images(urls: List[str]) -> List[str]:
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
    score = 0
    low = url.lower()
    if "origin" in low or "original" in low:
        score += 10
    if "1080" in low or "high" in low:
        score += 5
    if "jpeg" in low or "webp" in low:
        score += 3
    if "image_post" in low or "photomode" in low or "photo-mode" in low:
        score += 8
    if "100w" in low or "200w" in low or "thumb" in low:
        score -= 5
    if "720x720" in low:
        score -= 10
    return score


def _walk_json_for_images(obj: Any, _depth: int = 0) -> List[str]:
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
        priority_keys = (
            "imagePost", "images", "imageList", "photo", "photoImages",
            "slides", "carousel", "coverImage", "originCover", "dynamicCover",
        )
        for pk in priority_keys:
            if pk in obj:
                results.extend(_walk_json_for_images(obj[pk], _depth + 1))
        for key, val in obj.items():
            low_key = str(key).lower()
            if any(skip in low_key for skip in ("avatar", "icon", "logo", "nickname", "uniqueid")):
                continue
            if isinstance(val, str):
                cleaned = _clean_image_url(val)
                if _is_valid_photo_url(cleaned):
                    results.append(cleaned)
            elif isinstance(val, (dict, list)):
                if key not in priority_keys:
                    results.extend(_walk_json_for_images(val, _depth + 1))
    return results


def _extract_json_block(html: str, marker: str) -> Optional[Any]:
    idx = html.find(marker)
    if idx < 0:
        return None
    brace = html.find("{", idx)
    if brace < 0:
        return None
    depth = 0
    end = None
    for i in range(brace, min(len(html), brace + 2_000_000)):
        c = html[i]
        if c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                end = i + 1
                break
    if not end:
        return None
    try:
        return json.loads(html[brace:end])
    except Exception:
        return None


def _extract_from_rehydration(html: str) -> List[str]:
    data = _extract_json_block(html, "__UNIVERSAL_DATA_FOR_REHYDRATION__")
    return _walk_json_for_images(data) if data is not None else []


def _extract_from_sigi_state(html: str) -> List[str]:
    data = _extract_json_block(html, "SIGI_STATE")
    return _walk_json_for_images(data) if data is not None else []


def _extract_from_next_data(html: str) -> List[str]:
    data = _extract_json_block(html, "__NEXT_DATA__")
    return _walk_json_for_images(data) if data is not None else []


def _extract_from_regex(html: str) -> List[str]:
    results: List[str] = []
    for m in _IMAGE_CDN_RE.finditer(html):
        raw = _clean_image_url(m.group(0))
        if _is_valid_photo_url(raw):
            results.append(raw)
    return results


def _extract_og_image(html: str) -> List[str]:
    results: List[str] = []
    patterns = [
        re.compile(r'<meta\s+(?:[^>]*?)property=["\']og:image["\']\s+content=["\']([^"\']+)["\']', re.I),
        re.compile(r'<meta\s+(?:[^>]*?)content=["\']([^"\']+)["\']\s+property=["\']og:image["\']', re.I),
    ]
    for pat in patterns:
        for m in pat.finditer(html):
            url = _clean_image_url(m.group(1))
            if url and url.startswith("http"):
                results.append(url)
    return results


def _fetch_html(url: str, *, mobile: bool = True, referer: Optional[str] = None) -> Tuple[str, str]:
    """Descarga HTML de una URL (usando cookies si están disponibles). Devuelve (html, final_url)."""
    hdrs = dict(_HEADERS_MOBILE if mobile else _HEADERS_DESKTOP)
    if referer:
        hdrs["Referer"] = referer
    try:
        jar = _load_cookie_jar()
        resp = requests.get(url, headers=hdrs, cookies=jar, timeout=18, allow_redirects=True)
        resp.raise_for_status()
        return resp.text, resp.url
    except Exception as exc:
        print(f"[WORKER/extract] fetch_html failed: {exc}", flush=True)
        return "", url


def _resolve_tiktok_shortlink(url: str) -> str:
    """Resuelve shortlinks vt.tiktok.com / vm.tiktok.com."""
    try:
        parsed = urlparse(url)
        if parsed.netloc.lower() not in _TIKTOK_SHORTLINK_HOSTS:
            return url
        resp = requests.get(url, headers=_HEADERS_MOBILE, allow_redirects=True, timeout=18)
        final = resp.url or url
        print(f"[WORKER/extract] resolved {url} -> {final}", flush=True)
        return final
    except Exception as exc:
        print(f"[WORKER/extract] shortlink resolve failed: {exc}", flush=True)
        return url


def _extract_gallery_via_ytdlp(url: str) -> List[str]:
    """Fallback: extrae imágenes de TikTok Photo usando yt-dlp --dump-json."""
    try:
        cmd = [
            sys.executable, "-m", "yt_dlp",
            "--no-playlist", "--no-warnings", "--quiet",
            "--dump-json", url,
        ] + _cookies_args()
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=25)
        if proc.returncode != 0:
            return []
        urls: List[str] = []
        for line in (proc.stdout or "").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
            except Exception:
                continue
            # yt-dlp devuelve thumbnails o formats con type=image para TikTok Photo
            thumbnails = data.get("thumbnails") or []
            for t in thumbnails:
                u = str(t.get("url") or "").strip()
                if u and u.startswith("http") and not any(p in u.lower() for p in _IMG_REJECT_PATTERNS):
                    urls.append(u)
            # También revisar formats con ext=jpg/webp (slides)
            formats = data.get("formats") or []
            for f in formats:
                if str(f.get("ext") or "").lower() in ("jpg", "jpeg", "png", "webp"):
                    fu = str(f.get("url") or "").strip()
                    if fu and fu.startswith("http"):
                        urls.append(fu)
        print(f"[WORKER/extract] ytdlp_fallback found={len(urls)}", flush=True)
        return _dedup_images(urls)
    except Exception as exc:
        print(f"[WORKER/extract] ytdlp_fallback failed: {exc}", flush=True)
        return []


def extract_gallery(input_url: str, max_items: int = 48) -> List[Dict[str, str]]:
    """Pipeline completo: resolve -> fetch -> extract -> dedup -> rank -> items."""
    t0 = _now()
    print(f"[WORKER/extract] input_url={input_url}", flush=True)

    # 1. Resolver shortlinks TikTok
    url = _resolve_tiktok_shortlink(input_url)
    print(f"[WORKER/extract] final_url={url}", flush=True)

    # 2. Fetch HTML (mobile primero, luego desktop)
    html, final_url = _fetch_html(url, mobile=True)
    if not html or len(html) < 500:
        html, final_url = _fetch_html(url, mobile=False)
    if not html:
        print("[WORKER/extract] empty HTML", flush=True)
        # Fix #1: fallback directo a yt-dlp si no hay HTML
        items_ytdlp = _extract_gallery_via_ytdlp(url)
        return [
            {"url": u, "thumb": u, "label": f"IMAGE {i+1}", "kind": "image"}
            for i, u in enumerate(items_ytdlp[:max_items])
        ]

    print(f"[WORKER/extract] html_size={len(html)}", flush=True)

    # 3. Estrategias de extracción
    all_urls: List[str] = []

    rehydration = _extract_from_rehydration(html)
    print(f"[WORKER/extract] rehydration_candidates={len(rehydration)}", flush=True)
    all_urls.extend(rehydration)

    sigi = _extract_from_sigi_state(html)
    print(f"[WORKER/extract] sigi_candidates={len(sigi)}", flush=True)
    all_urls.extend(sigi)

    next_data = _extract_from_next_data(html)
    print(f"[WORKER/extract] next_data_candidates={len(next_data)}", flush=True)
    all_urls.extend(next_data)

    regex = _extract_from_regex(html)
    print(f"[WORKER/extract] regex_candidates={len(regex)}", flush=True)
    all_urls.extend(regex)

    # og:image solo como último recurso
    if not all_urls:
        og = _extract_og_image(html)
        print(f"[WORKER/extract] og_image_candidates={len(og)}", flush=True)
        all_urls.extend(og)

    # Retry con desktop si mobile no encontró nada
    if not all_urls:
        print("[WORKER/extract] retrying with desktop UA", flush=True)
        html2, _ = _fetch_html(url, mobile=False)
        if html2 and len(html2) > 500:
            all_urls.extend(_extract_from_rehydration(html2))
            all_urls.extend(_extract_from_sigi_state(html2))
            all_urls.extend(_extract_from_next_data(html2))
            all_urls.extend(_extract_from_regex(html2))
            if not all_urls:
                all_urls.extend(_extract_og_image(html2))

    # Fix #1: fallback a yt-dlp si el scraping HTML no encontró nada
    if not all_urls:
        print("[WORKER/extract] HTML scraping yielded 0 — trying yt-dlp fallback", flush=True)
        all_urls = _extract_gallery_via_ytdlp(url)

    # 4. Dedup + ranking
    unique = _dedup_images(all_urls)
    print(f"[WORKER/extract] candidates={len(unique)}", flush=True)
    unique.sort(key=_score_image_url, reverse=True)

    # 5. Construir items
    items: List[Dict[str, str]] = [
        {"url": img_url, "thumb": img_url, "label": f"IMAGE {idx}", "kind": "image"}
        for idx, img_url in enumerate(unique[:max_items], start=1)
    ]

    elapsed = _now() - t0
    print(f"[WORKER/extract] returning={len(items)} elapsed={elapsed:.1f}s", flush=True)
    return items


def _extract_instagram_gallery(url: str) -> List[str]:
    """
    Extracción completa de fotos y carruseles de Instagram en el Worker (Termux):
      1. Endpoint público/embed (/p/<shortcode>/embed/captioned/) con cookies e IP móvil.
      2. Scraping de la página del post con cookies (soporta posts privados/restringidos de cuentas seguidas).
      3. Fallback yt-dlp --dump-json.
    """
    m = re.search(r"/(?:p|reel|tv)/([A-Za-z0-9_-]+)", url)
    shortcode = m.group(1) if m else None
    jar = _load_cookie_jar()
    results: List[str] = []

    def _is_clean_ig_img(u: str) -> bool:
        if not u or not u.startswith("http"):
            return False
        low = u.lower()
        if any(p in low for p in _IMG_REJECT_PATTERNS):
            return False
        if any(bad in low for bad in ("/s150x150/", "/s320x320/", "profile_pic", "/rsrc.php", "static.cdninstagram.com")):
            return False
        return ("cdninstagram.com" in low) or ("fbcdn.net" in low)

    # 0. Estrategia API interna de Instagram (/api/v1/media/<media_id>/info/) con cookies
    # Funciona para posts privados de cuentas seguidas, +18 y carruseles completos en máxima calidad.
    if shortcode and jar is not None:
        try:
            alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
            media_id = 0
            for ch in shortcode:
                if ch in alphabet:
                    media_id = media_id * 64 + alphabet.index(ch)
            if media_id > 0:
                api_url = f"https://www.instagram.com/api/v1/media/{media_id}/info/"
                api_hdrs = {
                    "User-Agent": _UA_DESKTOP,
                    "X-IG-App-ID": "936619743392459",
                    "X-Requested-With": "XMLHttpRequest",
                    "Referer": f"https://www.instagram.com/p/{shortcode}/",
                }
                r_api = requests.get(api_url, headers=api_hdrs, cookies=jar, timeout=12)
                if r_api.status_code == 200:
                    api_items = (r_api.json() or {}).get("items") or []
                    if api_items and isinstance(api_items[0], dict):
                        it0 = api_items[0]
                        carousel = it0.get("carousel_media") or []
                        if carousel:
                            for cm in carousel:
                                cands = ((cm or {}).get("image_versions2") or {}).get("candidates") or []
                                if cands and isinstance(cands[0], dict):
                                    u = _clean_image_url(str(cands[0].get("url") or ""))
                                    if _is_clean_ig_img(u):
                                        results.append(u)
                        else:
                            cands = (it0.get("image_versions2") or {}).get("candidates") or []
                            if cands and isinstance(cands[0], dict):
                                u = _clean_image_url(str(cands[0].get("url") or ""))
                                if _is_clean_ig_img(u):
                                    results.append(u)
        except Exception as exc:
            print(f"[WORKER/extract] ig api v1 failed: {exc}", flush=True)

    if results:
        deduped = _dedup_images(results)
        print(f"[WORKER/extract] instagram api_v1 found={len(deduped)}", flush=True)
        return deduped

    # 1. Estrategia Embed (/embed/captioned/)
    if shortcode:
        embed_url = f"https://www.instagram.com/p/{shortcode}/embed/captioned/"
        try:
            hdrs = {
                "User-Agent": _UA_DESKTOP,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": "https://www.instagram.com/",
            }
            resp = requests.get(embed_url, headers=hdrs, cookies=jar, timeout=15)
            html = resp.text or ""
            if html:
                # 1a. Buscar contextJSON con carrusel completo (edge_sidecar_to_children)
                ctx_m = re.search(r'"contextJSON"\s*:\s*"((?:\\.|[^"\\])*)"', html)
                if ctx_m:
                    try:
                        ctx_raw = json.loads(f'"{ctx_m.group(1)}"')
                        ctx_data = json.loads(ctx_raw)
                        media = (ctx_data.get("gql_data") or {}).get("shortcode_media") or ctx_data.get("shortcode_media") or {}
                        edges = ((media.get("edge_sidecar_to_children") or {}).get("edges")) or []
                        for edge in edges:
                            node = (edge or {}).get("node") or {}
                            disp = _clean_image_url(str(node.get("display_url") or ""))
                            if not disp:
                                res_list = node.get("display_resources") or []
                                if res_list and isinstance(res_list[-1], dict):
                                    disp = _clean_image_url(str(res_list[-1].get("src") or ""))
                            if _is_clean_ig_img(disp):
                                results.append(disp)
                        if not results:
                            disp = _clean_image_url(str(media.get("display_url") or ""))
                            if _is_clean_ig_img(disp):
                                results.append(disp)
                    except Exception as exc:
                        print(f"[WORKER/extract] ig embed contextJSON parse error: {exc}", flush=True)

                # 1b. Buscar EmbeddedMediaImage en el HTML del embed
                if not results:
                    for em in re.finditer(r'<img[^>]+class="[^"]*EmbeddedMediaImage[^"]*"[^>]+src="([^"]+)"', html):
                        u = _clean_image_url(em.group(1))
                        if _is_clean_ig_img(u):
                            results.append(u)
                    for em in re.finditer(r'<img[^>]+src="([^"]+)"[^>]+class="[^"]*EmbeddedMediaImage[^"]*"', html):
                        u = _clean_image_url(em.group(1))
                        if _is_clean_ig_img(u):
                            results.append(u)
        except Exception as exc:
            print(f"[WORKER/extract] ig embed request failed: {exc}", flush=True)

    if results:
        deduped = _dedup_images(results)
        print(f"[WORKER/extract] instagram embed found={len(deduped)}", flush=True)
        return deduped

    # 2. Estrategia Direct Post Page con cookies (para posts con login/edad/privados)
    if shortcode:
        post_url = f"https://www.instagram.com/p/{shortcode}/"
        try:
            hdrs = {
                "User-Agent": _UA_DESKTOP,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": "https://www.instagram.com/",
                "Sec-Fetch-Mode": "navigate",
            }
            resp = requests.get(post_url, headers=hdrs, cookies=jar, timeout=15)
            html = resp.text or ""
            if html:
                # Extraer display_url del JSON embebido de Polaris / Relay
                for dm in re.finditer(r'"display_url"\s*:\s*"([^"]+)"', html):
                    u = _clean_image_url(dm.group(1))
                    if _is_clean_ig_img(u):
                        results.append(u)
                # Si no hay display_url, buscar image_versions2 candidates (primer candidato de cada bloque es el de mayor resolución)
                if not results:
                    for block in re.finditer(r'"image_versions2"\s*:\s*\{\s*"candidates"\s*:\s*\[(\{[^\]]+\})\]', html):
                        first_u = re.search(r'"url"\s*:\s*"([^"]+)"', block.group(1))
                        if first_u:
                            u = _clean_image_url(first_u.group(1))
                            if _is_clean_ig_img(u):
                                results.append(u)
                # Fallback a og:image si estamos logueados y es 1 sola imagen
                if not results and '"LoginAndSignupPage"' not in html:
                    for og_u in _extract_og_image(html):
                        if _is_clean_ig_img(og_u):
                            results.append(og_u)
        except Exception as exc:
            print(f"[WORKER/extract] ig direct page failed: {exc}", flush=True)

    if results:
        deduped = _dedup_images(results)
        print(f"[WORKER/extract] instagram page found={len(deduped)}", flush=True)
        return deduped

    # 3. Fallback: yt-dlp --dump-json (con cookies si están disponibles)
    try:
        cmd = [
            sys.executable, "-m", "yt_dlp",
            "--no-playlist", "--no-warnings", "--quiet",
            "--dump-json", url,
        ] + _cookies_args()
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=20)
        if proc.returncode == 0:
            for line in (proc.stdout or "").splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    data = json.loads(line)
                except Exception:
                    continue
                thumbnails = data.get("thumbnails") or []
                if thumbnails:
                    urls = [t.get("url") for t in thumbnails if t.get("url")]
                    filtered = [u for u in urls if _is_clean_ig_img(u)]
                    if filtered:
                        results.extend(filtered)
                thumb = data.get("thumbnail")
                if thumb and _is_clean_ig_img(thumb):
                    results.append(thumb)
    except Exception as exc:
        print(f"[WORKER/extract] instagram ytdlp failed: {exc}", flush=True)

    return _dedup_images(results)


def _normalize_pinterest_url(url: str) -> str:
    """Resuelve pin.it y normaliza dominios regionales de Pinterest a www.pinterest.com/pin/<id>/."""
    try:
        p = urlparse(url)
        host = (p.hostname or "").lower()
        if host == "pin.it" or host.endswith(".pin.it"):
            r = requests.get(url, headers={"User-Agent": _UA_DESKTOP}, allow_redirects=True, timeout=12)
            url = r.url or url
            p = urlparse(url)
            host = (p.hostname or "").lower()
        if "pinterest." in host:
            m = re.search(r"/pin/(?:[^/]+--)?(\d+)", p.path or "")
            if m:
                return f"https://www.pinterest.com/pin/{m.group(1)}/"
    except Exception as exc:
        print(f"[WORKER/extract] pinterest normalize failed: {exc}", flush=True)
    return url


def _extract_pinterest_gallery(url: str) -> List[str]:
    """Extrae imágenes en máxima calidad (originals) de un Pin de Pinterest."""
    norm_url = _normalize_pinterest_url(url)
    html, _ = _fetch_html(norm_url, mobile=False, referer="https://www.pinterest.com/")
    if not html:
        return []
    results: List[str] = []
    # Buscar URLs originals en i.pinimg.com
    for m in re.finditer(r'https://i\.pinimg\.com/originals/[A-Za-z0-9/_.-]+\.(?:jpg|jpeg|png|webp)', html):
        results.append(_clean_image_url(m.group(0)))
    if not results:
        for m in re.finditer(r'https://i\.pinimg\.com/736x/[A-Za-z0-9/_.-]+\.(?:jpg|jpeg|png|webp)', html):
            results.append(_clean_image_url(m.group(0)))
    if not results:
        results.extend(_extract_og_image(html))
    return _dedup_images(results)


def _extract_twitter_gallery(url: str) -> List[str]:
    """Extrae fotos de un post de X/Twitter usando api.fxtwitter.com."""
    m = re.search(r"(?:twitter\.com|x\.com)/([^/]+)/status/(\d+)", url)
    if not m:
        return []
    user, status_id = m.group(1), m.group(2)
    api_url = f"https://api.fxtwitter.com/{user}/status/{status_id}"
    try:
        r = requests.get(api_url, headers={"User-Agent": _UA_DESKTOP}, timeout=12)
        if r.status_code == 200:
            data = r.json()
            tweet = data.get("tweet") or {}
            media = tweet.get("media") or {}
            photos = media.get("photos") or []
            urls = [str(p.get("url") or "").strip() for p in photos if isinstance(p, dict) and p.get("url")]
            if urls:
                return _dedup_images(urls)
    except Exception as exc:
        print(f"[WORKER/extract] twitter gallery failed: {exc}", flush=True)
    return []


def _extract_image_urls(raw_url: str) -> List[str]:
    """Extrae URLs de imágenes según plataforma."""
    platform = detect_platform(raw_url)
    if platform == "tiktok":
        items = extract_gallery(raw_url)
        return [it["url"] for it in items]
    if platform == "instagram":
        urls = _extract_instagram_gallery(raw_url)
        if urls:
            return urls
    if platform == "pinterest":
        urls = _extract_pinterest_gallery(raw_url)
        if urls:
            return urls
    if platform == "twitter":
        urls = _extract_twitter_gallery(raw_url)
        if urls:
            return urls
    # Fallback: og:image
    html, _ = _fetch_html(raw_url, mobile=True)
    if html:
        return _dedup_images(_extract_og_image(html))
    return []


# ---------------------------------------------------------------------------
# Parseo universal del request de descarga
# (Parte B del spec — corrige bug audio/video)
# ---------------------------------------------------------------------------

def _parse_download_request(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Acepta: kind, type, mode, downloadType, mediaType
    Prioridad de detección:
      1. Si format es audio ext  -> kind=audio
      2. Si format es image ext  -> kind=image
      3. Si format es video ext  -> kind=video (salvo que kind_raw=audio)
      4. Si kind_raw explícito   -> usar ese
      5. Si URL es imagen directa -> kind=image
      6. Default                 -> video
    """
    raw_url = str(data.get("url") or "").strip()
    kind_raw = str(
        data.get("kind")
        or data.get("type")
        or data.get("mode")
        or data.get("downloadType")
        or data.get("mediaType")
        or ""
    ).strip().lower()
    fmt = str(data.get("format") or "").strip().lower()
    quality = str(data.get("quality") or "").strip().lower()

    # Mapeo de quality textual a número
    quality_map = {"low": "360", "medium": "720", "high": "1080", "best": "0"}
    if quality in quality_map:
        quality = quality_map[quality]

    # Detectar kind desde formato primero (más confiable)
    kind: str = ""
    if fmt in _AUDIO_EXTS:
        kind = "audio"
    elif fmt in _IMAGE_EXTS:
        kind = "image"
    elif fmt in _VIDEO_EXTS:
        # El formato es video, pero si kind_raw dice audio → audio
        if kind_raw == "audio":
            kind = "audio"
        else:
            kind = "video"

    # Si no se detectó por formato, usar kind_raw
    if not kind:
        if kind_raw in ("audio", "video", "image"):
            kind = kind_raw

    # Último fallback
    if not kind:
        if is_direct_image_url(raw_url):
            kind = "image"
        else:
            kind = "video"  # default para el sistema descargador

    # Formato default según kind
    if not fmt:
        if kind == "audio":
            fmt = "mp3"
        elif kind == "image":
            fmt = "jpg"
        else:
            fmt = "mp4"

    # Quality default
    if not quality:
        quality = "720"

    return {"url": raw_url, "kind": kind, "format": fmt, "quality": quality}


# ---------------------------------------------------------------------------
# Cache key — distingue audio vs video vs image
# ---------------------------------------------------------------------------

def _cache_key_for(url: str, kind: str, fmt: str, quality: str) -> str:
    """
    Clave única por: plataforma + ID + kind + format + quality
    Ejemplos:
      youtube:abc123:audio:mp3:720
      youtube:abc123:video:mp4:720
      tiktok:<sha1>:image:jpg:720
    """
    platform = detect_platform(url)
    if platform == "youtube":
        yt_id = extract_youtube_id(url) or ""
        if yt_id:
            return f"youtube:{yt_id}:{kind}:{fmt}:{quality}"
    return f"{platform}:{_sha1(url)}:{kind}:{fmt}:{quality}"


# ---------------------------------------------------------------------------
# Index helpers para cache de archivos
# ---------------------------------------------------------------------------

def _file_url(name: str) -> str:
    base = (request.host_url or "").rstrip("/")
    return f"{base}/files/{quote(name, safe='')}"


def _cached_files_from_index(key: str) -> Optional[List[Dict[str, Any]]]:
    """Devuelve lista de files si están en índice Y en disco. None si no existe o falta archivo."""
    index = _load_index()
    entry = index.get(key)
    if not isinstance(entry, dict):
        return None
    files = entry.get("files")
    if not isinstance(files, list) or not files:
        return None
    out = []
    for f in files:
        if not isinstance(f, dict):
            continue
        name = str(f.get("name") or "").strip()
        if not name:
            continue
        fp = (DOWNLOADS_DIR / name).resolve()
        if not fp.exists() or not fp.is_file() or fp.stat().st_size <= 0:
            # Archivo borrado — invalidar toda la entrada
            return None
        out.append({
            "name": name,
            "url": _file_url(name),
            "kind": str(f.get("kind") or ""),
            "size": int(fp.stat().st_size),
            "ext": Path(name).suffix.lstrip("."),
        })
    return out if out else None


def _save_cache_entry(key: str, files: List[Dict[str, Any]], meta: Dict[str, Any]) -> None:
    index = _load_index()
    index[key] = {
        "files": [
            {"name": f["name"], "kind": f.get("kind"), "size": f.get("size"), "ext": f.get("ext")}
            for f in files
        ],
        "createdAt": int(_now()),
        **meta,
    }
    _save_index(index)


# ---------------------------------------------------------------------------
# Descarga de imagen binaria con requests
# ---------------------------------------------------------------------------

# Tamaño mínimo aceptable para un archivo de imagen (500 bytes evita respuestas de error HTML)
_MIN_IMAGE_BYTES = 500

# Headers para descargar imágenes de CDNs que requieren Referer (TikTok CDN, Instagram CDN)
_CDN_DOWNLOAD_HEADERS: Dict[str, str] = {
    "User-Agent": _UA_MOBILE,
    "Referer": "https://www.tiktok.com/",  # Fix #2: necesario para tiktokcdn.com
    "Accept": "image/webp,image/avif,image/*,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}


def _cdn_headers_for(url: str) -> Dict[str, str]:
    """Devuelve headers apropiados según el dominio del CDN."""
    low = url.lower()
    if "tiktokcdn" in low or "tiktok.com" in low:
        return dict(_CDN_DOWNLOAD_HEADERS)
    if "instagram.com" in low or "cdninstagram.com" in low or "fbcdn.net" in low:
        return {
            "User-Agent": _UA_MOBILE,
            "Referer": "https://www.instagram.com/",
            "Accept": "image/webp,image/avif,image/*,*/*;q=0.8",
        }
    if "pinimg.com" in low or "pinterest." in low:
        return {
            "User-Agent": _UA_DESKTOP,
            "Referer": "https://www.pinterest.com/",
            "Accept": "image/webp,image/avif,image/*,*/*;q=0.8",
        }
    if "twimg.com" in low or "twitter.com" in low or "x.com" in low:
        return {
            "User-Agent": _UA_DESKTOP,
            "Referer": "https://x.com/",
            "Accept": "image/webp,image/avif,image/*,*/*;q=0.8",
        }
    return {"User-Agent": _UA_MOBILE}


def _download_binary(url: str, dest: Path) -> Optional[Path]:
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + f".tmp-{int(_now())}")
    try:
        hdrs = _cdn_headers_for(url)  # Fix #2: headers con Referer según CDN
        r = requests.get(url, stream=True, timeout=30, allow_redirects=True, headers=hdrs)
        if r.status_code < 200 or r.status_code >= 300:
            print(f"[WORKER/binary] HTTP {r.status_code} for {url[:80]}", flush=True)
            return None
        with open(tmp, "wb") as fh:
            for chunk in r.iter_content(chunk_size=256 * 1024):
                if chunk:
                    fh.write(chunk)
        # Fix #5: umbral mínimo 500 bytes (antes era 0, aceptaba respuestas de error HTML)
        if not tmp.exists() or tmp.stat().st_size < _MIN_IMAGE_BYTES:
            print(f"[WORKER/binary] file too small ({tmp.stat().st_size if tmp.exists() else 0}B) for {url[:80]}", flush=True)
            try:
                tmp.unlink()
            except Exception:
                pass
            return None
        tmp.replace(dest)
        return dest
    except Exception as exc:
        print(f"[WORKER/binary] download failed: {exc}", flush=True)
        try:
            if tmp.exists():
                tmp.unlink()
        except Exception:
            pass
        return None


# ---------------------------------------------------------------------------
# Detección de archivos nuevos post-yt-dlp (lógica robusta del worker.py)
# ---------------------------------------------------------------------------

_TEMP_SUFFIXES = {".part", ".ytdl", ".temp", ".tmp", ".mhtml"}
_MEDIA_SUFFIXES = (
    set(f".{e}" for e in _AUDIO_EXTS) |
    set(f".{e}" for e in _VIDEO_EXTS) |
    set(f".{e}" for e in _IMAGE_EXTS)
)


def _is_temp_file(p: Path) -> bool:
    if p.suffix.lower() in _TEMP_SUFFIXES:
        return True
    if re.match(r".*\.f\d+\.\w+$", p.name.lower()):
        return True
    return False


def _is_valid_media_file(p: Path) -> bool:
    if not p.is_file():
        return False
    if _is_temp_file(p):
        return False
    try:
        if p.stat().st_size < 100:
            return False
    except Exception:
        return False
    return p.suffix.lower() in _MEDIA_SUFFIXES


def _find_new_files(prefix: str, before_names: set) -> List[Path]:
    """Encuentra archivos nuevos creados por yt-dlp usando 3 estrategias."""
    # Estrategia 1: antes/después
    try:
        after_names = {f.name for f in DOWNLOADS_DIR.iterdir() if f.is_file()}
    except Exception:
        after_names = set()
    new_names = after_names - before_names
    new_files = [DOWNLOADS_DIR / n for n in new_names if _is_valid_media_file(DOWNLOADS_DIR / n)]

    # Estrategia 2: buscar por prefijo
    if not new_files:
        new_files = [f for f in DOWNLOADS_DIR.glob(f"{prefix}*") if _is_valid_media_file(f)]

    # Estrategia 3: archivos recientes (últimos 30s)
    if not new_files:
        cutoff = _now() - 30
        new_files = [
            f for f in DOWNLOADS_DIR.iterdir()
            if _is_valid_media_file(f) and f.stat().st_mtime >= cutoff
        ]

    return sorted(new_files, key=lambda f: f.stat().st_mtime, reverse=True)


# ---------------------------------------------------------------------------
# Flask: GET /
# ---------------------------------------------------------------------------

@app.get("/")
def index():
    return jsonify({
        "ok": True,
        "service": "universal-media-worker",
        "version": VERSION,
        "endpoints": {
            "GET /": "Info del servicio",
            "GET /health": "Estado con capabilities",
            "POST /search": "Búsqueda en YouTube",
            "POST /download": "Descarga audio/video/imagen",
            "POST /extract": "Extrae imágenes de TikTok/Instagram",
            "POST /cleanup": "Limpieza manual",
            "GET /files/<filename>": "Sirve archivos descargados",
        },
    })


# ---------------------------------------------------------------------------
# Flask: GET /health
# ---------------------------------------------------------------------------

@app.get("/health")
def health_check():
    """Responde siempre 200. Nunca falla con 500."""
    try:
        ytdlp_ok, ytdlp_version, ffmpeg_ok = _detect_tools()
    except Exception:
        ytdlp_ok, ytdlp_version, ffmpeg_ok = False, None, False
    try:
        count, size = _downloads_stats()
        size_mb = round(size / (1024 * 1024), 2)
    except Exception:
        count, size_mb = 0, 0
    cookies_path = _resolve_cookies_path()
    return jsonify({
        "ok": True,
        "service": "universal-media-worker",
        "version": VERSION,
        "yt_dlp": ytdlp_ok,
        "yt_dlp_version": ytdlp_version,
        "ffmpeg": ffmpeg_ok,
        "cookies_loaded": cookies_path is not None,
        "cookies_file": str(cookies_path) if cookies_path else None,
        "downloads_count": count,
        "downloads_size_mb": size_mb,
        "capabilities": {
            "search": True,
            "download": True,
            "extract": True,
            "files": True,
            "cleanup": True,
            "youtube": True,
            "tiktok": True,
            "instagram": True,
            "pinterest": True,
            "twitter": True,
            "facebook": True,
        },
    })


# ---------------------------------------------------------------------------
# Flask: POST /search
# ---------------------------------------------------------------------------

@app.post("/search")
def search_music():
    """
    Búsqueda en YouTube usando yt-dlp metadata (sin descargar).
    Body: { "q": "...", "limit": 10 }
    """
    data = _safe_json()
    q_raw = str(data.get("q") or data.get("query") or "").strip()
    if not q_raw:
        return jsonify({"ok": True, "items": []})

    try:
        limit = int(data.get("limit") or 10)
    except Exception:
        limit = 10
    limit = max(1, min(10, limit))

    q = _normalize_query(q_raw)
    key = f"search:{q}:{limit}"
    cached = _cache_get(key)
    if cached is not None:
        print(f"[WORKER/search] cache-hit q={q}", flush=True)
        return jsonify({"ok": True, "items": cached})

    print(f"[WORKER/search] start q={q}", flush=True)
    cmd = [
        sys.executable, "-m", "yt_dlp",
        "--no-playlist", "--no-warnings",
        "--skip-download", "--dump-json", "--quiet",
        f"ytsearch{limit}:{q}",
    ] + _cookies_args()
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=45)
        if proc.returncode != 0:
            print(f"[WORKER/search] failed reason=yt-dlp rc={proc.returncode}", flush=True)
            return jsonify({"ok": False, "items": [], "message": "yt-dlp failed"})

        out = []
        seen: set = set()
        for line in (proc.stdout or "").splitlines():
            line = (line or "").strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except Exception:
                continue
            vid = str(row.get("id") or "").strip()
            if not vid or vid in seen:
                continue
            seen.add(vid)
            title = str(row.get("title") or "").strip()
            uploader = str(row.get("uploader") or row.get("channel") or "").strip()
            dur = row.get("duration")
            try:
                dur = int(dur) if dur is not None else None
            except Exception:
                dur = None
            cover = row.get("thumbnail") or f"https://i.ytimg.com/vi/{vid}/hqdefault.jpg"
            out.append({
                "id": vid,
                "source": "youtube",
                "sourceId": vid,
                "title": title,
                "artist": uploader or "Internet",
                "duration": dur,
                "coverUrl": cover,
                "url": f"https://www.youtube.com/watch?v={vid}",
            })

        _cache_set(key, out)
        print(f"[WORKER/search] done items={len(out)}", flush=True)
        return jsonify({"ok": True, "items": out})

    except subprocess.TimeoutExpired:
        print("[WORKER/search] failed reason=timeout", flush=True)
        return jsonify({"ok": False, "items": [], "message": "timeout"})
    except Exception as exc:
        print(f"[WORKER/search] failed reason={type(exc).__name__}", flush=True)
        return jsonify({"ok": False, "items": [], "message": "error"})


# ---------------------------------------------------------------------------
# Flask: POST /extract
# ---------------------------------------------------------------------------

@app.post("/extract")
def extract_media():
    """
    Extrae imágenes de TikTok gallery / Instagram.
    Body: { "url": "..." }
    """
    data = _safe_json()
    raw_url = str(data.get("url") or "").strip()
    if not raw_url:
        return jsonify({"ok": False, "items": [], "message": "Missing 'url'", "url": ""}), 400
    if not is_allowed_media_url(raw_url):
        return jsonify({"ok": False, "items": [], "message": "URL not allowed", "url": raw_url}), 400

    platform = detect_platform(raw_url)
    try:
        if platform == "youtube":
            cmd = [
                sys.executable, "-m", "yt_dlp",
                "-f", "bestaudio/best",
                "-g", "--no-warnings", "--quiet",
                "--socket-timeout", "30"
            ] + _cookies_args()

            def _run_extract(extra_args: List[str] = []) -> "subprocess.CompletedProcess[str]":
                return subprocess.run(
                    cmd + extra_args + [raw_url],
                    capture_output=True, text=True,
                    timeout=45, check=False
                )
                
            _BOT_KEYWORDS = ("sign in", "bot", "cookies", "confirm", "login")
            proc = _run_extract()
            
            if proc.returncode != 0:
                stderr_lower = (proc.stderr or "").lower()
                if any(kw in stderr_lower for kw in _BOT_KEYWORDS):
                    print(f"[WORKER/extract] bot-detected, retrying with tv_embedded client", flush=True)
                    proc = _run_extract(["--extractor-args", "youtube:player_client=tv_embedded,web"])
            
            if proc.returncode == 0 and proc.stdout.strip():
                audio_url = proc.stdout.strip().split("\n")[0]
                return jsonify({
                    "ok": True,
                    "audioUrl": audio_url,
                    "message": "Stream URL extracted",
                    "url": raw_url,
                    "items": [{"url": audio_url, "kind": "audio"}]
                })
            else:
                return jsonify({
                    "ok": False,
                    "items": [],
                    "message": f"Extraction failed: rc={proc.returncode}",
                    "url": raw_url,
                })
        elif platform == "tiktok":
            items = extract_gallery(raw_url)
        elif platform == "instagram":
            img_urls = _extract_instagram_gallery(raw_url)
            items = [
                {"url": u, "thumb": u, "label": f"IMAGE {i+1}", "kind": "image"}
                for i, u in enumerate(img_urls)
            ]
        elif platform == "pinterest":
            img_urls = _extract_pinterest_gallery(raw_url)
            items = [
                {"url": u, "thumb": u, "label": f"IMAGE {i+1}", "kind": "image"}
                for i, u in enumerate(img_urls)
            ]
        elif platform == "twitter":
            img_urls = _extract_twitter_gallery(raw_url)
            items = [
                {"url": u, "thumb": u, "label": f"IMAGE {i+1}", "kind": "image"}
                for i, u in enumerate(img_urls)
            ]
        else:
            # Fallback genérico: og:image
            html, _ = _fetch_html(raw_url, mobile=True)
            og_urls = _extract_og_image(html) if html else []
            items = [
                {"url": u, "thumb": u, "label": f"IMAGE {i+1}", "kind": "image"}
                for i, u in enumerate(_dedup_images(og_urls))
            ]
    except Exception as exc:
        print(f"[WORKER/extract] ERROR: {exc}", flush=True)
        traceback.print_exc()
        return jsonify({
            "ok": True,
            "items": [],
            "message": f"Extraction failed: {exc}",
            "url": raw_url,
        })

    return jsonify({
        "ok": True,
        "items": items,
        "message": f"{len(items)} images extracted",
        "url": raw_url,
    })


def _handle_spotify_download(url: str, kind: str, fmt: str, quality: str) -> Tuple[Any, int]:
    """Descarga de Spotify usando yt-dlp (búsqueda en YouTube) como fallback robusto."""
    key = _cache_key_for(url, kind, fmt, quality)
    cached = _cached_files_from_index(key)
    if cached:
        return jsonify({"ok": True, "files": cached, "cached": True, "source": "worker-cache"}), 200

    join_event = None
    with PENDING_LOCK:
        if key in PENDING:
            join_event = PENDING[key]["event"]
        else:
            ev = threading.Event()
            PENDING[key] = {"event": ev}

    if join_event is not None:
        join_event.wait(timeout=240)
        result = _result_get(key)
        if result is not None:
            return jsonify(result), 200
        return jsonify({"ok": False, "files": [], "message": "Worker busy"}), 503

    try:
        _, _, ffmpeg_ok = _detect_tools()
        prefix = sanitize_filename(f"spotify_{_sha1(url)[:12]}_{fmt}_{quality}", f"spotify_{_sha1(url)[:12]}")
        out_tmpl = str((DOWNLOADS_DIR / f"{prefix}.%(ext)s").resolve())
        
        # Le decimos a yt-dlp que busque la canción de Spotify en YouTube automáticamente.
        # yt-dlp no soporta Spotify nativamente (solo extrae metadatos).
        # Al pasarle el link de Spotify con `--default-search ytsearch`, yt-dlp lee los metadatos
        # de Spotify, hace la búsqueda en YouTube, y descarga el primer resultado.
        cmd = [
            sys.executable, "-m", "yt_dlp",
            "--no-playlist", "--no-warnings", "--quiet",
            "--socket-timeout", "30",
            "--retries", "3",
            "--default-search", "ytsearch",
            "-o", out_tmpl,
        ] + _cookies_args()

        cmd += ["-f", "bestaudio[ext=m4a]/bestaudio/best"]
        if fmt == "mp3" and ffmpeg_ok:
            aq_map = {"360": "7", "720": "5", "1080": "2", "0": "0"}
            cmd += ["--extract-audio", "--audio-format", "mp3", "--audio-quality", aq_map.get(quality, "5")]
        elif fmt in _AUDIO_EXTS and ffmpeg_ok:
            cmd += ["--extract-audio", "--audio-format", fmt]

        cmd.append(url)

        _log.info(f"[WORKER/spotify] running yt-dlp search for {url}")
        proc = subprocess.run(cmd, capture_output=True, text=True)

        if proc.returncode == 0:
            files_out = []
            for f in DOWNLOADS_DIR.iterdir():
                if f.is_file() and f.name.startswith(prefix):
                    sz = int(f.stat().st_size)
                    files_out.append({
                        "name": f.name,
                        "url": _file_url(f.name),
                        "kind": "audio",
                        "size": sz,
                        "ext": f.suffix.lstrip(".").lower()
                    })

            if files_out:
                _save_cache_entry(key, files_out, {"platform": "spotify", "kind": "audio", "format": fmt})
                _result_put(key, {"ok": True, "files": files_out, "cached": False, "source": "worker-spotify"})
                return jsonify({"ok": True, "files": files_out, "cached": False, "source": "worker"}), 200

        err = proc.stderr.strip()
        _result_put(key, {"ok": False, "files": [], "message": f"Spotify yt-dlp error: {err[:200]}"})
        return jsonify({"ok": False, "files": [], "message": f"Spotify extract failed: {err[:100]}"}), 500

    except Exception as exc:
        _result_put(key, {"ok": False, "files": [], "message": f"Spotify error: {exc}"})
        return jsonify({"ok": False, "files": [], "message": str(exc)}), 500
    finally:
        with PENDING_LOCK:
            if key in PENDING:
                if "event" in PENDING[key]:
                    PENDING[key]["event"].set()
                del PENDING[key]


# ---------------------------------------------------------------------------
# Flask: POST /download
# ---------------------------------------------------------------------------

@app.post("/download")
def download_media():
    """
    Descarga audio / video / imagen.

    Acepta aliases: kind | type | mode | downloadType | mediaType
    Bug corregido: format=mp3 fuerza kind=audio incluso si no se envió kind.
    """
    data = _safe_json()
    parsed = _parse_download_request(data)
    raw_url = parsed["url"]
    kind = parsed["kind"]
    fmt = parsed["format"]
    quality = parsed["quality"]

    if not raw_url:
        return jsonify({"ok": False, "files": [], "message": "Missing 'url'"}), 400
    if not is_allowed_media_url(raw_url):
        return jsonify({"ok": False, "files": [], "message": "URL not allowed"}), 400

    # Filtro por modo: vibe solo acepta audio
    if WORKER_MODE == "vibe" and kind != "audio":
        return jsonify({"ok": False, "files": [], "message": f"Worker mode 'vibe' only supports audio, got '{kind}'"}), 403

    DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)

    # --- Caso especial: imágenes ---
    if kind == "image":
        key = _cache_key_for(raw_url, kind, fmt, quality)
        cached = _cached_files_from_index(key)
        if cached:
            print(f"[WORKER/download] cache-hit key={key}", flush=True)
            return jsonify({"ok": True, "files": cached, "cached": True, "source": "worker-cache"})

        print(f"[WORKER/download] start key={key} kind=image", flush=True)
        files_out: List[Dict[str, Any]] = []

        if is_direct_image_url(raw_url):
            img_urls_to_dl = [raw_url]
        elif isinstance(data.get("image_urls"), list) and data.get("image_urls"):
            img_urls_to_dl = [str(u).strip() for u in data["image_urls"] if str(u).strip().startswith("http")]
        else:
            img_urls_to_dl = _extract_image_urls(raw_url)

        for i, u in enumerate(img_urls_to_dl[:35]):  # Fix #3: TikTok Photo puede tener hasta 35 slides
            ext = Path(urlparse(u).path).suffix.lstrip(".").lower()
            if ext not in _IMAGE_EXTS:
                ext = "jpg"
            base = sanitize_filename(f"{_sha1(u)[:10]}_{i+1}.{ext}", f"img_{i+1}.{ext}")
            dest = (DOWNLOADS_DIR / base).resolve()
            stored = _download_binary(u, dest)
            if not stored:
                continue
            size = int(stored.stat().st_size)
            files_out.append({
                "name": stored.name,
                "url": _file_url(stored.name),
                "kind": "image",
                "size": size,
                "ext": ext,
            })

        if files_out:
            _save_cache_entry(key, files_out, {"platform": detect_platform(raw_url), "kind": kind, "format": fmt})
        payload = {"ok": bool(files_out), "files": files_out, "cached": False, "source": "worker"}
        return jsonify(payload), (200 if files_out else 502)

    # --- Spotify: spotdl o fallback YouTube search ---
    platform = detect_platform(raw_url)
    if platform == "spotify":
        return _handle_spotify_download(raw_url, kind, fmt, quality)

    # --- Audio / Video: yt-dlp ---
    if platform == "pinterest":
        yt_url = _normalize_pinterest_url(raw_url)
    else:
        yt_url = normalize_youtube_url(raw_url) or raw_url
    key = _cache_key_for(yt_url, kind, fmt, quality)

    # Verificar cache
    cached = _cached_files_from_index(key)
    if cached:
        print(f"[WORKER/download] cache-hit key={key}", flush=True)
        return jsonify({"ok": True, "files": cached, "cached": True, "source": "worker-cache"})

    # Pending: evitar descargas simultáneas del mismo media
    join_event = None
    with PENDING_LOCK:
        if key in PENDING:
            join_event = PENDING[key]["event"]
        else:
            ev = threading.Event()
            PENDING[key] = {"event": ev}
            join_event = None

    if join_event is not None:
        print(f"[WORKER/download] join-pending key={key}", flush=True)
        join_event.wait(timeout=240)
        result = _result_get(key)
        if result is not None:
            return jsonify(result)
        return jsonify({"ok": False, "files": [], "message": "Worker busy"}), 503

    status = 200
    payload: Dict[str, Any] = {"ok": False, "files": [], "message": "Unhandled error"}

    try:
        ytdlp_ok, _ytdlp_version, ffmpeg_ok = _detect_tools()
        if not ytdlp_ok:
            status = 500
            payload = {"ok": False, "files": [], "message": "yt-dlp not available"}
            return jsonify(payload), status

        # Construir prefijo único para identificar los archivos generados
        media_id = extract_youtube_id(yt_url) if platform == "youtube" else _sha1(yt_url)[:12]
        prefix = sanitize_filename(f"{platform}_{media_id}_{kind}_{fmt}_{quality}", f"{platform}_{media_id}")
        out_tmpl = str((DOWNLOADS_DIR / f"{prefix}.%(ext)s").resolve())

        cmd = [
            sys.executable, "-m", "yt_dlp",
            "--no-playlist", "--no-warnings", "--quiet",
            "--socket-timeout", "30",
            "--retries", "3",
            "--fragment-retries", "3",
            "-o", out_tmpl,
        ] + _cookies_args()

        if kind == "audio":
            # Siempre bestaudio; convertir a mp3 solo si hay ffmpeg
            cmd += ["-f", "bestaudio[ext=m4a]/bestaudio/best"]
            if fmt == "mp3" and ffmpeg_ok:
                aq_map = {"360": "7", "720": "5", "1080": "2", "0": "0"}
                aq = aq_map.get(quality, "5")
                cmd += ["--extract-audio", "--audio-format", "mp3", "--audio-quality", aq]
            elif fmt in _AUDIO_EXTS and ffmpeg_ok:
                cmd += ["--extract-audio", "--audio-format", fmt]
        else:
            # Video
            try:
                h = int(quality)
            except Exception:
                h = 720
            if h == 0:
                h_str = ""
            else:
                h_str = f"[height<={h}]"

            if ffmpeg_ok:
                cmd += ["-f", (
                    f"bv*{h_str}[ext=mp4]+ba[ext=m4a]/"
                    f"bv*{h_str}+ba/"
                    f"b{h_str}[ext=mp4]/"
                    f"best{h_str}/"
                    "18/best"
                )]
                cmd += ["--merge-output-format", fmt or "mp4"]
            else:
                cmd += ["-f", (
                    f"b{h_str}[ext=mp4]/"
                    f"best{h_str}[ext=mp4]/"
                    f"best{h_str}/"
                    "18/best[ext=mp4]/best"
                )]

        cmd.append(yt_url)
        print(f"[WORKER/download] start key={key} kind={kind} platform={platform}", flush=True)

        # Snapshot de archivos antes de descargar
        try:
            before_names: set = {f.name for f in DOWNLOADS_DIR.iterdir() if f.is_file()}
        except Exception:
            before_names = set()

        def _run_download(extra_args: List[str] = []) -> "subprocess.CompletedProcess[str]":
            return subprocess.run(
                cmd + extra_args,
                capture_output=True, text=True,
                timeout=DOWNLOAD_TIMEOUT_SEC, check=False
            )

        _BOT_KEYWORDS = ("sign in", "bot", "cookies", "confirm", "login")

        try:
            proc = _run_download()

            # Si YouTube detecta bot → reintentar con cliente TV (no necesita cookies)
            if proc.returncode != 0 and platform == "youtube":
                stderr_lower = (proc.stderr or "").lower()
                if any(kw in stderr_lower for kw in _BOT_KEYWORDS):
                    print(f"[WORKER/download] bot-detected, retrying with tv_embedded client key={key}", flush=True)
                    proc = _run_download(["--extractor-args", "youtube:player_client=tv_embedded,web"])

        except subprocess.TimeoutExpired:
            status = 504
            payload = {"ok": False, "files": [], "message": "Download timeout"}
            return jsonify(payload), status

        if proc.returncode != 0:
            stderr_tail = "\n".join((proc.stderr or "").splitlines()[-5:])
            print(f"[WORKER/download] failed key={key} rc={proc.returncode}", flush=True)
            status = 502
            payload = {"ok": False, "files": [], "message": "yt-dlp failed", "returncode": proc.returncode}
            return jsonify(payload), status

        # Buscar archivos nuevos
        new_files = _find_new_files(prefix, before_names)
        if not new_files:
            print(f"[WORKER/download] failed key={key} reason=no-file", flush=True)
            status = 502
            payload = {"ok": False, "files": [], "message": "No output file"}
            return jsonify(payload), status

        files_out = []
        for pth in new_files[:1]:  # tomar solo el primero (el más nuevo)
            # Renombrar si necesario
            safe_name = sanitize_filename(pth.name, pth.name)
            if safe_name != pth.name:
                new_path = pth.parent / safe_name
                try:
                    pth.rename(new_path)
                    pth = new_path
                except Exception:
                    safe_name = pth.name
            ext = pth.suffix.lstrip(".").lower()
            actual_kind = detect_kind_from_path(pth)
            size = int(pth.stat().st_size)
            files_out.append({
                "name": pth.name,
                "url": _file_url(pth.name),
                "kind": actual_kind,
                "size": size,
                "ext": ext,
            })

        _save_cache_entry(key, files_out, {
            "platform": platform, "kind": kind, "format": fmt, "quality": quality,
        })
        payload = {"ok": True, "files": files_out, "cached": False, "source": "worker"}
        print(f"[WORKER/download] ok key={key}", flush=True)
        try:
            _maybe_cleanup_throttled()
        except Exception:
            pass
        return jsonify(payload), 200

    finally:
        _pending_done(key, payload)


# ---------------------------------------------------------------------------
# Flask: GET /files/<filename>
# ---------------------------------------------------------------------------

@app.get("/files/<path:filename>")
def serve_downloaded_file(filename: str):
    """Sirve archivos descargados. Bloquea path traversal."""
    name = str(filename or "")
    # Bloquear caracteres peligrosos
    if (not name
            or name != os.path.basename(name)
            or ".." in name
            or "/" in name
            or "\\" in name
            or "\x00" in name
            or ";" in name):
        return jsonify({"ok": False, "message": "Not found"}), 404
    if not DOWNLOADS_DIR.exists():
        return jsonify({"ok": False, "message": "Not found"}), 404
    fp = (DOWNLOADS_DIR / name).resolve()
    # Asegurar que está dentro de DOWNLOADS_DIR
    if not str(fp).startswith(str(DOWNLOADS_DIR)):
        return jsonify({"ok": False, "message": "Not found"}), 404
    if not fp.exists() or not fp.is_file():
        return jsonify({"ok": False, "message": "Not found"}), 404
    return send_from_directory(str(DOWNLOADS_DIR), name, as_attachment=False)


# ---------------------------------------------------------------------------
# Flask: POST /cookies
# ---------------------------------------------------------------------------

@app.post("/cookies")
def upload_cookies_route():
    """Permite sincronizar ~/cookies.txt directamente vía HTTP desde la PC o el VPS."""
    data = _safe_json()
    cookies_text = str(data.get("cookies_text") or "").strip()
    if not cookies_text or "Netscape" not in cookies_text and "\t" not in cookies_text:
        return jsonify({"ok": False, "message": "Invalid cookies_text"}), 400
    try:
        dest = Path.home() / "cookies.txt"
        dest.write_text(cookies_text + "\n", encoding="utf-8")
        return jsonify({"ok": True, "cookies_file": str(dest), "bytes": dest.stat().st_size})
    except Exception as exc:
        return jsonify({"ok": False, "message": str(exc)}), 500


# ---------------------------------------------------------------------------
# Flask: POST /cleanup
# ---------------------------------------------------------------------------

@app.post("/cleanup")
def cleanup_route():
    """Limpieza manual: borra archivos >30min o si supera 2GB."""
    try:
        result = _cleanup_impl()
        return jsonify({"ok": True, **result})
    except Exception as exc:
        return jsonify({"ok": True, "deletedCount": 0, "deleted": [], "totalBytes": 0, "error": str(exc)})


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)
    print(f"[WORKER] Universal Media Worker v{VERSION}", flush=True)
    print(f"[WORKER] Listening on {LISTEN_HOST}:{LISTEN_PORT}", flush=True)
    print(f"[WORKER] Downloads dir: {DOWNLOADS_DIR}", flush=True)

    # Startup cleanup
    try:
        _cleanup_impl()
    except Exception:
        pass

    # Verificar herramientas al inicio
    try:
        ytdlp_ok, ytdlp_version, ffmpeg_ok = _detect_tools()
        if ytdlp_ok:
            print(f"[WORKER] yt-dlp: {ytdlp_version}", flush=True)
        else:
            print("[WORKER] WARNING: yt-dlp NOT FOUND! pip install yt-dlp", flush=True)
        print(f"[WORKER] ffmpeg: {'available' if ffmpeg_ok else 'NOT FOUND (audio conversion disabled)'}", flush=True)
    except Exception:
        pass

    app.run(host=LISTEN_HOST, port=LISTEN_PORT, debug=False, threaded=True)
