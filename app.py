from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import hashlib
import json
import logging
from logging.handlers import RotatingFileHandler
import mimetypes
import os
import re
import shutil
import sys
import threading
import time
import urllib.error
import urllib.request

from flask import Flask, abort, jsonify, render_template, request, send_file, g

from media_tools import (
    process_links,
    probe_download_options,
    probe_media_capabilities,
    probe_image_candidates,
    resolve_tiktok_url_for_detection,
    validate_instagram_cookiefile,
)
from worker_client import (
    worker_enabled,
    is_blocking_error,
    call_worker_extract,
    call_worker_download,
    download_worker_files_to_local,
    worker_extract_tiktok_photos,
    worker_extract_instagram,
    worker_download_youtube,
    worker_download_instagram,
    log_worker_status,
)

ROOT_DIR = Path(os.getcwd()).resolve()
DOWNLOADS_DIR = (ROOT_DIR / "downloads").resolve()
PREVIEW_DIR = (ROOT_DIR / "preview_cache").resolve()
SERVED_DIR = (ROOT_DIR / "served_files").resolve()
DOWNLOADS_DIR.mkdir(exist_ok=True)
PREVIEW_DIR.mkdir(exist_ok=True)
SERVED_DIR.mkdir(exist_ok=True)

MAX_SESSION_AGE_SECONDS = int(os.environ.get("MAX_SESSION_AGE_SECONDS", "1800"))
MAX_SESSION_COUNT = int(os.environ.get("MAX_SESSION_COUNT", "5"))
MAX_DOWNLOADS_SIZE_MB = int(os.environ.get("MAX_DOWNLOADS_SIZE_MB", "2048"))
MAX_CONCURRENT_DOWNLOADS = int(os.environ.get("MAX_CONCURRENT_DOWNLOADS", "2"))
MAX_REQ_PER_MIN = int(os.environ.get("MAX_REQ_PER_MIN", "60"))
HISTORY_MAX_ITEMS = int(os.environ.get("HISTORY_MAX_ITEMS", "30"))
AUTODESTRUCT_INTERVAL_SECONDS = int(os.environ.get("AUTODESTRUCT_INTERVAL_SECONDS", "600"))
AUTODESTRUCT_MAX_AGE_SECONDS = int(os.environ.get("AUTODESTRUCT_MAX_AGE_SECONDS", "1800"))

_DOWNLOAD_SEMAPHORE = threading.BoundedSemaphore(max(1, MAX_CONCURRENT_DOWNLOADS))
_RATE_LOCK = threading.Lock()
_REQ_TIMES_BY_IP: Dict[str, List[float]] = {}
_HISTORY_LOCK = threading.Lock()

AUDIO_EXTS = {".mp3", ".m4a", ".ogg", ".opus", ".wav", ".flac", ".aac", ".wma"}
VIDEO_EXTS = {".mp4", ".mkv", ".webm", ".avi", ".mov", ".flv", ".ts"}
IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp", ".tiff"}

_URL_RE = re.compile(r"^https?://\S+", re.IGNORECASE)

DEFAULTS: Dict[str, Dict[str, Any]] = {
    "audio": {
        "format_choices": ["mp3", "m4a", "opus", "ogg", "wav", "flac"],
        "detail_choices": ["320", "256", "192", "160", "128"],
        "format_value": "mp3",
        "detail_value": "192",
    },
    "video": {
        "format_choices": ["mp4", "webm", "mkv"],
        "detail_choices": ["1080", "720", "480", "360"],
        "format_value": "mp4",
        "detail_value": "720",
    },
    "image": {
        "format_choices": ["auto"],
        "detail_choices": ["original"],
        "format_value": "auto",
        "detail_value": "original",
    },
}


def _cookies_path() -> Optional[Path]:
    raw = (
        os.environ.get("MEDIA_DOWNLOADER_COOKIES_FILE")
        or os.environ.get("COOKIES_FILE")
        or os.environ.get("YTDLP_COOKIES_FILE")
        or ""
    ).strip()
    if raw:
        try:
            p = Path(raw).expanduser()
            if not p.is_absolute():
                p = (ROOT_DIR / p).resolve()
            p = p.resolve()
            if p.exists() and p.is_file():
                return p
        except Exception:
            return None

    insta_raw = (os.environ.get("MEDIA_DOWNLOADER_COOKIES_INSTAGRAM_FILE") or "").strip()
    tiktok_raw = (os.environ.get("MEDIA_DOWNLOADER_COOKIES_TIKTOK_FILE") or "").strip()

    def _resolve(path_str: str) -> Optional[Path]:
        if not path_str:
            return None
        try:
            p = Path(path_str).expanduser()
            if not p.is_absolute():
                p = (ROOT_DIR / p).resolve()
            p = p.resolve()
            return p if p.exists() and p.is_file() else None
        except Exception:
            return None

    insta = _resolve(insta_raw) or next(
        (
            p
            for p in [
                (ROOT_DIR / "cookies_instagram.txt").resolve(),
                (ROOT_DIR / "www.instagram.com_cookies.txt").resolve(),
                (ROOT_DIR / "instagram_cookies.txt").resolve(),
                (ROOT_DIR / "cookies" / "www.instagram.com_cookies.txt").resolve(),
            ]
            if p.exists() and p.is_file()
        ),
        None,
    )

    tiktok = _resolve(tiktok_raw) or next(
        (
            p
            for p in [
                Path("/app/cookies/tiktok/current.txt").resolve(),
                (ROOT_DIR / "www.tiktok.com_cookies.txt").resolve(),
                (ROOT_DIR / "tiktok_cookies.txt").resolve(),
                (ROOT_DIR / "cookies" / "www.tiktok.com_cookies.txt").resolve(),
            ]
            if p.exists() and p.is_file()
        ),
        None,
    )

    merge_raw = (os.environ.get("MEDIA_DOWNLOADER_COOKIES_MERGE", "1") or "").strip().lower()
    merge = merge_raw not in ("0", "false", "no", "off")
    if merge and insta and tiktok and insta != tiktok:
        try:
            tmp = (Path(os.environ.get("TMPDIR") or "/tmp") / "muxivo_cookies_merged.txt").resolve()
            a = insta.read_text(encoding="utf-8", errors="ignore")
            b = tiktok.read_text(encoding="utf-8", errors="ignore")
            tmp.write_text((a.rstrip() + "\n" + b.lstrip()).strip() + "\n", encoding="utf-8")
            return tmp
        except Exception:
            return insta

    return insta or tiktok


def _dir_size_bytes(path: Path) -> int:
    total = 0
    try:
        for item in path.rglob("*"):
            if item.is_file():
                total += item.stat().st_size
    except Exception:
        return total
    return total


def _cleanup_download_limits() -> None:
    session_dirs = [
        d for d in DOWNLOADS_DIR.iterdir() if d.is_dir() and d.name.startswith("session_")
    ]
    session_dirs.sort(key=lambda d: d.stat().st_mtime, reverse=True)

    for old_dir in session_dirs[MAX_SESSION_COUNT:]:
        shutil.rmtree(old_dir, ignore_errors=True)

    size_limit_bytes = max(0, MAX_DOWNLOADS_SIZE_MB) * 1024 * 1024
    if size_limit_bytes <= 0:
        return

    session_dirs = [
        d for d in DOWNLOADS_DIR.iterdir() if d.is_dir() and d.name.startswith("session_")
    ]
    session_dirs.sort(key=lambda d: d.stat().st_mtime, reverse=True)
    total_size = sum(_dir_size_bytes(d) for d in session_dirs)
    for old_dir in reversed(session_dirs):
        if total_size <= size_limit_bytes:
            break
        dir_size = _dir_size_bytes(old_dir)
        shutil.rmtree(old_dir, ignore_errors=True)
        total_size -= dir_size


def _history_dir() -> Path:
    d = SERVED_DIR / "history"
    d.mkdir(exist_ok=True)
    return d


def _history_path(session_id: str = "") -> Path:
    sid = _sanitize_session_id(session_id)
    if sid:
        return _history_dir() / f"history_{sid}.json"
    return SERVED_DIR / "history.json"


def _sanitize_session_id(session_id: str) -> str:
    raw = (session_id or "").strip()
    if not raw:
        return ""
    safe = re.sub(r"[^a-zA-Z0-9_-]", "", raw)
    return safe[:64]


def _append_history(entry: dict, session_id: str = "") -> None:
    if HISTORY_MAX_ITEMS <= 0:
        return
    with _HISTORY_LOCK:
        p = _history_path(session_id)
        try:
            data = json.loads(p.read_text(encoding="utf-8")) if p.exists() else []
            if not isinstance(data, list):
                data = []
        except Exception:
            data = []
        data.insert(0, entry)
        data = data[:HISTORY_MAX_ITEMS]
        p.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _rate_limit_ok(ip: str) -> bool:
    if MAX_REQ_PER_MIN <= 0:
        return True
    now = time.time()
    window = 60.0
    with _RATE_LOCK:
        times = _REQ_TIMES_BY_IP.get(ip, [])
        times = [t for t in times if now - t < window]
        if len(times) >= MAX_REQ_PER_MIN:
            _REQ_TIMES_BY_IP[ip] = times
            return False
        times.append(now)
        _REQ_TIMES_BY_IP[ip] = times
        return True


def _cleanup_old_sessions(max_age_seconds: int = MAX_SESSION_AGE_SECONDS) -> None:
    now = time.time()
    for d in DOWNLOADS_DIR.iterdir():
        if d.is_dir() and d.name.startswith("session_"):
            try:
                age = now - d.stat().st_mtime
                if age > max_age_seconds:
                    shutil.rmtree(d, ignore_errors=True)
            except Exception:
                pass
    _cleanup_download_limits()
    for f in PREVIEW_DIR.iterdir():
        if f.is_file() and f.name.startswith("preview_"):
            try:
                age = now - f.stat().st_mtime
                if age > max_age_seconds:
                    f.unlink(missing_ok=True)
            except Exception:
                pass
    for f in SERVED_DIR.iterdir():
        if f.is_file():
            try:
                age = now - f.stat().st_mtime
                if age > max_age_seconds:
                    f.unlink(missing_ok=True)
            except Exception:
                pass


def _autodestruct_once(max_age_seconds: int = AUTODESTRUCT_MAX_AGE_SECONDS) -> None:
    now = time.time()
    for d in DOWNLOADS_DIR.iterdir():
        if d.is_dir() and d.name.startswith("session_"):
            try:
                age = now - d.stat().st_mtime
                if age > max_age_seconds:
                    shutil.rmtree(d, ignore_errors=True)
            except Exception:
                pass
    for root in (PREVIEW_DIR, SERVED_DIR):
        try:
            for p in root.rglob("*"):
                try:
                    age = now - p.stat().st_mtime
                    if age <= max_age_seconds:
                        continue
                    if p.is_file():
                        p.unlink(missing_ok=True)
                    elif p.is_dir():
                        shutil.rmtree(p, ignore_errors=True)
                except Exception:
                    pass
        except Exception:
            pass
    for root in (PREVIEW_DIR, SERVED_DIR):
        try:
            for p in sorted(root.rglob("*"), reverse=True):
                if p.is_dir():
                    try:
                        if not any(p.iterdir()):
                            p.rmdir()
                    except Exception:
                        pass
        except Exception:
            pass


def _start_autodestruct_daemon() -> None:
    def _loop() -> None:
        while True:
            try:
                _autodestruct_once()
            except Exception:
                pass
            time.sleep(max(30, AUTODESTRUCT_INTERVAL_SECONDS))

    t = threading.Thread(target=_loop, name="muxivo-autodestruct", daemon=True)
    t.start()


def _filter_valid_urls(text: str) -> str:
    def _extract_url(s: str) -> Optional[str]:
        t = (s or "").strip()
        if not t:
            return None
        m = re.search(r"https?://\S+", t, re.IGNORECASE)
        if not m:
            return None
        u = m.group(0).strip()
        u = u.strip("`'\"<>[](){}")
        u = u.rstrip("`'\"<>)]}.;,")
        return u if _URL_RE.match(u) else None

    lines = (text or "").strip().splitlines()
    valid: List[str] = []
    for line in lines:
        u = _extract_url(line)
        if u:
            valid.append(u)
    return "\n".join(valid)


def _safe_int(val: Any, default: Optional[int] = None) -> Optional[int]:
    if val is None:
        return default
    try:
        return int(val)
    except (ValueError, TypeError):
        return default


def _file_kind_for_path(path: Path) -> str:
    ext = path.suffix.lower()
    if ext in AUDIO_EXTS:
        return "audio"
    if ext in VIDEO_EXTS:
        return "video"
    if ext in IMAGE_EXTS:
        return "image"
    return "file"


def _copy_for_preview(src: Path) -> Path:
    suffix = src.suffix or ".bin"
    dst = PREVIEW_DIR / f"preview_{os.urandom(4).hex()}{suffix}"
    shutil.copy2(src, dst)
    return dst.resolve()


def _cache_preview_image(url: str, *, referer: Optional[str] = None) -> Optional[Path]:
    u = (url or "").strip()
    if not u:
        return None
    if not _URL_RE.match(u):
        return None

    ext = (Path(u.split("?", 1)[0].split("#", 1)[0]).suffix or "").lower()
    if ext not in IMAGE_EXTS:
        ext = ""

    ref_key = (referer or "").strip()
    key = hashlib.sha1((u + "|" + ref_key).encode("utf-8", errors="ignore")).hexdigest()[:32]
    dst = (PREVIEW_DIR / f"preview_{key}{ext or '.jpg'}").resolve()
    try:
        if dst.exists() and dst.is_file() and dst.stat().st_size > 0:
            return dst
    except Exception:
        pass

    tmp = (PREVIEW_DIR / f".tmp_preview_{key}_{os.urandom(3).hex()}").resolve()
    headers: Dict[str, str] = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
    }
    if referer:
        headers["Referer"] = referer

    try:
        req = urllib.request.Request(u, headers=headers)
        with urllib.request.urlopen(req, timeout=25) as resp:
            ct = (resp.headers.get("Content-Type") or "").split(";")[0].strip().lower()
            data = resp.read()
        if not data:
            return None

        final_ext = ext
        if not final_ext and ct:
            guessed = mimetypes.guess_extension(ct) or ""
            if guessed.lower() in IMAGE_EXTS:
                final_ext = guessed.lower()
        if not final_ext:
            final_ext = ".jpg"

        final_dst = (PREVIEW_DIR / f"preview_{key}{final_ext}").resolve()
        with open(tmp, "wb") as f:
            f.write(data)
            f.flush()
        os.replace(tmp, final_dst)
        return final_dst
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError):
        try:
            if tmp.exists():
                tmp.unlink(missing_ok=True)  # type: ignore[arg-type]
        except Exception:
            pass
        return None
    except Exception:
        try:
            if tmp.exists():
                tmp.unlink(missing_ok=True)  # type: ignore[arg-type]
        except Exception:
            pass
        return None


def _rel_to_url(path: Path) -> str:
    rel = path.resolve().relative_to(ROOT_DIR).as_posix()
    return f"/files/{rel}"


def _path_to_relstr(path: Path) -> str:
    return path.resolve().relative_to(ROOT_DIR).as_posix()


def _try_resolve_allowed_relpath(relpath: str) -> Optional[Path]:
    try:
        rel = Path(relpath)
        if rel.is_absolute() or not rel.parts:
            return None
        base_name = rel.parts[0]
        base_dir = ALLOWED_BASES.get(base_name)
        if not base_dir:
            return None
        full = (ROOT_DIR / rel).resolve()
        if base_dir not in full.parents and full != base_dir:
            return None
        if not full.is_file():
            return None
        return full
    except Exception:
        return None


def _probe_image_candidates_from_url(url: str, max_items: int = 24) -> List[Dict[str, str]]:
    try:
        import yt_dlp  # type: ignore
    except Exception:
        return []

    class _NullLogger:
        def debug(self, msg: Any) -> None:
            return None

        def warning(self, msg: Any) -> None:
            return None

        def error(self, msg: Any) -> None:
            return None

    opts: Dict[str, Any] = {
        "quiet": True,
        "noprogress": True,
        "skip_download": True,
        "socket_timeout": 25,
        "no_warnings": True,
        "logger": _NullLogger(),
    }
    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(url, download=False)
    except Exception:
        return []

    urls: List[str] = []

    def _add(u: Any) -> None:
        if not u:
            return
        s = str(u).strip()
        if not _URL_RE.match(s):
            return
        urls.append(s)

    def _from_entry(entry: Any) -> None:
        if not isinstance(entry, dict):
            return
        thumbs = entry.get("thumbnails")
        if isinstance(thumbs, list):
            for t in thumbs:
                if isinstance(t, dict):
                    _add(t.get("url"))
        _add(entry.get("url"))

    if isinstance(info, dict):
        thumbs = info.get("thumbnails")
        if isinstance(thumbs, list):
            for t in thumbs:
                if isinstance(t, dict):
                    _add(t.get("url"))
        entries = info.get("entries")
        if isinstance(entries, list):
            for e in entries:
                _from_entry(e)

    dedup: List[str] = []
    seen = set()
    for u in urls:
        if u in seen:
            continue
        seen.add(u)
        dedup.append(u)
        if len(dedup) >= max(1, int(max_items)):
            break

    return [{"url": u, "label": f"IMAGE {i + 1}"} for i, u in enumerate(dedup)]


ALLOWED_BASES = {
    "downloads": DOWNLOADS_DIR,
    "preview_cache": PREVIEW_DIR,
    "served_files": SERVED_DIR,
}


def _resolve_allowed_relpath(relpath: str) -> Path:
    rel = Path(relpath)
    if rel.is_absolute() or not rel.parts:
        abort(404)
    base_name = rel.parts[0]
    base_dir = ALLOWED_BASES.get(base_name)
    if not base_dir:
        abort(404)
    full = (ROOT_DIR / rel).resolve()
    if base_dir not in full.parents and full != base_dir:
        abort(404)
    if not full.is_file():
        abort(404)
    return full


app = Flask(__name__, template_folder="templates", static_folder="static")
app.config["TEMPLATES_AUTO_RELOAD"] = True
app.jinja_env.auto_reload = True
app.config["SEND_FILE_MAX_AGE_DEFAULT"] = 0

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
    force=True,
)
_log = logging.getLogger("muxivo")
_log.setLevel(logging.INFO)
_wz = logging.getLogger("werkzeug")
_wz.setLevel(logging.INFO)
_wz.propagate = True

try:
    log_path = (SERVED_DIR / "server.log").resolve()
    fh = RotatingFileHandler(str(log_path), maxBytes=1024 * 1024, backupCount=3, encoding="utf-8")
    fh.setLevel(logging.INFO)
    fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logging.getLogger().addHandler(fh)
except Exception:
    pass


def _append_plain_log(line: str) -> None:
    try:
        p = (SERVED_DIR / "server.log").resolve()
        with open(p, "a", encoding="utf-8") as f:
            f.write(f"{line}\n")
            f.flush()
    except Exception:
        return None


@app.before_request
def _log_req_start():
    try:
        g._req_t0 = time.time()
        qs = request.query_string.decode("utf-8", errors="ignore") if request.query_string else ""
        start_line = f">> {request.method} {request.path}"
        if qs:
            start_line += f"?{qs}"

        body_preview = ""
        if request.method in ("POST", "PUT", "PATCH"):
            try:
                ct = (request.headers.get("Content-Type") or "").lower()
                if "application/json" in ct:
                    data = request.get_json(silent=True)
                    if isinstance(data, dict):
                        redacted = {}
                        for k, v in data.items():
                            key = str(k)
                            low = key.lower()
                            if any(x in low for x in ("cookie", "password", "token", "authorization")):
                                redacted[key] = "[REDACTED]"
                            elif low in ("raw_input",):
                                s = str(v or "")
                                redacted[key] = (s[:300] + "…") if len(s) > 300 else s
                            elif low in ("image_urls",):
                                if isinstance(v, list):
                                    redacted[key] = f"[{len(v)} urls]"
                                else:
                                    redacted[key] = "[urls]"
                            else:
                                s = str(v)
                                redacted[key] = (s[:200] + "…") if len(s) > 200 else s
                        body_preview = f" body={redacted}"
                    elif data is not None:
                        body_preview = " body=[json]"
                else:
                    raw = request.get_data(cache=True) or b""
                    if raw:
                        txt = raw.decode("utf-8", errors="ignore")
                        txt = txt.replace("\r", "\\r").replace("\n", "\\n")
                        if len(txt) > 500:
                            txt = txt[:500] + "…"
                        body_preview = f" body={txt}"
            except Exception:
                pass

        line = start_line + body_preview
        _log.info("%s", line)
        try:
            print(line, flush=True)
        except Exception:
            pass
        _append_plain_log(line)
    except Exception:
        return None


@app.after_request
def _log_req_end(resp):
    try:
        start = getattr(g, "_req_t0", None)
        ms = int((time.time() - start) * 1000) if start else None
        if ms is None:
            line = f"{request.method} {request.path} -> {resp.status_code}"
        else:
            line = f"{request.method} {request.path} -> {resp.status_code} ({ms}ms)"
        _log.info("%s", line)
        try:
            print(line, flush=True)
        except Exception:
            pass
        _append_plain_log(line)
    except Exception:
        pass
    return resp


@app.get("/")
def index():
    return render_template("index.html")


@app.get("/manifest.webmanifest")
def manifest():
    p = (ROOT_DIR / "manifest.webmanifest").resolve()
    if not p.exists():
        abort(404)
    return send_file(p, mimetype="application/manifest+json", max_age=3600)


@app.get("/service-worker.js")
def service_worker():
    p = (ROOT_DIR / "service-worker.js").resolve()
    if not p.exists():
        abort(404)
    resp = send_file(p, mimetype="application/javascript", max_age=0)
    try:
        resp.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        resp.headers["Pragma"] = "no-cache"
        resp.headers["Expires"] = "0"
    except Exception:
        pass
    return resp


@app.get("/icon-apk.png")
def icon_apk():
    p = (ROOT_DIR / "icon-apk.png").resolve()
    if not p.exists():
        abort(404)
    return send_file(p, mimetype="image/png", max_age=3600)


@app.get("/icon.png")
def icon_png():
    p = (ROOT_DIR / "icon.png").resolve()
    if not p.exists():
        abort(404)
    return send_file(p, mimetype="image/png", max_age=3600)


@app.get("/files/<path:relpath>")
def files(relpath: str):
    p = _resolve_allowed_relpath(relpath)
    as_attachment = request.args.get("download") in ("1", "true", "yes")
    return send_file(p, as_attachment=as_attachment, download_name=p.name)


@app.get("/api/options")
def api_options():
    raw_input = request.args.get("raw_input", "") or ""
    requested_kind = (request.args.get("kind", "") or "video").strip().lower()
    if requested_kind not in ("audio", "video", "image"):
        requested_kind = "video"

    if requested_kind == "image":
        return jsonify(DEFAULTS["image"])

    filtered = _filter_valid_urls(raw_input)
    if not filtered:
        return jsonify(DEFAULTS[requested_kind])

    cookies_path = _cookies_path()
    try:
        detected = probe_download_options(
            filtered,
            "video" if requested_kind == "video" else "audio",
            cookies_path=cookies_path,
        )
    except Exception:
        detected = {}

    if not detected:
        return jsonify(DEFAULTS[requested_kind])

    result = {
        "format_choices": detected.get("format_choices") or DEFAULTS[requested_kind]["format_choices"],
        "format_value": detected.get("format_value") or DEFAULTS[requested_kind]["format_value"],
        "detail_choices": detected.get("detail_choices") or DEFAULTS[requested_kind]["detail_choices"],
        "detail_value": detected.get("detail_value") or DEFAULTS[requested_kind]["detail_value"],
        "detected_from_source": True,
    }
    return jsonify(result)


@app.get("/api/history")
def api_history():
    session_id = _sanitize_session_id(request.args.get("session_id", "") or "")
    if not session_id:
        return jsonify({"items": []})

    try:
        p = _history_path(session_id)
        data = json.loads(p.read_text(encoding="utf-8")) if p.exists() else []
        if not isinstance(data, list):
            data = []
    except Exception:
        data = []

    items: List[Dict[str, Any]] = []
    for entry in data[: max(0, HISTORY_MAX_ITEMS)]:
        if not isinstance(entry, dict):
            continue
        files_out: List[Dict[str, str]] = []
        for f in entry.get("files") or []:
            if not isinstance(f, dict):
                continue
            relpath = f.get("relpath")
            if not relpath:
                continue
            resolved = _try_resolve_allowed_relpath(str(relpath))
            if not resolved:
                continue
            files_out.append(
                {
                    "name": (f.get("name") or resolved.name),
                    "url": _rel_to_url(resolved),
                    "kind": (f.get("kind") or _file_kind_for_path(resolved)),
                }
            )

        zip_obj = None
        zip_rel = entry.get("zip_relpath") or entry.get("zip")
        if zip_rel:
            resolved_zip = _try_resolve_allowed_relpath(str(zip_rel))
            if resolved_zip:
                zip_obj = {"name": resolved_zip.name, "url": _rel_to_url(resolved_zip), "kind": "zip"}

        items.append(
            {
                "ts": entry.get("ts"),
                "kind": entry.get("kind"),
                "ok": entry.get("ok"),
                "fail": entry.get("fail"),
                "zip": zip_obj,
                "files": files_out,
            }
        )

    return jsonify({"items": items})


@app.get("/api/detect")
def api_detect():
    raw_input = request.args.get("raw_input", "") or ""
    filtered = _filter_valid_urls(raw_input)
    if not filtered:
        return jsonify(
            {
                "platform": None,
                "resolved_url": None,
                "detected_kind": None,
                "allowed_kinds": None,
                "image_candidates": [],
                "type": None,
                "is_gallery": False,
                "items": [],
                "disable_modes": None,
            }
        )

    input_url = filtered.splitlines()[0].strip()
    cookies_path = _cookies_path()

    def _detect_payload(
        *,
        platform: Optional[str],
        resolved_url: Optional[str],
        detected_kind: Optional[str],
        allowed_kinds: Optional[List[str]],
        items: Optional[List[Dict[str, str]]] = None,
        files: Optional[List[Dict[str, str]]] = None,
        summary: Optional[str] = None,
        message: Optional[str] = None,
        error: Optional[str] = None,
    ) -> Dict[str, Any]:
        items_list: List[Dict[str, str]] = []
        for it in items or []:
            if not isinstance(it, dict):
                continue
            u = str(it.get("url") or "").strip()
            if not u:
                continue
            thumb = str(it.get("thumb") or "").strip()
            kind0 = str(it.get("kind") or "").strip().lower()
            if kind0 and kind0 not in ("image", "video", "audio", "file"):
                kind0 = ""
            row: Dict[str, Any] = {
                "url": u,
                "label": str(it.get("label") or "").strip() or "IMAGE",
                "fallback": bool(str(it.get("fallback") or "").strip() in ("1", "true", "yes")),
            }
            if thumb:
                row["thumb"] = thumb
            if kind0:
                row["kind"] = kind0
            items_list.append(row)  # type: ignore[arg-type]

        t: Optional[str] = None
        if detected_kind == "image":
            t = "GALLERY" if len(items_list) > 1 else "IMAGE"
        elif detected_kind == "video":
            t = "VIDEO"
        elif detected_kind == "audio":
            t = "AUDIO"

        disable_modes: Optional[List[str]] = None
        if isinstance(allowed_kinds, list) and allowed_kinds:
            all_modes = ["audio", "video", "image"]
            disable_modes = [m for m in all_modes if m not in allowed_kinds]

        out: Dict[str, Any] = {
            "platform": platform,
            "resolved_url": resolved_url,
            "summary": summary,
            "detected_kind": detected_kind,
            "allowed_kinds": allowed_kinds,
            "image_candidates": items_list,
            "type": t,
            "is_gallery": bool(t == "GALLERY"),
            "items": items_list,
            "disable_modes": disable_modes,
        }
        try:
            if platform == "instagram" and t == "GALLERY":
                cnt = len(items_list)
                uniq = len({str(x.get("url") or "").strip() for x in items_list if isinstance(x, dict)})
                print(f"[INSTAGRAM] gallery items count={cnt} unique_urls={uniq}", flush=True)
        except Exception:
            pass
        if isinstance(files, list):
            safe_files: List[Dict[str, str]] = []
            for f in files:
                if not isinstance(f, dict):
                    continue
                name = str(f.get("name") or "").strip()
                url = str(f.get("url") or "").strip()
                knd = str(f.get("kind") or "").strip().lower()
                if not url or not name:
                    continue
                if knd not in ("audio", "video", "image", "file", "zip"):
                    knd = "file"
                safe_files.append({"name": name, "url": url, "kind": knd})
            out["files"] = safe_files
        if message:
            out["message"] = message
        if error:
            out["error"] = error
        return out

    try:
        from urllib.parse import urlparse

        input_host = (urlparse(input_url).netloc or "").lower()
    except Exception:
        input_host = ""

    is_tiktok_short = input_host in ("vt.tiktok.com", "vm.tiktok.com", "t.tiktok.com")
    resolved_first, resolve_note = resolve_tiktok_url_for_detection(input_url)
    first_url = resolved_first or input_url
    resolved_out = first_url if first_url != input_url else None

    if is_tiktok_short and resolve_note and resolved_out is None:
        return jsonify(
            _detect_payload(
                platform="tiktok",
                resolved_url=None,
                detected_kind=None,
                allowed_kinds=None,
                items=[],
                error=resolve_note,
            )
        )

    is_direct_image = bool(
        re.search(r"\.(jpe?g|png|webp|gif|bmp|tiff)(\?|#|$)", first_url, re.IGNORECASE)
    )
    if is_direct_image:
        return jsonify(
            _detect_payload(
                platform="direct",
                resolved_url=resolved_out,
                detected_kind="image",
                allowed_kinds=["image"],
                items=[],
            )
        )

    parsed = None
    try:
        from urllib.parse import urlparse

        parsed = urlparse(first_url)
    except Exception:
        parsed = None

    host = (parsed.netloc or "").lower() if parsed else ""
    path = (parsed.path or "").lower() if parsed else ""
    insta_cookie_diag = None
    if "instagram.com" in host:
        try:
            insta_cookie_diag = validate_instagram_cookiefile(cookies_path)
        except Exception:
            insta_cookie_diag = None
    if "tiktok.com" in host:
        is_photo = "/photo/" in path
        should_probe = is_photo or is_tiktok_short or path.startswith("/t/")
        candidates: List[Dict[str, str]] = []
        if should_probe:
            try:
                candidates = probe_image_candidates(first_url, cookies_path=cookies_path)
            except Exception:
                candidates = []

        # --- WORKER FALLBACK: TikTok photo ---
        # Si no hay candidatos o solo hay preview fallback, llamar worker
        if is_photo and not candidates and worker_enabled():
            try:
                _log.info("[WORKER] TikTok photo detect: local empty, trying worker")
                wk_items = worker_extract_tiktok_photos(first_url, timeout=20)
                if wk_items:
                    candidates = wk_items
                    _log.info("[WORKER] TikTok photo detect: worker returned %d items", len(wk_items))
            except Exception as wk_exc:
                _log.warning("[WORKER] TikTok photo detect: worker failed: %s", wk_exc)

        if is_photo or candidates:
            def _is_fb(v: Any) -> bool:
                return str(v or "").strip().lower() in ("1", "true", "yes")

            msg = None
            if is_photo and not candidates:
                msg = (
                    "TikTok de imágenes no expone la galería completa en este entorno del servidor. "
                    "Solo se pudo obtener una vista previa. "
                    "Para descargar la galería completa, usa modo local o cookies/navegador compatibles."
                )
            elif is_photo and candidates and any(_is_fb(it.get("fallback")) for it in candidates if isinstance(it, dict)):
                # Hay solo preview fallback: intentar worker antes de resignarse
                if worker_enabled():
                    try:
                        _log.info("[WORKER] TikTok photo detect: only preview fallback, trying worker")
                        wk_items2 = worker_extract_tiktok_photos(first_url, timeout=20)
                        if wk_items2:
                            candidates = wk_items2
                            msg = None  # Worker resolvió, sin mensaje de preview
                            _log.info("[WORKER] TikTok photo detect: worker replaced preview with %d items", len(wk_items2))
                    except Exception:
                        pass
                if msg is None and candidates and any(_is_fb(it.get("fallback")) for it in candidates if isinstance(it, dict)):
                    msg = (
                        "TikTok de imágenes no expone la galería completa en este entorno del servidor. "
                        "Solo se pudo obtener una vista previa. "
                        "Para descargar la galería completa, usa modo local o cookies/navegador compatibles."
                    )

            preview_files: Optional[List[Dict[str, str]]] = None
            if is_photo and candidates and any(_is_fb(it.get("fallback")) for it in candidates if isinstance(it, dict)):
                try:
                    fb = next((it for it in candidates if isinstance(it, dict) and _is_fb(it.get("fallback"))), None)
                except Exception:
                    fb = None
                if isinstance(fb, dict):
                    remote_preview = str(fb.get("url") or "").strip()
                    cached = _cache_preview_image(remote_preview, referer=first_url)
                    if cached:
                        local_url = _rel_to_url(cached)
                        fb["url"] = local_url
                        name = f"preview{cached.suffix.lower() or '.jpg'}"
                        preview_files = [{"name": name, "url": local_url, "kind": "image"}]
            return jsonify(
                _detect_payload(
                    platform="tiktok",
                    resolved_url=resolved_out,
                    detected_kind="image",
                    allowed_kinds=["image"],
                    items=candidates,
                    files=preview_files,
                    message=msg or resolve_note,
                )
            )

    try:
        caps = probe_media_capabilities(first_url, cookies_path=cookies_path)
    except Exception:
        caps = {}

    allowed = caps.get("allowed_kinds") or []
    allowed_front: List[str] = []
    for k in allowed:
        if k == "imagen":
            allowed_front.append("image")
        elif k in ("audio", "video"):
            allowed_front.append(k)

    platform = caps.get("platform") or "other"
    summary = caps.get("summary")
    auth_required = bool(caps.get("auth_required"))
    supports_audio = bool(caps.get("supports_audio"))
    supports_video = bool(caps.get("supports_video"))
    supports_image = bool(caps.get("supports_image"))

    is_instagram_post = bool(platform == "instagram" and "/p/" in path and "/reel/" not in path)
    if (supports_video or supports_audio) and not is_instagram_post:
        allowed_front = [k for k in allowed_front if k != "image"]
    if supports_image and not (supports_video or supports_audio):
        allowed_front = ["image"]

    detected = caps.get("detected_kind")
    if "detected_front" not in locals():
        if detected == "imagen":
            detected_front: Optional[str] = "image"
        elif detected in ("audio", "video"):
            detected_front = str(detected)
        else:
            detected_front = None

    image_candidates: List[Dict[str, str]] = []
    if allowed_front == ["image"]:
        try:
            image_candidates = probe_image_candidates(first_url, cookies_path=cookies_path)
        except Exception:
            image_candidates = []

    # --- WORKER FALLBACK: Instagram gallery ---
    # Si es Instagram y la galería está vacía o tiene items repetidos, llamar worker
    if platform == "instagram" and allowed_front == ["image"] and worker_enabled():
        should_try_worker = False
        if not image_candidates:
            should_try_worker = True
            print("[WORKER] fallback reason=instagram_gallery_empty", flush=True)
        elif len(image_candidates) >= 2:
            # Detectar items repetidos
            urls_set = {str(c.get("url") or "").strip() for c in image_candidates if isinstance(c, dict)}
            if len(urls_set) < len(image_candidates):
                should_try_worker = True
                print("[WORKER] fallback reason=instagram_gallery_repeated_items", flush=True)

        if should_try_worker:
            try:
                _log.info("[WORKER] Instagram gallery detect: trying worker")
                wk_items = worker_extract_instagram(first_url, timeout=25)
                if wk_items:
                    image_candidates = wk_items
                    _log.info("[WORKER] Instagram gallery detect: worker returned %d items", len(wk_items))
            except Exception as wk_exc:
                _log.warning("[WORKER] Instagram gallery detect: worker failed: %s", wk_exc)

    return jsonify(
        _detect_payload(
            platform=platform,
            resolved_url=resolved_out,
            detected_kind=detected_front,
            allowed_kinds=allowed_front or None,
            items=image_candidates,
            summary=summary,
            message=(
                resolve_note
                or (
                    (
                        str(insta_cookie_diag.get("message") or "").strip()
                        if (
                            platform == "instagram"
                            and cookies_path
                            and isinstance(insta_cookie_diag, dict)
                            and (not bool(insta_cookie_diag.get("seems_valid")))
                        )
                        else None
                    )
                    or (
                    "Instagram requiere autenticación válida para acceder a este contenido."
                    if platform == "instagram" and auth_required and not cookies_path
                    else (
                        "No se pudo acceder al contenido de Instagram en este entorno, incluso usando cookies."
                        if platform == "instagram" and auth_required and cookies_path
                        else None
                    )
                    )
                )
            ),
        )
    )


@app.get("/api/thumb")
def api_thumb():
    u = (request.args.get("url", "") or "").strip()
    ref = (request.args.get("ref", "") or "").strip() or None
    if not u or not _URL_RE.match(u):
        abort(400)

    try:
        from urllib.parse import urlparse

        p = urlparse(u)
        host = (p.netloc or "").lower().strip()
        scheme = (p.scheme or "").lower().strip()
    except Exception:
        host = ""
        scheme = ""

    if scheme not in ("http", "https"):
        abort(400)

    allowed = (
        host.endswith("tiktokcdn.com")
        or host.endswith("tiktokcdn-us.com")
        or host.endswith("muscdn.com")
        or host.endswith("cdninstagram.com")
        or host.endswith("fbcdn.net")
        or ".tiktokcdn" in host
    )
    if not allowed:
        abort(403)

    cached = _cache_preview_image(u, referer=ref)
    if not cached:
        cached = _cache_preview_image(u, referer="https://www.tiktok.com/")
    if not cached:
        cached = _cache_preview_image(u, referer=None)
    if not cached:
        abort(502)

    mt = mimetypes.guess_type(str(cached))[0] or "application/octet-stream"
    resp = send_file(cached, mimetype=mt, conditional=True)
    try:
        resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        resp.headers["Pragma"] = "no-cache"
        resp.headers["Expires"] = "0"
    except Exception:
        pass
    return resp


@app.post("/api/download")
def api_download():
    payload = request.get_json(silent=True) or {}
    raw_input = (payload.get("raw_input") or "").strip()
    requested_kind = (payload.get("kind") or "video").strip().lower()
    session_id = _sanitize_session_id(payload.get("session_id") or "")
    fmt = (payload.get("format") or "").strip().lower()
    detail = (payload.get("detail") or "").strip()
    image_urls = payload.get("image_urls")

    ip = (request.headers.get("X-Forwarded-For", "") or "").split(",")[0].strip() or request.remote_addr or ""
    if ip and not _rate_limit_ok(ip):
        return jsonify({"message": "Demasiadas solicitudes. Intenta de nuevo en 1 minuto.", "tone": "warning"}), 429

    if requested_kind not in ("audio", "video", "image"):
        requested_kind = "video"

    filtered = _filter_valid_urls(raw_input)
    if not filtered:
        return jsonify({"message": "Pega al menos una URL válida (http/https).", "tone": "warning"}), 400

    selected_image_urls: Optional[List[str]] = None
    selected_local_paths: Optional[List[Path]] = None
    if requested_kind == "image" and isinstance(image_urls, list):
        remote: List[str] = []
        local: List[Path] = []
        for raw_u in image_urls:
            s = str(raw_u or "").strip()
            if not s:
                continue
            if _URL_RE.match(s):
                remote.append(s)
                continue
            if s.startswith("/files/"):
                rel = s[len("/files/") :].lstrip("/")
                p = _try_resolve_allowed_relpath(rel)
                if p:
                    local.append(p)
        if not remote and not local:
            return jsonify({"message": "Selecciona al menos una imagen.", "tone": "warning"}), 400
        selected_image_urls = remote if remote else []
        selected_local_paths = local or None
        try:
            first_url = _filter_valid_urls(raw_input).splitlines()[0].strip()
            if first_url and "instagram.com" in first_url.lower():
                uniq = len({str(u or "").strip() for u in (selected_image_urls or []) if str(u or "").strip()})
                print(f"[INSTAGRAM] download selected_images={len(selected_image_urls or [])} unique_urls={uniq}", flush=True)
        except Exception:
            pass

    _cleanup_old_sessions()

    if not _DOWNLOAD_SEMAPHORE.acquire(blocking=False):
        return jsonify({"message": "Servidor ocupado. Intenta de nuevo en unos segundos.", "tone": "warning"}), 429

    def _platform_for_url(url: str) -> str:
        try:
            from urllib.parse import urlparse

            host = (urlparse(url).netloc or "").lower()
        except Exception:
            host = ""
        if "tiktok.com" in host:
            return "TikTok"
        if "instagram.com" in host:
            return "Instagram"
        if "youtube.com" in host or "youtu.be" in host:
            return "YouTube"
        return "la plataforma"

    def _friendly_error(url: str, exc: Exception, has_cookies: bool) -> str:
        msg = str(exc or "")
        low = msg.lower()
        platform = _platform_for_url(url)
        try:
            from urllib.parse import urlparse

            path = (urlparse(url).path or "").lower()
        except Exception:
            path = ""

        auth_markers = [
            "login required",
            "please log in",
            "checkpoint required",
            "cookie",
            "consent",
            "requires login",
            "private",
            "not available",
        ]
        if platform == "Instagram" and any(m in low for m in auth_markers):
            if not has_cookies:
                return "Instagram requiere autenticación adicional y no se encontraron cookies válidas."
            return "No se pudo acceder al contenido incluso usando cookies."

        if "unsupported url" in low:
            if platform == "TikTok" and "/photo/" in path:
                return "Este tipo de enlace de TikTok (photo) aún no está soportado por el descargador actual."
            return "Este tipo de enlace aún no está soportado por el descargador actual."

        if "impersonation" in low or "no impersonate target" in low:
            if platform == "TikTok":
                return "TikTok requiere compatibilidad adicional del extractor en este entorno."
            return "La plataforma requiere compatibilidad adicional del extractor en este entorno."

        net_markers = [
            "getaddrinfo failed",
            "name or service not known",
            "temporary failure in name resolution",
            "failed to establish a new connection",
            "connection timed out",
            "timed out",
            "connection reset",
            "proxy",
            "ssl",
            "forbidden",
            "http error 403",
            "http error 429",
        ]
        if any(m in low for m in net_markers):
            return f"No se pudo conectar con {platform}. Verifica internet, VPN o acceso a la red social."

        return "No se pudo procesar el enlace. Verifica que la URL sea correcta y que la plataforma sea accesible."

    first_url_for_error = filtered.splitlines()[0].strip() if filtered else ""
    cookies_path = _cookies_path()

    def _is_instagram_post(url: str) -> bool:
        try:
            from urllib.parse import urlparse
            p = urlparse(url)
            host = (p.netloc or "").lower()
            path = (p.path or "").lower()
        except Exception:
            return False
        return ("instagram.com" in host) and ("/p/" in path) and ("/reel/" not in path)

    if requested_kind in ("audio", "video") and _is_instagram_post(first_url_for_error):
        try:
            caps = probe_media_capabilities(first_url_for_error, cookies_path=cookies_path)
        except Exception:
            caps = {}
        auth_required = bool(caps.get("auth_required"))
        supports_video = bool(caps.get("supports_video"))
        supports_image = bool(caps.get("supports_image"))
        if auth_required:
            return jsonify(
                {
                    "message": "Instagram bloqueó el acceso al contenido incluso usando cookies.",
                    "tone": "warning",
                }
            ), 400
        if not supports_video:
            msg = "No se pudo determinar el tipo real del post de Instagram en este entorno."
            if supports_image:
                msg = "El post parece ser IMAGE/GALLERY. Cambia a IMAGE para descargar."
            return jsonify({"message": msg, "tone": "warning"}), 400

    _semaphore_released = False
    try:
        kind_for_tools = "imagen" if requested_kind == "image" else requested_kind
        audio_format = fmt or DEFAULTS["audio"]["format_value"]
        abr_kbps = _safe_int(detail, 192)
        container = fmt or DEFAULTS["video"]["format_value"]
        max_height = _safe_int(detail, 720) if detail.lower() != "best" else 9999

        _log.info(
            "download_start kind=%s urls=%s cookies=%s",
            requested_kind,
            len([l for l in filtered.splitlines() if l.strip()]),
            "yes" if cookies_path else "no",
        )
        zip_path, generated, failures = process_links(
            raw_input=filtered,
            kind=kind_for_tools,
            audio_format=audio_format,
            abr_kbps=int(abr_kbps or 192),
            container=container,
            max_height=int(max_height or 720),
            per_channel_folders=True,
            cookies_path=cookies_path,
            proxy_url=None,
            force_ipv4=True,
            sleep_between=0.0,
            sleep_requests=0.0,
            geo_country=None,
            max_retries=4,
            pl_start=None,
            pl_end=None,
            po_token=None,
            selected_image_urls=selected_image_urls,
            selected_local_paths=selected_local_paths,
        )
    except Exception as e:
        error_msg = str(e or "")

        # --- WORKER FALLBACK: Download ---
        # Si el error indica bloqueo/cookies/bot y el worker está habilitado
        if worker_enabled() and is_blocking_error(error_msg):
            _log.info("[WORKER] fallback reason=%s", error_msg[:120])
            try:
                worker_result_files: List[Path] = []
                worker_failures: List[Tuple[str, str]] = []

                from urllib.parse import urlparse as _urlparse
                _first_host = (_urlparse(first_url_for_error).netloc or "").lower()

                if requested_kind == "image":
                    # Para imágenes, usar extract
                    if "tiktok.com" in _first_host:
                        wk_items = worker_extract_tiktok_photos(first_url_for_error, timeout=25)
                    elif "instagram.com" in _first_host:
                        wk_items = worker_extract_instagram(first_url_for_error, timeout=25)
                    else:
                        wk_items = call_worker_extract(first_url_for_error, timeout=25)

                    if wk_items:
                        # Descargar las imágenes del worker al VPS
                        from pathlib import Path as _Path
                        import os as _os
                        from datetime import datetime as _dt
                        _dl_root = _Path("downloads")
                        _dl_root.mkdir(exist_ok=True)
                        _tmp_name = f"session_{_dt.now().strftime('%Y%m%d%H%M%S')}_{_os.urandom(4).hex()}"
                        _out_dir = _dl_root / _tmp_name / "out"
                        _out_dir.mkdir(parents=True, exist_ok=True)
                        worker_result_files, worker_failures = download_worker_files_to_local(
                            [{"url": it.get("url"), "name": f"image_{i+1}.jpg"} for i, it in enumerate(wk_items)],
                            _out_dir,
                            timeout=30,
                        )
                else:
                    # Para video/audio, usar download
                    wk_kind = requested_kind
                    wk_fmt = fmt or ("mp4" if requested_kind == "video" else "mp3")
                    wk_quality = detail or "720"

                    if "youtube.com" in _first_host or "youtu.be" in _first_host:
                        wk_files = worker_download_youtube(
                            first_url_for_error, kind=wk_kind, fmt=wk_fmt, quality=wk_quality, timeout=90
                        )
                    elif "instagram.com" in _first_host:
                        wk_files = worker_download_instagram(
                            first_url_for_error, kind=wk_kind, fmt=wk_fmt, quality=wk_quality, timeout=60
                        )
                    else:
                        wk_files = call_worker_download(
                            first_url_for_error, kind=wk_kind, fmt=wk_fmt, quality=wk_quality, timeout=60
                        )

                    if wk_files:
                        # Descargar archivos del worker al VPS
                        from pathlib import Path as _Path
                        import os as _os
                        from datetime import datetime as _dt
                        _dl_root = _Path("downloads")
                        _dl_root.mkdir(exist_ok=True)
                        _tmp_name = f"session_{_dt.now().strftime('%Y%m%d%H%M%S')}_{_os.urandom(4).hex()}"
                        _out_dir = _dl_root / _tmp_name / "out"
                        _out_dir.mkdir(parents=True, exist_ok=True)
                        worker_result_files, worker_failures = download_worker_files_to_local(
                            wk_files, _out_dir, timeout=90
                        )

                # Si el worker produjo archivos, retornar éxito
                if worker_result_files:
                    _DOWNLOAD_SEMAPHORE.release()
                    _semaphore_released = True
                    _log.info("[WORKER] download fallback ok: %d files", len(worker_result_files))

                    files_out: List[Dict[str, str]] = []
                    history_files_out: List[Dict[str, str]] = []
                    for p in worker_result_files:
                        pp = p.resolve()
                        item_kind = _file_kind_for_path(pp)
                        served = pp
                        if item_kind == "video":
                            try:
                                served = _copy_for_preview(pp)
                            except Exception:
                                served = pp
                        files_out.append({"name": pp.name, "url": _rel_to_url(served), "kind": item_kind})
                        try:
                            history_files_out.append(
                                {"name": pp.name, "relpath": _path_to_relstr(served), "kind": item_kind}
                            )
                        except Exception:
                            pass

                    ok_count = len(worker_result_files)
                    _append_history(
                        {
                            "ts": int(time.time()),
                            "kind": requested_kind,
                            "urls": 1,
                            "ok": ok_count,
                            "fail": len(worker_failures),
                            "files": history_files_out,
                            "worker": True,
                        },
                        session_id=session_id,
                    )

                    return jsonify(
                        {
                            "tone": "success",
                            "message": f"Se descargaron {ok_count} archivo(s) via worker externo.",
                            "files": files_out,
                            "failures": [{"url": u, "reason": r} for (u, r) in worker_failures],
                        }
                    )
                else:
                    _log.info("[WORKER] download fallback: worker returned no files")
            except Exception as wk_exc:
                _log.warning("[WORKER] download fallback failed: %s", wk_exc)

        status = 500
        if requested_kind in ("audio", "video") and _is_instagram_post(first_url_for_error):
            status = 400
        return jsonify({"message": _friendly_error(first_url_for_error, e, bool(cookies_path)), "tone": "error"}), status
    finally:
        if not _semaphore_released:
            _DOWNLOAD_SEMAPHORE.release()

    _log.info("download_end ok=%s fail=%s", len(generated or []), len(failures or []))

    files: List[Dict[str, str]] = []
    history_files: List[Dict[str, str]] = []
    for p in generated:
        pp = p.resolve()
        item_kind = _file_kind_for_path(pp)
        served = pp
        if item_kind == "video":
            try:
                served = _copy_for_preview(pp)
            except Exception:
                served = pp
        files.append({"name": pp.name, "url": _rel_to_url(served), "kind": item_kind})
        try:
            history_files.append(
                {"name": pp.name, "relpath": _path_to_relstr(served), "kind": item_kind}
            )
        except Exception:
            pass

    zip_url = _rel_to_url(Path(zip_path).resolve()) if zip_path else None
    zip_obj = {"name": Path(zip_path).name, "url": zip_url, "kind": "zip"} if zip_url else None

    ok_count = len(generated)
    fail_count = len(failures or [])

    # --- WORKER FALLBACK: 0 archivos generados con errores de bloqueo ---
    if ok_count == 0 and fail_count > 0 and worker_enabled():
        # Verificar si algún failure parece error de bloqueo
        failure_texts = " ".join(r for (_, r) in (failures or []))
        if is_blocking_error(failure_texts):
            _log.info("[WORKER] post-download fallback: 0 files, blocking errors detected")
            print(f"[WORKER] fallback reason={failure_texts[:120]}", flush=True)
            try:
                from urllib.parse import urlparse as _urlparse2
                _first_host2 = (_urlparse2(first_url_for_error).netloc or "").lower()

                wk_result_files: List[Path] = []
                wk_fail: List[Tuple[str, str]] = []

                if requested_kind == "image":
                    if "tiktok.com" in _first_host2:
                        wk_items = worker_extract_tiktok_photos(first_url_for_error, timeout=25)
                    elif "instagram.com" in _first_host2:
                        wk_items = worker_extract_instagram(first_url_for_error, timeout=25)
                    else:
                        wk_items = call_worker_extract(first_url_for_error, timeout=25)
                    if wk_items:
                        from pathlib import Path as _Path2
                        import os as _os2
                        from datetime import datetime as _dt2
                        _dl_root2 = _Path2("downloads")
                        _dl_root2.mkdir(exist_ok=True)
                        _tmp2 = f"session_{_dt2.now().strftime('%Y%m%d%H%M%S')}_{_os2.urandom(4).hex()}"
                        _out2 = _dl_root2 / _tmp2 / "out"
                        _out2.mkdir(parents=True, exist_ok=True)
                        wk_result_files, wk_fail = download_worker_files_to_local(
                            [{"url": it.get("url"), "name": f"image_{i+1}.jpg"} for i, it in enumerate(wk_items)],
                            _out2, timeout=30,
                        )
                else:
                    wk_kind2 = requested_kind
                    wk_fmt2 = fmt or ("mp4" if requested_kind == "video" else "mp3")
                    wk_q2 = detail or "720"
                    if "youtube.com" in _first_host2 or "youtu.be" in _first_host2:
                        wk_dl = worker_download_youtube(first_url_for_error, kind=wk_kind2, fmt=wk_fmt2, quality=wk_q2, timeout=90)
                    elif "instagram.com" in _first_host2:
                        wk_dl = worker_download_instagram(first_url_for_error, kind=wk_kind2, fmt=wk_fmt2, quality=wk_q2, timeout=60)
                    else:
                        wk_dl = call_worker_download(first_url_for_error, kind=wk_kind2, fmt=wk_fmt2, quality=wk_q2, timeout=60)
                    if wk_dl:
                        from pathlib import Path as _Path2
                        import os as _os2
                        from datetime import datetime as _dt2
                        _dl_root2 = _Path2("downloads")
                        _dl_root2.mkdir(exist_ok=True)
                        _tmp2 = f"session_{_dt2.now().strftime('%Y%m%d%H%M%S')}_{_os2.urandom(4).hex()}"
                        _out2 = _dl_root2 / _tmp2 / "out"
                        _out2.mkdir(parents=True, exist_ok=True)
                        wk_result_files, wk_fail = download_worker_files_to_local(wk_dl, _out2, timeout=90)

                if wk_result_files:
                    _log.info("[WORKER] post-download fallback ok: %d files", len(wk_result_files))
                    # Reemplazar los resultados
                    generated = wk_result_files
                    failures = wk_fail
                    ok_count = len(generated)
                    fail_count = len(failures)
                    # Recalcular files y zip
                    files = []
                    history_files = []
                    for p in generated:
                        pp = p.resolve()
                        item_kind = _file_kind_for_path(pp)
                        served = pp
                        if item_kind == "video":
                            try:
                                served = _copy_for_preview(pp)
                            except Exception:
                                served = pp
                        files.append({"name": pp.name, "url": _rel_to_url(served), "kind": item_kind})
                        try:
                            history_files.append({"name": pp.name, "relpath": _path_to_relstr(served), "kind": item_kind})
                        except Exception:
                            pass
                    zip_path = None
                    zip_url = None
                    zip_obj = None
            except Exception as wk_exc2:
                _log.warning("[WORKER] post-download fallback failed: %s", wk_exc2)

    if ok_count > 0 and fail_count == 0:
        tone = "success"
        message = f"Se descargaron {ok_count} archivo(s)."
    elif ok_count > 0:
        tone = "warning"
        message = f"Se descargaron {ok_count} archivo(s) y hubo {fail_count} fallo(s)."
    else:
        tone = "warning"
        message = "No se descargó ningún archivo."

    _append_history(
        {
            "ts": int(time.time()),
            "kind": requested_kind,
            "urls": len([l for l in filtered.splitlines() if l.strip()]),
            "ok": ok_count,
            "fail": fail_count,
            "zip": str(zip_path) if zip_path else None,
            "zip_relpath": _path_to_relstr(Path(zip_path)) if zip_path else None,
            "files": history_files,
        },
        session_id=session_id,
    )

    failures_out = [{"url": u, "reason": r} for (u, r) in (failures or [])]

    return jsonify(
        {
            "tone": tone,
            "message": message,
            "zip": zip_obj,
            "files": files,
            "failures": failures_out,
        }
    )


if __name__ == "__main__":
    _start_autodestruct_daemon()
    log_worker_status()
    port = int(os.environ.get("PORT", "7860"))
    debug = os.environ.get("FLASK_DEBUG", "").strip() in ("1", "true", "yes")
    app.run(host="0.0.0.0", port=port, debug=debug, use_reloader=False)
