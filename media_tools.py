# media_tools.py — yt-dlp + ffmpeg; clients tv_embedded→tv→web; ZIP amigável;
# playlist em lotes; backoff rate-limit; sleep-requests; Fallback IPv4/IPv6;
# NORMALIZAÇÃO de URLs (mobile/alias/shorts); leitura de proxies do ambiente;
# suporte a force_ipv4 vindo do app.
from __future__ import annotations

from pathlib import Path
from typing import List, Dict, Any, Tuple, Optional
import json
import os
import shutil
import time
import tempfile
import re
import ssl
import unicodedata
import traceback
import urllib.request
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
from datetime import datetime

import yt_dlp
from yt_dlp.utils import DownloadError

PLAYLIST_CHUNK_SIZE = 75  # itens por lote
IMAGE_EXTS = {"jpg", "jpeg", "png", "webp"}
VIDEO_EXTS = {"mp4", "mkv", "webm", "mov"}


# -------------------- utilidades --------------------
def _ffmpeg_location() -> Optional[str]:
    for env_key in ("FFMPEG_LOCATION", "FFMPEG_DIR", "FFMPEG_BINARY"):
        v = os.getenv(env_key)
        if v:
            p = Path(v)
            if p.is_file():
                return str(p.parent)
            if p.is_dir():
                return str(p)
    cand = Path("C:/ffmpeg/bin")
    if cand.exists():
        return str(cand)
    return None


def _env_proxy() -> Optional[str]:
    # prioriza HTTPS/ALL; aceita minúsculos também
    for k in ("HTTPS_PROXY", "ALL_PROXY", "HTTP_PROXY", "https_proxy", "all_proxy", "http_proxy"):
        v = os.environ.get(k)
        if v:
            return v
    return None


def _is_auth_error(msg: str) -> bool:
    low = (msg or "").lower()
    markers = [
        "login required",
        "please log in",
        "requires login",
        "checkpoint required",
        "cookie",
        "consent",
        "private",
        "not available",
        "this content is not available",
        "post isn't available",
        "post is not available",
        "instagram api is not granting access",
        "instagram sent an empty media response",
        "empty media response",
        "forbidden",
        "http error 403",
    ]
    return any(m in low for m in markers)


def _cookies_file_usable(p: Optional[Path]) -> Tuple[bool, Optional[int]]:
    if not p:
        return False, None
    try:
        if not p.exists() or not p.is_file():
            return False, None
        size = int(p.stat().st_size)
        if size <= 0:
            return False, size
        return True, size
    except Exception:
        return False, None


def _read_netscape_cookies(p: Optional[Path]) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    usable, _ = _cookies_file_usable(p)
    if not usable or not p:
        return out
    try:
        for line in p.read_text(encoding="utf-8", errors="ignore").splitlines():
            t = (line or "").strip()
            if not t or t.startswith("#"):
                continue
            parts = line.split("\t")
            if len(parts) < 7:
                continue
            domain, _flag, path0, secure, expires, name, value = parts[:7]
            out.append(
                {
                    "domain": str(domain or "").strip(),
                    "path": str(path0 or "/").strip() or "/",
                    "secure": str(secure or "").strip(),
                    "expires": str(expires or "").strip(),
                    "name": str(name or "").strip(),
                    "value": str(value or "").strip(),
                }
            )
    except Exception:
        return []
    return out


def _cookie_header_for_domain(cookies_path: Optional[Path], domain_hint: str) -> str:
    hint = str(domain_hint or "").lower().strip()
    if not hint:
        return ""
    rows = _read_netscape_cookies(cookies_path)
    pairs: List[str] = []
    for row in rows:
        domain = str(row.get("domain") or "").lower()
        name = str(row.get("name") or "").strip()
        value = str(row.get("value") or "").strip()
        if not name:
            continue
        if hint not in domain:
            continue
        pairs.append(f"{name}={value}")
    return "; ".join(pairs)


def validate_instagram_cookiefile(cookies_path: Optional[Path]) -> Dict[str, Any]:
    usable, size = _cookies_file_usable(cookies_path)
    out: Dict[str, Any] = {
        "has_cookie_file": bool(cookies_path),
        "cookies_usable": bool(usable),
        "cookie_size": int(size or 0),
        "instagram_cookie_count": 0,
        "has_instagram_domain": False,
        "has_sessionid": False,
        "is_suspicious": True,
        "seems_valid": False,
        "message": "No se encontró un archivo de cookies válido para Instagram.",
    }
    if not usable or not cookies_path:
        return out

    rows = _read_netscape_cookies(cookies_path)
    ig_rows = [r for r in rows if "instagram.com" in str(r.get("domain") or "").lower()]
    names = {str(r.get("name") or "").strip().lower() for r in ig_rows if r.get("name")}
    has_sessionid = "sessionid" in names
    has_csrf = "csrftoken" in names
    has_user = "ds_user_id" in names
    suspicious = (len(ig_rows) < 4) or (len(names) < 3) or (not has_sessionid)

    out.update(
        {
            "instagram_cookie_count": len(ig_rows),
            "has_instagram_domain": bool(ig_rows),
            "has_sessionid": has_sessionid,
            "is_suspicious": suspicious,
            "seems_valid": bool((not suspicious) and has_sessionid and (has_csrf or has_user)),
        }
    )
    if not ig_rows:
        out["message"] = (
            "Las cookies de Instagram no parecen contener cookies de .instagram.com. "
            "Vuelve a exportarlas desde un navegador logueado."
        )
    elif not has_sessionid:
        out["message"] = (
            "Las cookies de Instagram no parecen contener una sesión válida. "
            "Vuelve a exportarlas desde un navegador donde el post abra correctamente estando logueado."
        )
    elif suspicious:
        out["message"] = (
            "El archivo de cookies de Instagram parece incompleto para autenticación estable. "
            "Reexporta cookies completas con sesión activa (incluyendo sessionid/csrftoken)."
        )
    else:
        out["message"] = "Cookies de Instagram con estructura válida detectada."
    return out


def _should_log_cookies(url: str) -> bool:
    try:
        host = (urlparse(url).netloc or "").lower()
    except Exception:
        host = ""
    if "instagram.com" in host:
        return True
    raw = (os.environ.get("MEDIA_DOWNLOADER_DEBUG_COOKIES", "0") or "").strip().lower()
    return raw not in ("0", "false", "no", "off")


def _log_cookie_state(prefix: str, url: str, opts: Dict[str, Any], cookies_path: Optional[Path]) -> None:
    if not _should_log_cookies(url):
        return
    usable, size = _cookies_file_usable(cookies_path)
    try:
        cookiefile_in_opts = bool(opts.get("cookiefile"))
    except Exception:
        cookiefile_in_opts = False
    using = bool(usable and cookies_path)
    print(f"USING COOKIES: {using}", flush=True)
    print(f"COOKIE FILE: {str(cookies_path) if using else 'None'}", flush=True)
    print(
        f"{prefix} USING_COOKIES_PATH={bool(cookies_path)} COOKIES_USABLE={usable} "
        f"COOKIE_SIZE={size if size is not None else 'NA'} COOKIE_FILE={str(cookies_path) if cookies_path else 'None'} "
        f"YTDLP_COOKIEFILE_IN_OPTS={cookiefile_in_opts}",
        flush=True,
    )


def _extract_info_with_cookie_fallback(
    url: str,
    opts: Dict[str, Any],
    cookies_path: Optional[Path],
) -> Tuple[Optional[Dict[str, Any]], bool, bool]:
    used_cookies = False
    auth_failed = False
    try:
        if _is_youtube_url(url):
            try:
                url2 = _normalize_youtube_single_video_url(url)
                if url2 and url2 != url:
                    print(f"[YOUTUBE] Normalized URL: {url} -> {url2}", flush=True)
                    url = url2
            except Exception:
                pass
            try:
                opts["noplaylist"] = True
            except Exception:
                pass
        _log_cookie_state("[YTDLP extract]", url, opts, cookies_path)
        try:
            _apply_youtube_cookiefile(url, opts)
        except Exception:
            pass
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(url, download=False)
        return info, used_cookies, auth_failed
    except Exception as e:
        msg0 = str(e or "")
        low0 = msg0.lower()
        if "impersonate" in opts and (
            "curl:" in low0
            or "transporterror" in low0
            or "resolving timed out" in low0
            or "could not resolve" in low0
        ):
            try:
                opts0 = dict(opts)
                opts0.pop("impersonate", None)
                with yt_dlp.YoutubeDL(opts0) as ydl:
                    info = ydl.extract_info(url, download=False)
                return info, used_cookies, auth_failed
            except Exception:
                pass
        if _is_auth_error(str(e)):
            auth_failed = True
        if cookies_path and auth_failed and "cookiefile" not in opts:
            try:
                usable, _ = _cookies_file_usable(cookies_path)
                if not usable:
                    _log_cookie_state("[YTDLP extract] COOKIEFILE_UNUSABLE", url, opts, cookies_path)
                    return None, used_cookies, auth_failed
                opts2 = dict(opts)
                opts2["cookiefile"] = str(cookies_path)
                _log_cookie_state("[YTDLP extract] RETRY_WITH_COOKIEFILE", url, opts2, cookies_path)
                with yt_dlp.YoutubeDL(opts2) as ydl:
                    info = ydl.extract_info(url, download=False)
                return info, True, auth_failed
            except Exception:
                return None, True, auth_failed
        return None, used_cookies, auth_failed


class _NullLogger:
    def debug(self, msg: Any) -> None:
        return None

    def warning(self, msg: Any) -> None:
        return None

    def error(self, msg: Any) -> None:
        return None


class _YTDLPCaptureLogger:
    def __init__(self, max_lines: int = 250) -> None:
        self._max = max(50, int(max_lines))
        self._lines: List[str] = []

    def _add(self, level: str, msg: Any) -> None:
        try:
            s = str(msg)
        except Exception:
            s = repr(msg)
        line = f"{level}: {s}"
        self._lines.append(line)
        if len(self._lines) > self._max:
            self._lines = self._lines[-self._max :]

    def debug(self, msg: Any) -> None:
        self._add("DEBUG", msg)

    def info(self, msg: Any) -> None:
        self._add("INFO", msg)

    def warning(self, msg: Any) -> None:
        self._add("WARN", msg)

    def error(self, msg: Any) -> None:
        self._add("ERROR", msg)

    def tail(self, n: int = 60) -> str:
        try:
            nn = max(1, int(n))
        except Exception:
            nn = 60
        return "\n".join(self._lines[-nn:])


_DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)


def _has_curl_cffi() -> bool:
    try:
        import curl_cffi  # type: ignore

        raw = str(getattr(curl_cffi, "__version__", "") or "")
        parts = re.split(r"[^\d]+", raw)[:3]
        ver = tuple(int(p) for p in parts if p != "")
        if len(ver) >= 2:
            major, minor = ver[0], ver[1]
            if (major, minor) >= (0, 15):
                return False
            if (major, minor) < (0, 10):
                return False
        return True
    except Exception:
        return False


def _ydl_net_opts(
    *,
    proxy_url: Optional[str],
    cookies_path: Optional[Path],
    force_ipv4: Optional[bool],
    set_cookiefile: bool,
) -> Dict[str, Any]:
    opts: Dict[str, Any] = {}

    ua = (os.environ.get("MEDIA_DOWNLOADER_UA") or _DEFAULT_UA).strip()
    opts["http_headers"] = {"Accept-Language": "en-US,en;q=0.9", "User-Agent": ua}

    effective_proxy = proxy_url or _env_proxy()
    if effective_proxy:
        opts["proxy"] = effective_proxy

    if force_ipv4 is None or force_ipv4 is True:
        opts["source_address"] = "0.0.0.0"
        opts["force_ipv4"] = True
    else:
        opts["force_ipv6"] = True

    if set_cookiefile and cookies_path:
        opts["cookiefile"] = str(cookies_path)

    enable_impersonate = (os.environ.get("MEDIA_DOWNLOADER_IMPERSONATE", "0") or "").strip().lower()
    if enable_impersonate not in ("0", "false", "no", "off") and _has_curl_cffi():
        target_raw = (os.environ.get("YTDLP_IMPERSONATE") or "chrome").strip()
        try:
            from yt_dlp.networking.impersonate import ImpersonateTarget  # type: ignore

            target_obj = ImpersonateTarget.from_str(target_raw) if target_raw else ImpersonateTarget("chrome")
            opts["impersonate"] = target_obj
        except Exception:
            pass

    return opts


def _is_youtube_url(url: str) -> bool:
    try:
        host = (urlparse(url).netloc or "").lower()
    except Exception:
        host = ""
    if host == "youtu.be":
        return True
    return host.endswith("youtube.com") or host.endswith("googlevideo.com") or "youtube.com" in host


def _youtube_cookiefile_path() -> Optional[Path]:
    raw = (os.environ.get("MEDIA_DOWNLOADER_YOUTUBE_COOKIES_FILE") or "").strip()
    if not raw:
        for cand in ("youtube_cookies.txt", "www.youtube.com_cookies.txt", "/app/youtube_cookies.txt"):
            try:
                p0 = Path(cand).expanduser()
                if not p0.is_absolute():
                    p0 = (Path(os.getcwd()) / p0).resolve()
                p0 = p0.resolve()
                usable, _ = _cookies_file_usable(p0)
                if usable:
                    return p0
            except Exception:
                continue
        return None
    try:
        p = Path(raw).expanduser()
        if not p.is_absolute():
            p = (Path(os.getcwd()) / p).resolve()
        p = p.resolve()
        usable, _ = _cookies_file_usable(p)
        if usable:
            return p
        return None
    except Exception:
        return None


def _apply_youtube_cookiefile(url: str, opts: Dict[str, Any]) -> None:
    if not _is_youtube_url(url):
        return
    raw = (os.environ.get("MEDIA_DOWNLOADER_YOUTUBE_COOKIES_FILE") or "").strip()
    p_raw: Optional[Path] = None
    exists = False
    size: Optional[int] = None
    try:
        cand0 = raw or "www.youtube.com_cookies.txt"
        if cand0:
            p_raw = Path(cand0).expanduser()
            if not p_raw.is_absolute():
                p_raw = (Path(os.getcwd()) / p_raw).resolve()
            p_raw = p_raw.resolve()
            exists = bool(p_raw.exists() and p_raw.is_file())
            size = int(p_raw.stat().st_size) if exists else None
    except Exception:
        p_raw = None
        exists = False
        size = None

    p = _youtube_cookiefile_path()
    using = bool(p)
    if using and p:
        opts["cookiefile"] = str(p)
    else:
        try:
            opts.pop("cookiefile", None)
        except Exception:
            pass

    print(f"[YOUTUBE] Using cookies: {using}", flush=True)
    print(f"[YOUTUBE] Cookie file: {str(p) if p else '/app/youtube_cookies.txt'}", flush=True)
    print(f"[YOUTUBE] Cookie file exists: {exists}", flush=True)
    print(f"[YOUTUBE] Cookie size: {int(size) if isinstance(size, int) else 0}", flush=True)
    try:
        ver = ""
        try:
            ver = str(getattr(yt_dlp, "__version__", "") or "")
        except Exception:
            ver = ""
        if not ver:
            try:
                import yt_dlp.version as _ydv  # type: ignore

                ver = str(getattr(_ydv, "__version__", "") or "")
            except Exception:
                ver = ""
        if ver:
            print(f"[YOUTUBE] yt-dlp version: {ver}", flush=True)
    except Exception:
        pass
    try:
        print(f"[YOUTUBE] ffmpeg in PATH: {bool(shutil.which('ffmpeg'))}", flush=True)
    except Exception:
        pass


def _slugify(text: Optional[str], maxlen: int = 40) -> str:
    if not text:
        return "media"
    txt = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    txt = re.sub(r"[^A-Za-z0-9._-]+", "-", txt).strip("-._")
    if not txt:
        return "media"
    if len(txt) > maxlen:
        txt = txt[:maxlen].rstrip("-")
    return txt.lower()


def _guess_ext_from_url(u: Optional[str], default: str = "jpg") -> str:
    if not u:
        return default
    try:
        suffix = Path(urlparse(u).path).suffix.lower().lstrip(".")
        if suffix:
            return suffix
    except Exception:
        pass
    return default


# -------- NORMALIZAÇÃO DE URLS (mobile → desktop; alias → canônico) --------
def _clean_youtube_params(q: Dict[str, List[str]]) -> Dict[str, List[str]]:
    keep = {"v", "list", "t", "start", "index"}
    cleaned = {k: v for k, v in q.items() if k in keep and v}
    list_values = cleaned.get("list") or []
    has_single_video = bool(cleaned.get("v"))
    is_radio_mix = any(v.startswith("RD") for v in list_values)
    if has_single_video and is_radio_mix:
        cleaned.pop("list", None)
        cleaned.pop("index", None)
    return cleaned

def _norm_youtube(u: str) -> str:
    p = urlparse(u)
    host = (p.netloc or "").lower()
    path = p.path or ""
    q = parse_qs(p.query or "")

    if host in {"m.youtube.com", "music.youtube.com"}:
        host = "www.youtube.com"
    if host == "youtu.be":
        vid = path.strip("/").split("/")[0] if path.strip("/") else None
        if vid:
            q = _clean_youtube_params(q)
            q["v"] = [vid]
            new = p._replace(netloc="www.youtube.com", path="/watch", query=urlencode(q, doseq=True))
            return urlunparse(new)
        return u

    if host.endswith("youtube.com") and path.startswith("/shorts/"):
        parts = path.split("/")
        vid = parts[2] if len(parts) >= 3 else None
        if vid:
            q = _clean_youtube_params(q)
            q["v"] = [vid]
            new = p._replace(netloc="www.youtube.com", path="/watch", query=urlencode(q, doseq=True))
            return urlunparse(new)

    if host.endswith("youtube.com") and path.startswith("/live/"):
        parts = path.split("/")
        vid = parts[2] if len(parts) >= 3 else None
        if vid:
            q = _clean_youtube_params(q)
            q["v"] = [vid]
            new = p._replace(netloc="www.youtube.com", path="/watch", query=urlencode(q, doseq=True))
            return urlunparse(new)

    if host.endswith("youtube.com") and path.startswith("/watch"):
        q = _clean_youtube_params(q)
        new = p._replace(netloc="www.youtube.com", path="/watch", query=urlencode(q, doseq=True))
        return urlunparse(new)

    return u


def _normalize_youtube_single_video_url(u: str) -> str:
    p = urlparse(u)
    host = (p.netloc or "").lower()
    path = p.path or ""
    q = parse_qs(p.query or "")

    if host in {"m.youtube.com", "music.youtube.com"}:
        host = "www.youtube.com"
    if host == "youtu.be":
        vid = path.strip("/").split("/")[0] if path.strip("/") else ""
        if vid:
            new = p._replace(scheme="https", netloc="www.youtube.com", path="/watch", query=urlencode({"v": vid}))
            return urlunparse(new)
        return u

    if host.endswith("youtube.com") and path.startswith("/shorts/"):
        parts = path.split("/")
        vid = parts[2] if len(parts) >= 3 else ""
        if vid:
            new = p._replace(scheme="https", netloc="www.youtube.com", path="/watch", query=urlencode({"v": vid}))
            return urlunparse(new)

    if host.endswith("youtube.com") and path.startswith("/watch"):
        vid = (q.get("v") or [""])[0]
        if vid:
            new = p._replace(scheme="https", netloc="www.youtube.com", path="/watch", query=urlencode({"v": vid}))
            return urlunparse(new)

    return u


def _apply_youtube_ydl_tuning(url: str, params: Dict[str, Any], kind: str, max_height: int) -> None:
    if not _is_youtube_url(url):
        return
    params["noplaylist"] = True
    params["no_warnings"] = False
    h = int(max_height)
    if kind == "video":
        params["format"] = (
            f"bestvideo[height<=?{h}]+bestaudio/"
            f"best[height<=?{h}]/"
            f"bestvideo+bestaudio/"
            f"best"
        )
    elif kind == "audio":
        params["format"] = "bestaudio/best"

# -------- SHORT-URL RESOLVER (segue redirects HTTP) --------
_SHORT_TIKTOK_HOSTS = {"vt.tiktok.com", "vm.tiktok.com", "t.tiktok.com"}

def _resolve_short_url(u: str, timeout: int = 12) -> str:
    """Sigue redirects HTTP para resolver short URLs (vt.tiktok.com, vm.tiktok.com, etc).
    Retorna la URL final o la original si falla."""
    try:
        parsed = urlparse(u)
        host = (parsed.netloc or "").lower()
        if host not in _SHORT_TIKTOK_HOSTS:
            return u

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,*/*",
        }
        req = urllib.request.Request(u, headers=headers, method="HEAD")
        ctx = ssl.create_default_context()
        resp = urllib.request.urlopen(req, timeout=timeout, context=ctx)
        final_url = resp.url
        resp.close()
        if final_url and final_url != u:
            return final_url
        return u
    except Exception:
        # Si no puede resolver, intenta con GET (algunos servidores no soportan HEAD)
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                              "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,*/*",
            }
            req = urllib.request.Request(u, headers=headers, method="GET")
            ctx = ssl.create_default_context()
            resp = urllib.request.urlopen(req, timeout=timeout, context=ctx)
            final_url = resp.url
            resp.close()
            if final_url and final_url != u:
                return final_url
        except Exception:
            pass
        return u


def _norm_tiktok(u: str) -> str:
    p = urlparse(u)
    host = (p.netloc or "").lower()
    if host in _SHORT_TIKTOK_HOSTS:
        code = (p.path or "").strip("/").split("/")[0]
        if code:
            # Evita depender de DNS de vt/vm.tiktok.com; el dominio principal sí resuelve mejor.
            return f"https://www.tiktok.com/t/{code}/"

    # Resolver short URLs si aún llega una variante no contemplada
    if host in _SHORT_TIKTOK_HOSTS:
        resolved = _resolve_short_url(u)
        if resolved != u:
            # Re-parsear la URL resuelta
            p = urlparse(resolved)
            host = (p.netloc or "").lower()
            u = resolved
    if host == "m.tiktok.com":
        p = p._replace(netloc="www.tiktok.com")
        return urlunparse(p)
    return u


def resolve_tiktok_url_for_detection(u: str, timeout: int = 12) -> Tuple[str, Optional[str]]:
    """Resuelve enlaces cortos de TikTok (vt/vm/t) y también /t/<code>/ dentro de www.tiktok.com.
    Retorna (url_efectiva, aviso_o_error). Nunca lanza."""
    try:
        p = urlparse(u)
        host = (p.netloc or "").lower()
        path = p.path or ""
    except Exception:
        return u, None

    is_short = host in _SHORT_TIKTOK_HOSTS
    is_t_path = host.endswith("tiktok.com") and path.startswith("/t/")
    if not (is_short or is_t_path):
        return u, None

    def _follow_redirects(url: str) -> str:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,*/*",
        }
        ctx = ssl.create_default_context()
        try:
            req = urllib.request.Request(url, headers=headers, method="HEAD")
            resp = urllib.request.urlopen(req, timeout=timeout, context=ctx)
            final_url = resp.url
            resp.close()
            return final_url or url
        except Exception:
            try:
                req = urllib.request.Request(url, headers=headers, method="GET")
                resp = urllib.request.urlopen(req, timeout=timeout, context=ctx)
                final_url = resp.url
                resp.close()
                return final_url or url
            except Exception:
                return url

    if is_short:
        resolved = _resolve_short_url(u, timeout=timeout)
        if resolved and resolved != u:
            return resolved, None
        normalized = _norm_tiktok(u)
        if normalized and normalized != u:
            u = normalized
            p = urlparse(u)
            host = (p.netloc or "").lower()
            path = p.path or ""
            is_t_path = host.endswith("tiktok.com") and path.startswith("/t/")
        else:
            return u, "No se pudo resolver el enlace corto de TikTok para detectar su tipo real."

    if is_t_path:
        resolved2 = _follow_redirects(u)
        if resolved2 and resolved2 != u:
            return resolved2, None
        return u, "No se pudo resolver el enlace corto de TikTok para detectar su tipo real."

    return u, None

def _norm_instagram(u: str) -> str:
    p = urlparse(u)
    host = (p.netloc or "").lower()
    if host == "m.instagram.com":
        q = parse_qs(p.query or "")
        q.pop("igshid", None)
        p = p._replace(netloc="www.instagram.com", query=urlencode(q, doseq=True))
        return urlunparse(p)
    return u

def _normalize_url(u: str) -> str:
    if not u:
        return u
    try:
        host = urlparse(u).netloc.lower()
    except Exception:
        return u
    if "youtube.com" in host or "youtu.be" in host or "yout" in host:
        return _norm_youtube(u)
    if "tiktok.com" in host:
        return _norm_tiktok(u)
    if "instagram.com" in host:
        return _norm_instagram(u)
    if host.startswith("m."):
        p = urlparse(u)
        p = p._replace(netloc=host[2:])
        return urlunparse(p)
    return u


def _split_links(raw: str) -> List[str]:
    items: List[str] = []
    for line in (raw or "").replace(",", "\n").splitlines():
        u = (line or "").strip()
        if not u:
            continue
        items.append(u)
    normed = [_normalize_url(u) for u in items]
    seen = set()
    uniq: List[str] = []
    for u in normed:
        if u not in seen:
            uniq.append(u)
            seen.add(u)
    return uniq


def _domains_from_urls(urls: List[str]) -> List[str]:
    ds: List[str] = []
    for u in urls:
        try:
            u2 = _normalize_url(u)
            d = urlparse(u2).netloc.lower()
        except Exception:
            d = ""
        if d.startswith("www."):
            d = d[4:]
        if d in {"youtube.com", "m.youtube.com", "music.youtube.com", "youtu.be"}:
            d = "youtube"
        if d:
            ds.append(d)
    seen = set()
    uniq: List[str] = []
    for d in ds:
        if d not in seen:
            uniq.append(d)
            seen.add(d)
    return uniq


def _client_order() -> List[str]:
    # android/ios/web dan formatos progresivos más compatibles sin requerir JS extra
    return ["android", "ios", "web"]


def _is_yt_playlist_url(u: str) -> bool:
    try:
        parsed = urlparse(u)
        q = parse_qs(parsed.query)
        list_values = q.get("list") or []
        is_radio_mix = any(v.startswith("RD") for v in list_values)
        if parsed.netloc.endswith("youtube.com"):
            if parsed.path.startswith("/playlist") and "list" in q:
                return True
            if parsed.path.startswith("/watch") and "list" in q and "v" not in q and not is_radio_mix:
                return True
    except Exception:
        pass
    return False


def _is_rate_limit_error(msg: str) -> bool:
    m = msg.lower()
    return (
        "rate-limit" in m
        or "rate limited" in m
        or "try again later" in m
        or "http error 429" in m
        or "too many requests" in m
    )


def _is_dns_error(msg: str) -> bool:
    m = msg.lower()
    return (
        "failed to resolve" in m
        or "temporary failure in name resolution" in m
        or "name or service not known" in m
        or "no address associated with hostname" in m
        or "getaddrinfo failed" in m
    )


# -------------------- imagem/carroseis --------------------
def _collect_image_candidates(info: Any) -> List[Dict[str, str]]:
    candidates: List[Dict[str, str]] = []
    seen = set()

    def add_candidate(url: Optional[str], title: Optional[str], idx: int, ext: Optional[str] = None) -> None:
        if not url or url in seen:
            return
        seen.add(url)
        final_ext = (ext or _guess_ext_from_url(url, "jpg")).lower()
        if final_ext not in IMAGE_EXTS:
            final_ext = "jpg"
        candidates.append({
            "url": url,
            "title": title or "imagen",
            "ext": final_ext,
            "index": str(idx),
        })

    def walk(node: Any, title_hint: Optional[str] = None) -> None:
        if isinstance(node, list):
            for item in node:
                walk(item, title_hint)
            return

        if not isinstance(node, dict):
            return

        title = node.get("title") or title_hint or "imagen"
        entries = node.get("entries") or []
        if entries:
            for entry in entries:
                walk(entry, title)

        formats = node.get("formats") or []
        image_idx = len(candidates) + 1
        for fmt in formats:
            fmt_url = fmt.get("url")
            fmt_ext = (fmt.get("ext") or "").lower()
            vcodec = fmt.get("vcodec")
            if fmt_url and fmt_ext in IMAGE_EXTS and vcodec in (None, "none"):
                add_candidate(fmt_url, title, image_idx, fmt_ext)
                image_idx += 1

        direct_url = node.get("url")
        direct_ext = (node.get("ext") or "").lower()
        if direct_url and (direct_ext in IMAGE_EXTS or _guess_ext_from_url(direct_url, "") in IMAGE_EXTS):
            add_candidate(direct_url, title, image_idx, direct_ext or None)
            image_idx += 1

        # Fallback para posts de foto donde el extractor solo expone thumbnails.
        has_video = direct_ext in VIDEO_EXTS or any((fmt.get("vcodec") not in (None, "none")) for fmt in formats)
        if not has_video and not entries:
            thumbs = [t for t in (node.get("thumbnails") or []) if t.get("url")]
            thumbs = [
                t for t in thumbs
                if (t.get("ext") or _guess_ext_from_url(t.get("url"), "")).lower() in IMAGE_EXTS
            ]
            thumbs = sorted(
                thumbs,
                key=lambda t: ((t.get("width") or 0) * (t.get("height") or 0), t.get("preference") or 0),
                reverse=True,
            )
            if thumbs:
                best = thumbs[0]
                add_candidate(best.get("url"), title, image_idx, best.get("ext"))

    walk(info)
    return candidates


def download_images_with_ytdlp(
    urls: List[str],
    out_dir: Path,
    per_channel_folders: bool,
    cookies_path: Optional[Path],
    proxy_url: Optional[str],
    force_ipv4: Optional[bool],
    sleep_between: float,
    sleep_requests: float,
    geo_country: Optional[str],
    max_retries: int,
    selected_image_urls: Optional[List[str]] = None,
) -> Tuple[List[Path], List[Tuple[str, str]]]:
    out_dir.mkdir(parents=True, exist_ok=True)
    fails: List[Tuple[str, str]] = []
    generated: List[Path] = []
    cookiefile = str(cookies_path) if cookies_path else None

    ydl_opts: Dict[str, Any] = {
        "quiet": True,
        "noprogress": True,
        "skip_download": True,
        "no_warnings": True,
        "logger": _NullLogger(),
        "socket_timeout": 30,
        "retries": max(1, int(max_retries)),
        "extractor_retries": max(1, int(max_retries)),
        "sleep_interval": max(0.0, float(sleep_between)),
        "max_sleep_interval": max(0.0, float(sleep_between)),
        "sleep_requests": max(0.0, float(sleep_requests)),
        "max_sleep_interval_requests": max(0.0, float(sleep_requests)),
    }
    ydl_opts.update(
        _ydl_net_opts(
            proxy_url=proxy_url,
            cookies_path=cookies_path,
            force_ipv4=force_ipv4,
            set_cookiefile=False,
        )
    )
    if geo_country:
        ydl_opts["geo_bypass_country"] = geo_country.upper().strip()

    opener_handlers: List[Any] = [urllib.request.HTTPSHandler(context=ssl.create_default_context())]
    effective_proxy = ydl_opts.get("proxy")
    if isinstance(effective_proxy, str) and effective_proxy.strip():
        opener_handlers.append(
            urllib.request.ProxyHandler({"http": effective_proxy.strip(), "https": effective_proxy.strip()})
        )
    opener = urllib.request.build_opener(*opener_handlers)

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        for raw_url in urls:
            url = _normalize_url(raw_url)
            try:
                eff, _ = resolve_tiktok_url_for_detection(url)
                if eff:
                    url = eff
            except Exception:
                pass
            try:
                try:
                    p = urlparse(url)
                    host = (p.netloc or "").lower()
                    path = (p.path or "").lower()
                except Exception:
                    host, path = "", ""

                if "tiktok.com" in host and "/photo/" in path:
                    debug = (os.environ.get("MEDIA_DOWNLOADER_DEBUG_TIKTOK_PHOTO", "1") or "").strip().lower() not in (
                        "0",
                        "false",
                        "no",
                        "off",
                    )
                    def _dbg(msg: str) -> None:
                        if not debug:
                            return
                        try:
                            print(f"[TIKTOK-PHOTO] {msg}", flush=True)
                        except Exception:
                            pass

                    if url != _normalize_url(raw_url):
                        _dbg(f"Normalizado/resuelto: raw={raw_url} effective={url}")
                    else:
                        _dbg(f"Procesando: {url}")

                    candidates_from_selection = False
                    if selected_image_urls is not None:
                        chosen = [
                            str(u).strip()
                            for u in (selected_image_urls or [])
                            if str(u).strip().startswith(("http://", "https://"))
                        ]
                        candidates = []
                        for u in chosen:
                            nu = _normalize_tiktok_photo_image_url(u)
                            if not nu:
                                continue
                            candidates.append({"url": nu, "ext": _guess_ext_from_url(nu, "jpg").lower()})
                        candidates = [c for c in candidates if c.get("url")]
                        uploader_name, title_name = "tiktok", "tiktok_photo"
                        candidates_from_selection = True
                    else:
                        candidates, uploader_name, title_name = _tiktok_photo_candidates_playwright(
                            url, timeout=12, cookies_path=cookies_path
                        )
                        _dbg(f"Playwright candidates={len(candidates)}")
                        if not candidates:
                            candidates, uploader_name, title_name = _tiktok_photo_candidates(url, timeout=18)
                            _dbg(f"HTML/JSON candidates={len(candidates)}")
                    if selected_image_urls is not None and not candidates_from_selection:
                        sel = {str(u).strip() for u in (selected_image_urls or []) if str(u).strip()}
                        sel_keys = {_url_key_for_selection(u) for u in sel if u}
                        candidates = [
                            c
                            for c in candidates
                            if (str(c.get("url") or "").strip() in sel)
                            or (_url_key_for_selection(str(c.get("url") or "").strip()) in sel_keys)
                        ]
                    if not candidates:
                        video_url = _tiktok_video_url_from_photo_url(url)
                        info2 = None
                        if video_url:
                            _dbg(f"Fallback /photo/->/video/: {video_url}")
                            try:
                                info2 = ydl.extract_info(video_url, download=False)
                            except Exception:
                                info2 = None
                        candidates2 = _collect_image_candidates(info2) if isinstance(info2, dict) else []
                        _dbg(f"Fallback yt-dlp(video) image_candidates={len(candidates2)}")
                        if selected_image_urls is not None and candidates2:
                            sel = {str(u).strip() for u in (selected_image_urls or []) if str(u).strip()}
                            sel_keys = {_url_key_for_selection(u) for u in sel if u}
                            candidates2 = [
                                c
                                for c in candidates2
                                if (str(c.get("url") or "").strip() in sel)
                                or (_url_key_for_selection(str(c.get("url") or "").strip()) in sel_keys)
                            ]
                        if not candidates2:
                            fails.append(
                                (
                                    url,
                                    "TikTok de imágenes no expone la galería completa en este entorno del servidor. "
                                    "Solo se pudo obtener una vista previa. "
                                    "Para descargar la galería completa, usa modo local o cookies/navegador compatibles.",
                                )
                            )
                            _dbg("Sin imágenes reales: cae al mensaje de entorno (preview fallback en UI).")
                            continue
                        candidates, uploader_name, title_name = candidates2, (uploader_name or "tiktok"), (title_name or "tiktok_photo")

                    m_id = None
                    try:
                        m = re.search(r"/photo/(\d+)", path)
                        m_id = m.group(1) if m else None
                    except Exception:
                        m_id = None
                    uploader = _slugify(str(uploader_name or "tiktok"), maxlen=50)
                    target_dir = out_dir / uploader if per_channel_folders else out_dir
                    target_dir.mkdir(parents=True, exist_ok=True)
                    if m_id:
                        title_base = f"tiktok_{m_id}"
                    else:
                        title_base = _slugify(str(title_name or "tiktok_photo"), maxlen=80)

                    planned: List[Tuple[str, Path]] = []
                    for idx, item in enumerate(candidates, start=1):
                        iu = str(item.get("url") or "").strip()
                        if not iu:
                            continue
                        ext = str(item.get("ext") or "").lower() or _guess_ext_from_url(iu, "jpg")
                        if ext not in IMAGE_EXTS:
                            ext = "jpg"
                        suffix = f"_{idx:02d}" if len(candidates) > 1 else ""
                        file_name = f"{title_base}{suffix}.{ext}"
                        planned.append((iu, (target_dir / file_name).resolve()))
                    _dbg(f"Plan de descarga: {len(planned)} archivos")

                    wrote_any = False
                    try:
                        from playwright.sync_api import sync_playwright  # type: ignore

                        with sync_playwright() as p:
                            browser = p.chromium.launch(headless=True)
                            context = browser.new_context(
                                user_agent=(os.environ.get("MEDIA_DOWNLOADER_UA") or _DEFAULT_UA).strip(),
                                locale="en-US",
                                viewport={"width": 1200, "height": 900},
                            )
                            if cookies_path and cookies_path.exists() and cookies_path.is_file():
                                try:
                                    cookies: List[Dict[str, Any]] = []
                                    tiktok_cookie_lines = 0
                                    for line in cookies_path.read_text(encoding="utf-8", errors="ignore").splitlines():
                                        t = (line or "").strip()
                                        if not t or t.startswith("#"):
                                            continue
                                        parts = t.split("\t")
                                        if len(parts) < 7:
                                            continue
                                        domain, _flag, path0, secure, expires, name, value = parts[:7]
                                        domain = (domain or "").strip()
                                        if "tiktok.com" not in domain.lower():
                                            continue
                                        tiktok_cookie_lines += 1
                                        path0 = (path0 or "/").strip() or "/"
                                        secure_bool = str(secure or "").strip().upper() == "TRUE"
                                        try:
                                            exp = int(float(expires))
                                        except Exception:
                                            exp = -1
                                        ck: Dict[str, Any] = {
                                            "name": str(name or ""),
                                            "value": str(value or ""),
                                            "domain": domain,
                                            "path": path0,
                                            "secure": secure_bool,
                                        }
                                        if exp > 0:
                                            ck["expires"] = exp
                                        cookies.append(ck)
                                    if cookies:
                                        context.add_cookies(cookies)
                                    _dbg(f"Cookies Playwright(download): tiktok_lines={tiktok_cookie_lines} added={len(cookies)}")
                                except Exception:
                                    _dbg("Cookies Playwright(download): fallo al cargar/aplicar cookies (silenciado).")
                                    pass

                            _dbg(f"Descarga Playwright: intentando {len(planned)} imágenes")
                            for iu, dest in planned:
                                try:
                                    resp = context.request.get(
                                        iu,
                                        headers={
                                            "Referer": url,
                                            "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
                                        },
                                        timeout=30000,
                                    )
                                    if not resp.ok:
                                        fails.append((iu, f"HTTP {resp.status}"))
                                        _dbg(f"PW GET fail: {resp.status} url={iu}")
                                        continue
                                    body = resp.body()
                                    if not body:
                                        fails.append((iu, "Contenido vacío."))
                                        _dbg(f"PW GET vacío: url={iu}")
                                        continue
                                    dest.parent.mkdir(parents=True, exist_ok=True)
                                    with open(dest, "wb") as fh:
                                        fh.write(body)
                                        fh.flush()
                                    generated.append(dest.resolve())
                                    wrote_any = True
                                    time.sleep(float(sleep_between))
                                except Exception as e:
                                    fails.append((iu, f"No se pudo descargar la imagen: {type(e).__name__}"))
                                    _dbg(f"PW GET excepción: {type(e).__name__} url={iu}")
                            try:
                                context.close()
                            except Exception:
                                pass
                            try:
                                browser.close()
                            except Exception:
                                pass
                    except Exception:
                        wrote_any = False
                        _dbg("Descarga Playwright: falló inicialización/ejecución (silenciado), pasa a urllib fallback.")

                    if not wrote_any:
                        for iu, dest in planned:
                            req = urllib.request.Request(
                                iu,
                                headers={
                                    "User-Agent": ydl_opts["http_headers"]["User-Agent"],
                                    "Referer": url,
                                    "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
                                },
                            )
                            with opener.open(req, timeout=30) as resp, open(dest, "wb") as fh:
                                shutil.copyfileobj(resp, fh)
                            generated.append(dest.resolve())
                            time.sleep(float(sleep_between))
                    continue

                if "instagram.com" in host and "/p/" in path and "/reel/" not in path:
                    candidates: List[Dict[str, str]] = []
                    def _ig_key(u: str) -> str:
                        s = str(u or "").strip()
                        if not s:
                            return ""
                        try:
                            p0 = urlparse(s)
                            name0 = (p0.path or "").rsplit("/", 1)[-1]
                            return name0.lower()
                        except Exception:
                            return s.split("?", 1)[0].rsplit("/", 1)[-1].lower()

                    if selected_image_urls is not None:
                        for u in (selected_image_urls or []):
                            su = str(u or "").strip()
                            if su.startswith(("http://", "https://")):
                                ext = _guess_ext_from_url(su, "jpg").lower()
                                if ext not in IMAGE_EXTS:
                                    ext = "jpg"
                                candidates.append({"url": su, "ext": ext})
                        try:
                            ig_pw_best = _instagram_post_probe_playwright(url, timeout=22, cookies_path=cookies_path)
                        except Exception:
                            ig_pw_best = {}
                        best_map: Dict[str, str] = {}
                        if isinstance(ig_pw_best, dict):
                            for iu in (ig_pw_best.get("images") or []):
                                su = str(iu or "").strip()
                                k0 = _ig_key(su)
                                if k0 and su:
                                    best_map[k0] = su
                        if best_map and candidates:
                            for c in candidates:
                                cu = str(c.get("url") or "").strip()
                                k0 = _ig_key(cu)
                                better = best_map.get(k0)
                                if better and better != cu:
                                    c["url"] = better
                                    c["ext"] = _guess_ext_from_url(better, str(c.get("ext") or "jpg")).lower()
                    if not candidates:
                        ig_pw = _instagram_post_probe_playwright(url, timeout=22, cookies_path=cookies_path)
                        if str(ig_pw.get("media_type") or "").lower() in ("image", "gallery"):
                            for iu in (ig_pw.get("images") or []):
                                su = str(iu or "").strip()
                                if not su:
                                    continue
                                ext = _guess_ext_from_url(su, "jpg").lower()
                                if ext not in IMAGE_EXTS:
                                    ext = "jpg"
                                candidates.append({"url": su, "ext": ext})
                    if candidates:
                        if selected_image_urls is not None:
                            sel = {str(u).strip() for u in (selected_image_urls or []) if str(u).strip()}
                            sel_keys = {_url_key_for_selection(u) for u in sel if u}
                            candidates = [
                                c
                                for c in candidates
                                if (str(c.get("url") or "").strip() in sel)
                                or (_url_key_for_selection(str(c.get("url") or "").strip()) in sel_keys)
                            ]
                        if candidates:
                            target_dir = out_dir / "instagram" if per_channel_folders else out_dir
                            target_dir.mkdir(parents=True, exist_ok=True)
                            sc = _instagram_shortcode(url) or "post"
                            title_base = f"instagram_{_slugify(sc, maxlen=30)}"
                            cookie_header = _cookie_header_for_domain(cookies_path, "instagram.com")
                            for idx, item in enumerate(candidates, start=1):
                                iu = str(item.get("url") or "").strip()
                                ext = str(item.get("ext") or "jpg").lower()
                                suffix = f"_{idx:02d}" if len(candidates) > 1 else ""
                                dest = (target_dir / f"{title_base}{suffix}.{ext}").resolve()
                                hdrs = {
                                    "User-Agent": ydl_opts["http_headers"]["User-Agent"],
                                    "Referer": url,
                                    "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
                                }
                                if cookie_header:
                                    hdrs["Cookie"] = cookie_header
                                req = urllib.request.Request(iu, headers=hdrs)
                                with opener.open(req, timeout=30) as resp, open(dest, "wb") as fh:
                                    shutil.copyfileobj(resp, fh)
                                generated.append(dest.resolve())
                                time.sleep(float(sleep_between))
                            continue

                try:
                    info = ydl.extract_info(url, download=False)
                except DownloadError as e:
                    if _is_auth_error(str(e)):
                        if cookiefile and not ydl.params.get("cookiefile"):
                            ydl.params["cookiefile"] = cookiefile
                            info = ydl.extract_info(url, download=False)
                        else:
                            try:
                                host2 = (urlparse(url).netloc or "").lower()
                            except Exception:
                                host2 = ""
                            if "instagram.com" in host2:
                                if not cookiefile:
                                    fails.append((url, "Instagram requiere autenticación adicional y no se encontraron cookies válidas."))
                                else:
                                    fails.append((url, "No se pudo acceder al contenido incluso usando cookies."))
                                continue
                            raise
                    else:
                        raise
                candidates = _collect_image_candidates(info)
                if selected_image_urls is not None:
                    sel = {str(u).strip() for u in (selected_image_urls or []) if str(u).strip()}
                    sel_keys = {_url_key_for_selection(u) for u in sel if u}
                    candidates = [
                        c
                        for c in candidates
                        if (str(c.get("url") or "").strip() in sel)
                        or (_url_key_for_selection(str(c.get("url") or "").strip()) in sel_keys)
                    ]
                if not candidates:
                    fails.append((url, "La plataforma no expuso imágenes descargables para este enlace."))
                    continue

                uploader = _slugify(str(info.get("uploader") or info.get("channel") or "media"), maxlen=50)
                target_dir = out_dir / uploader if per_channel_folders else out_dir
                target_dir.mkdir(parents=True, exist_ok=True)
                title_base = _slugify(str(info.get("title") or "imagen"), maxlen=80)

                for idx, item in enumerate(candidates, start=1):
                    ext = item["ext"]
                    suffix = f"_{idx:02d}" if len(candidates) > 1 else ""
                    file_name = f"{title_base}{suffix}.{ext}"
                    dest = target_dir / file_name
                    req = urllib.request.Request(
                        item["url"],
                        headers={
                            "User-Agent": ydl_opts["http_headers"]["User-Agent"],
                            "Referer": url,
                            "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
                        },
                    )
                    with opener.open(req, timeout=30) as resp, open(dest, "wb") as fh:
                        shutil.copyfileobj(resp, fh)
                    generated.append(dest.resolve())
                    time.sleep(float(sleep_between))
            except Exception as e:
                msg = str(e or "")
                low = msg.lower()
                if "impersonation" in low or "no impersonate target" in low:
                    fails.append(
                        (url, "TikTok requiere compatibilidad adicional del extractor en este entorno.")
                    )
                elif "unsupported url" in low:
                    fails.append((url, "Este tipo de enlace aún no está soportado por el descargador actual."))
                elif _is_dns_error(msg) or "timed out" in low or "http error 403" in low or "http error 429" in low:
                    fails.append(
                        (url, "No se pudo conectar con la plataforma. Verifica internet, VPN o acceso a la red social.")
                    )
                else:
                    fails.append((url, "No se pudo procesar el enlace. Verifica la URL y el acceso a la plataforma."))

    return generated, fails


def _tiktok_photo_candidates(url: str, timeout: int = 18) -> Tuple[List[Dict[str, str]], Optional[str], Optional[str]]:
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,*/*",
        "Accept-Language": "en-US,en;q=0.9",
    }
    ctx = ssl.create_default_context()
    req = urllib.request.Request(url, headers=headers, method="GET")
    resp = urllib.request.urlopen(req, timeout=timeout, context=ctx)
    raw = resp.read()
    resp.close()
    html = raw.decode("utf-8", errors="ignore")

    blob = None
    m = re.search(
        r'id="__UNIVERSAL_DATA_FOR_REHYDRATION__"[^>]*>\s*(.*?)\s*</script>',
        html,
        re.DOTALL,
    )
    if m:
        blob = m.group(1)
    if not blob:
        m2 = re.search(r'id="SIGI_STATE"[^>]*>\s*(.*?)\s*</script>', html, re.DOTALL)
        if m2:
            blob = m2.group(1)

    if not blob:
        return [], None, None

    try:
        data = json.loads(blob)
    except Exception:
        return [], None, None

    def _get(d: Any, keys: List[str]) -> Any:
        cur = d
        for k in keys:
            if not isinstance(cur, dict):
                return None
            cur = cur.get(k)
        return cur

    item_struct = (
        _get(data, ["__DEFAULT_SCOPE__", "webapp.video-detail", "itemInfo", "itemStruct"])
        or _get(data, ["__DEFAULT_SCOPE__", "webapp.video-detail", "itemInfo", "itemStruct"])
        or None
    )
    if not isinstance(item_struct, dict):
        item_struct = None

    uploader = None
    title = None
    images_node = None

    if item_struct:
        author = item_struct.get("author") if isinstance(item_struct.get("author"), dict) else None
        uploader = (author or {}).get("uniqueId") or (author or {}).get("nickname")
        title = item_struct.get("desc") or item_struct.get("id")
        image_post = item_struct.get("imagePost") if isinstance(item_struct.get("imagePost"), dict) else None
        images_node = (image_post or {}).get("images")

    if images_node is None:
        try:
            scope = _get(data, ["__DEFAULT_SCOPE__", "webapp.video-detail"])
            if isinstance(scope, dict):
                item_info = scope.get("itemInfo") if isinstance(scope.get("itemInfo"), dict) else None
                if isinstance(item_info, dict):
                    item_struct = item_info.get("itemStruct") if isinstance(item_info.get("itemStruct"), dict) else None
                    if isinstance(item_struct, dict):
                        image_post = item_struct.get("imagePost") if isinstance(item_struct.get("imagePost"), dict) else None
                        images_node = (image_post or {}).get("images")
        except Exception:
            pass

    out: List[Dict[str, str]] = []
    if isinstance(images_node, list):
        for img in images_node:
            if not isinstance(img, dict):
                continue
            disp = img.get("displayImage") if isinstance(img.get("displayImage"), dict) else None
            url_list = (disp or {}).get("urlList")
            u = None
            if isinstance(url_list, list) and url_list:
                u = str(url_list[0] or "").strip()
            if not u:
                u = str(img.get("url") or "").strip()
            if not u:
                continue
            ext = _guess_ext_from_url(u, "jpg").lower()
            if ext not in IMAGE_EXTS:
                ext = "jpg"
            out.append({"url": u, "ext": ext})

    return out, (str(uploader) if uploader else None), (str(title) if title else None)


def _tiktok_photo_og_image(url: str, timeout: int = 15) -> Optional[str]:
    headers = {
        "User-Agent": (os.environ.get("MEDIA_DOWNLOADER_UA") or _DEFAULT_UA).strip(),
        "Accept": "text/html,application/xhtml+xml,*/*",
        "Accept-Language": "en-US,en;q=0.9",
    }
    ctx = ssl.create_default_context()
    try:
        req = urllib.request.Request(url, headers=headers, method="GET")
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as resp:
            raw = resp.read()
        html = raw.decode("utf-8", errors="ignore")
    except Exception:
        return None
    m = re.search(r'<meta\s+property="og:image"\s+content="([^"]+)"', html, re.IGNORECASE)
    if not m:
        m = re.search(r"<meta\s+property='og:image'\s+content='([^']+)'", html, re.IGNORECASE)
    if not m:
        return None
    u = str(m.group(1) or "").strip()
    if not u.startswith(("http://", "https://")):
        return None
    if not re.search(r"\.(jpe?g|png|webp)(\?|#|$)", u, re.IGNORECASE):
        return None
    return u


def _tiktok_photo_preview_from_html(url: str, timeout: int = 15) -> Optional[str]:
    headers = {
        "User-Agent": (os.environ.get("MEDIA_DOWNLOADER_UA") or _DEFAULT_UA).strip(),
        "Accept": "text/html,application/xhtml+xml,*/*",
        "Accept-Language": "en-US,en;q=0.9",
    }
    ctx = ssl.create_default_context()
    try:
        req = urllib.request.Request(url, headers=headers, method="GET")
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as resp:
            raw = resp.read()
        html = raw.decode("utf-8", errors="ignore")
    except Exception:
        return None

    candidates = re.findall(
        r"https://[^\s\"']+tiktokcdn\.com[^\s\"']+photomode[^\s\"']+\.(?:jpe?g|png|webp)[^\s\"']*",
        html,
        re.IGNORECASE,
    )
    if not candidates:
        candidates = re.findall(
            r"https://[^\s\"']+tiktokcdn\.com[^\s\"']+\.(?:jpe?g|png|webp)[^\s\"']*",
            html,
            re.IGNORECASE,
        )
    if not candidates:
        return None
    u = str(candidates[0] or "").strip()
    u = u.replace("\\u002F", "/").replace("\\/", "/").replace("&amp;", "&")
    if not u.startswith(("http://", "https://")):
        return None
    if not re.search(r"\.(jpe?g|png|webp)(\?|#|$)", u, re.IGNORECASE):
        return None
    return u


def _tiktok_photo_candidates_playwright(
    url: str,
    timeout: int = 25,
    cookies_path: Optional[Path] = None,
) -> Tuple[List[Dict[str, str]], Optional[str], Optional[str]]:
    try:
        from playwright.sync_api import sync_playwright  # type: ignore
    except Exception:
        return [], None, None

    debug = (os.environ.get("MEDIA_DOWNLOADER_DEBUG_TIKTOK_PHOTO", "1") or "").strip().lower() not in (
        "0",
        "false",
        "no",
        "off",
    )
    try:
        max_steps = int(os.environ.get("MEDIA_DOWNLOADER_TIKTOK_PHOTO_PW_MAX_STEPS", "6") or "6")
    except Exception:
        max_steps = 6
    if max_steps < 0:
        max_steps = 0

    def _dbg(msg: str) -> None:
        if not debug:
            return
        try:
            print(f"[TIKTOK-PHOTO-PW] {msg}", flush=True)
        except Exception:
            pass

    def _is_img_url(u: str) -> bool:
        s = str(u or "").strip()
        if not s:
            return False
        if not (s.startswith("http://") or s.startswith("https://")):
            return False
        low = s.lower()
        if re.search(r"\.(jpe?g|png|webp)(\?|#|$)", s, re.IGNORECASE):
            return True
        if "tiktokcdn.com" not in low:
            return False
        if "tplv-photomode-image" in low or "photomode" in low:
            return True
        if "mime_type=image" in low or "mime_type=image%2f" in low:
            return True
        return False

    def _clean_url(u: str) -> str:
        s = str(u or "").strip()
        if not s:
            return ""
        return s.replace("\\u002F", "/").replace("\\/", "/").replace("&amp;", "&")

    def _url_key(u: str) -> str:
        s = str(u or "").strip()
        if not s:
            return ""
        try:
            p = urlparse(s)
            host = (p.netloc or "").lower()
            path = p.path or ""
            if host and path:
                return f"{host}{path}"
        except Exception:
            pass
        return s.split("?", 1)[0].strip()

    def _score_img_url(u: str) -> int:
        s = str(u or "").strip()
        if not s:
            return -1
        low = s.lower()
        deny = (
            "cropcenter",
            "tiktokx-cropcenter",
            "tos-alisg-avt",
            "tos-alisg-i-avt",
            "/avt-",
            "avatar",
            "emoji",
            "sticker",
            "icon",
            "favicon",
            "twemoji",
            "sprite",
            "~noop",
            "musically-maliva-obj",
        )
        if any(x in low for x in deny):
            return -1
        score = 0
        if "tplv-photomode-image" in low:
            score += 100
        if "photomode" in low:
            score += 40
        if "tiktokcdn" in low:
            score += 10
        if "mime_type=image" in low or "mime_type=image%2f" in low:
            score += 5
        if low.endswith(".jpeg") or low.endswith(".jpg"):
            score += 2
        return score

    def _from_blob(blob_text: str) -> List[str]:
        try:
            data = json.loads(blob_text)
        except Exception:
            candidates = re.findall(r"https://[^\s\"']+tiktokcdn\.com[^\s\"']+", str(blob_text or ""), re.IGNORECASE)
            return [u for u in candidates if isinstance(u, str) and _is_img_url(u)]

        out_urls: List[str] = []

        def add_any(v: Any) -> None:
            if isinstance(v, str):
                out_urls.append(v)
                return
            if isinstance(v, list):
                for x in v:
                    add_any(x)
                return
            if isinstance(v, dict):
                for x in v.values():
                    add_any(x)
                return

        def walk(node: Any) -> None:
            if isinstance(node, dict):
                ip = node.get("imagePost")
                if isinstance(ip, dict):
                    imgs = ip.get("images")
                    if isinstance(imgs, list):
                        for im in imgs:
                            if isinstance(im, dict):
                                add_any(im.get("imageURL"))
                                add_any(im.get("displayImage"))
                                add_any(im.get("imageUrl"))
                                add_any(im.get("urls"))
                                add_any(im.get("url"))
                            else:
                                add_any(im)
                for v in node.values():
                    walk(v)
            elif isinstance(node, list):
                for v in node:
                    walk(v)

        walk(data)
        filtered = [u for u in out_urls if isinstance(u, str) and _is_img_url(u)]
        if filtered:
            return filtered
        candidates = re.findall(r"https://[^\s\"']+tiktokcdn\.com[^\s\"']+", str(blob_text or ""), re.IGNORECASE)
        return [u for u in candidates if isinstance(u, str) and _is_img_url(u)]

    def _from_state_obj(state: Any) -> List[str]:
        out_urls: List[str] = []

        def add_any(v: Any) -> None:
            if isinstance(v, str):
                out_urls.append(v)
                return
            if isinstance(v, list):
                for x in v:
                    add_any(x)
                return
            if isinstance(v, dict):
                for x in v.values():
                    add_any(x)
                return

        def walk(node: Any) -> None:
            if isinstance(node, dict):
                item_module = node.get("ItemModule")
                if isinstance(item_module, dict):
                    for v in item_module.values():
                        walk(v)

                ip = node.get("imagePost")
                if isinstance(ip, dict):
                    imgs = ip.get("images") or ip.get("imageList")
                    if isinstance(imgs, list):
                        for im in imgs:
                            if isinstance(im, dict):
                                add_any(im.get("imageURL"))
                                add_any(im.get("displayImage"))
                                add_any(im.get("imageUrl"))
                                add_any(im.get("urls"))
                                add_any(im.get("url"))
                                add_any(im.get("urlList"))
                            else:
                                add_any(im)

                images = node.get("images")
                if isinstance(images, list):
                    for im in images:
                        if isinstance(im, dict):
                            add_any(im.get("imageURL"))
                            add_any(im.get("displayImage"))
                            add_any(im.get("imageUrl"))
                            add_any(im.get("urls"))
                            add_any(im.get("url"))
                            add_any(im.get("urlList"))
                        else:
                            add_any(im)

                for v in node.values():
                    walk(v)
            elif isinstance(node, list):
                for v in node:
                    walk(v)

        walk(state)
        return [u for u in out_urls if isinstance(u, str) and _is_img_url(u)]

    preferred_item_id = None
    try:
        m0 = re.search(r"/photo/(\d+)", str(url or ""))
        preferred_item_id = m0.group(1) if m0 else None
    except Exception:
        preferred_item_id = None

    def _pick_best_from_url_list(urls: Any) -> Optional[str]:
        if not isinstance(urls, list):
            return None
        best_u: Optional[str] = None
        best_score = -10_000
        for u in urls:
            s = str(u or "").strip()
            if not s:
                continue
            s = _normalize_tiktok_photo_image_url(_clean_url(s))
            if not _is_img_url(s):
                continue
            score = _score_img_url(s)
            if score > best_score:
                best_score = score
                best_u = s
        return best_u

    def _extract_images_from_sigi_state(sigi: Dict[str, Any]) -> Tuple[List[str], Optional[str]]:
        item_module = sigi.get("ItemModule")
        if not isinstance(item_module, dict) or not item_module:
            return [], None

        chosen_id: Optional[str] = None
        chosen_item: Optional[Dict[str, Any]] = None

        if preferred_item_id and preferred_item_id in item_module:
            it = item_module.get(preferred_item_id)
            if isinstance(it, dict):
                chosen_id, chosen_item = preferred_item_id, it

        if chosen_item is None:
            for k, v in item_module.items():
                if not isinstance(k, str) or not isinstance(v, dict):
                    continue
                ip = v.get("imagePost")
                if isinstance(ip, dict) and isinstance(ip.get("images"), list) and ip.get("images"):
                    chosen_id, chosen_item = k, v
                    break

        if chosen_item is None:
            return [], None

        ip = chosen_item.get("imagePost")
        if not isinstance(ip, dict):
            return [], chosen_id

        imgs = ip.get("images") or ip.get("imageList")
        if not isinstance(imgs, list) or not imgs:
            return [], chosen_id

        out: List[str] = []
        for im in imgs:
            best: Optional[str] = None
            if isinstance(im, dict):
                best = _pick_best_from_url_list(im.get("urlList"))
                if not best:
                    img_url = im.get("imageURL")
                    if isinstance(img_url, dict):
                        best = _pick_best_from_url_list(img_url.get("urlList"))
                if not best:
                    disp = im.get("displayImage")
                    if isinstance(disp, dict):
                        best = _pick_best_from_url_list(disp.get("urlList"))
                if not best:
                    best = _pick_best_from_url_list(im.get("urls"))
            elif isinstance(im, list):
                best = _pick_best_from_url_list(im)
            elif isinstance(im, str):
                s = _normalize_tiktok_photo_image_url(_clean_url(im))
                if _is_img_url(s) and _score_img_url(s) >= 0:
                    best = s
            if best:
                out.append(best)

        return out, chosen_id

    def _extract_images_from_universal_data(universal: Dict[str, Any]) -> Tuple[List[str], bool]:
        try:
            default_scope = universal.get("__DEFAULT_SCOPE__")
        except Exception:
            default_scope = None
        if not isinstance(default_scope, dict) or not default_scope:
            return [], False

        video_detail = default_scope.get("webapp.video-detail")
        if not isinstance(video_detail, dict):
            for k, v in default_scope.items():
                if isinstance(k, str) and k.startswith("webapp.video-detail") and isinstance(v, dict):
                    video_detail = v
                    break
        if not isinstance(video_detail, dict) or not video_detail:
            return [], False

        item_info = video_detail.get("itemInfo")
        if not isinstance(item_info, dict):
            return [], False

        item_struct = item_info.get("itemStruct")
        if not isinstance(item_struct, dict) or not item_struct:
            return [], False

        image_post = item_struct.get("imagePost")
        if not isinstance(image_post, dict) or not image_post:
            return [], True

        imgs = image_post.get("images") or image_post.get("imageList")
        if not isinstance(imgs, list) or not imgs:
            return [], True

        out: List[str] = []
        for im in imgs:
            best: Optional[str] = None
            if isinstance(im, dict):
                best = _pick_best_from_url_list(im.get("urlList"))
                if not best:
                    img_url = im.get("imageURL")
                    if isinstance(img_url, dict):
                        best = _pick_best_from_url_list(img_url.get("urlList"))
                if not best:
                    disp = im.get("displayImage")
                    if isinstance(disp, dict):
                        best = _pick_best_from_url_list(disp.get("urlList"))
                if not best:
                    best = _pick_best_from_url_list(im.get("urls"))
                if not best:
                    s = _normalize_tiktok_photo_image_url(_clean_url(str(im.get("url") or "")))
                    if _is_img_url(s) and _score_img_url(s) >= 0:
                        best = s
            elif isinstance(im, list):
                best = _pick_best_from_url_list(im)
            elif isinstance(im, str):
                s = _normalize_tiktok_photo_image_url(_clean_url(im))
                if _is_img_url(s) and _score_img_url(s) >= 0:
                    best = s
            if best:
                out.append(best)

        return out, True

    seen_keys: set[str] = set()
    photomode: List[str] = []
    other: List[str] = []
    stats: Dict[str, int] = {"net": 0, "dom": 0, "blob": 0, "next": 0, "json": 0}

    def _add(u: str, *, w: Optional[int] = None, h: Optional[int] = None, from_network: bool = False) -> None:
        s0 = _clean_url(u)
        if not _is_img_url(s0):
            return
        s = _normalize_tiktok_photo_image_url(s0)
        score = _score_img_url(s)
        if score < 0:
            return
        if w is not None and h is not None:
            try:
                iw, ih = int(w), int(h)
                if iw > 0 and ih > 0 and (iw < 220 or ih < 220):
                    return
            except Exception:
                pass

        k = _url_key(s)
        if not k or k in seen_keys:
            return
        seen_keys.add(k)

        if score >= 80:
            photomode.append(s)
        else:
            other.append(s)
        if from_network:
            stats["net"] = int(stats.get("net") or 0) + 1

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent=(os.environ.get("MEDIA_DOWNLOADER_UA") or _DEFAULT_UA).strip(),
                locale="en-US",
                viewport={"width": 1200, "height": 900},
            )

            usable, size = _cookies_file_usable(cookies_path)
            _dbg(f"Entrando. url={url} timeout={timeout}s cookies_usable={usable} cookie_size={size if size is not None else 'NA'}")

            if cookies_path and cookies_path.exists() and cookies_path.is_file():
                try:
                    cookies: List[Dict[str, Any]] = []
                    tiktok_cookie_lines = 0
                    for line in cookies_path.read_text(encoding="utf-8", errors="ignore").splitlines():
                        t = (line or "").strip()
                        if not t or t.startswith("#"):
                            continue
                        parts = t.split("\t")
                        if len(parts) < 7:
                            continue
                        domain, _flag, path, secure, expires, name, value = parts[:7]
                        domain = (domain or "").strip()
                        if "tiktok.com" not in domain.lower():
                            continue
                        tiktok_cookie_lines += 1
                        path = (path or "/").strip() or "/"
                        secure_bool = str(secure or "").strip().upper() == "TRUE"
                        try:
                            exp = int(float(expires))
                        except Exception:
                            exp = -1
                        ck: Dict[str, Any] = {
                            "name": str(name or ""),
                            "value": str(value or ""),
                            "domain": domain,
                            "path": path,
                            "secure": secure_bool,
                        }
                        if exp > 0:
                            ck["expires"] = exp
                        cookies.append(ck)
                    if cookies:
                        context.add_cookies(cookies)
                    _dbg(f"Cookies Playwright: tiktok_lines={tiktok_cookie_lines} added={len(cookies)}")
                except Exception:
                    _dbg("Cookies Playwright: fallo al cargar/aplicar cookies (silenciado).")
                    pass

            page = context.new_page()

            def _scan_dom(tag: str) -> None:
                try:
                    items = page.evaluate(
                        """() => {
                          const out = []
                          const imgs = Array.from(document.images || [])
                          for (const i of imgs) {
                            const src = (i.currentSrc || i.src || i.getAttribute('src') || '').trim()
                            if (!src) continue
                            const w = (i.naturalWidth || 0)
                            const h = (i.naturalHeight || 0)
                            out.push({src, w, h})
                          }
                          return out
                        }"""
                    )
                    cnt = 0
                    if isinstance(items, list):
                        for it in items:
                            if isinstance(it, dict):
                                _add(str(it.get("src") or ""), w=it.get("w"), h=it.get("h"))
                                cnt += 1
                            else:
                                _add(str(it))
                                cnt += 1
                    stats["dom"] = int(stats.get("dom") or 0) + cnt
                    _dbg(f"DOM scan({tag}): images={cnt} photomode={len(photomode)} other={len(other)} seen={len(seen_keys)}")
                except Exception:
                    _dbg(f"DOM scan({tag}): error")

            def _scan_visible_big(tag: str) -> None:
                try:
                    items = page.evaluate(
                        """() => {
                          const out = []
                          const vw = window.innerWidth || 0
                          const vh = window.innerHeight || 0
                          const els = Array.from(document.images || [])
                          for (const i of els) {
                            const src = (i.currentSrc || i.src || i.getAttribute('src') || '').trim()
                            if (!src) continue
                            const r = i.getBoundingClientRect()
                            const w = Math.round(r.width || 0)
                            const h = Math.round(r.height || 0)
                            const vis = (r.bottom > 0 && r.right > 0 && r.top < vh && r.left < vw)
                            if (!vis) continue
                            if (w < 220 || h < 220) continue
                            out.push({src, w, h})
                          }
                          return out
                        }"""
                    )
                    cnt = 0
                    if isinstance(items, list):
                        for it in items:
                            if isinstance(it, dict):
                                _add(str(it.get("src") or ""), w=it.get("w"), h=it.get("h"))
                                cnt += 1
                            else:
                                _add(str(it))
                                cnt += 1
                    _dbg(f"Visible scan({tag}): big_images={cnt} photomode={len(photomode)} other={len(other)} seen={len(seen_keys)}")
                except Exception:
                    _dbg(f"Visible scan({tag}): error")

            def _scan_scripts_for_urls(tag: str) -> None:
                try:
                    urls = page.evaluate(
                        """() => {
                          const out = []
                          const scripts = Array.from(document.scripts || [])
                          for (const s of scripts) {
                            const t = (s.textContent || '')
                            if (!t) continue
                            if (t.indexOf('tiktokcdn') === -1 && t.indexOf('photomode') === -1 && t.indexOf('imagePost') === -1) continue
                            out.push(t.slice(0, 200000))
                          }
                          return out
                        }"""
                    )
                    blobs = 0
                    hits = 0
                    if isinstance(urls, list):
                        for t in urls:
                            blobs += 1
                            if isinstance(t, str) and t:
                                found = re.findall(r"https://[^\s\"']+tiktokcdn\.com[^\s\"']+", t, re.IGNORECASE)
                                for u in found:
                                    _add(str(u))
                                    hits += 1
                    _dbg(f"Scripts scan({tag}): script_blobs={blobs} url_hits={hits} photomode={len(photomode)} other={len(other)} seen={len(seen_keys)}")
                except Exception:
                    _dbg(f"Scripts scan({tag}): error")

            def _scan_inline_bg(tag: str) -> None:
                try:
                    urls = page.evaluate(
                        """() => {
                          const out = []
                          const els = Array.from(document.querySelectorAll('[style*="background"]') || [])
                          for (const el of els) {
                            const st = el.getAttribute('style') || ''
                            if (!st) continue
                            if (st.indexOf('tiktokcdn') === -1) continue
                            out.push(st)
                          }
                          return out
                        }"""
                    )
                    hits = 0
                    if isinstance(urls, list):
                        for st in urls:
                            if not isinstance(st, str):
                                continue
                            found = re.findall(r"https://[^\s\"')]+tiktokcdn\.com[^\s\"')]+", st, re.IGNORECASE)
                            for u in found:
                                _add(str(u))
                                hits += 1
                    _dbg(f"BG scan({tag}): url_hits={hits} photomode={len(photomode)} other={len(other)} seen={len(seen_keys)}")
                except Exception:
                    _dbg(f"BG scan({tag}): error")

            def on_response(resp) -> None:
                try:
                    req = resp.request
                    h = resp.headers or {}
                    ct = str(h.get("content-type") or "").lower()
                    u2 = str(resp.url or "")
                    is_img = False
                    if req and str(getattr(req, "resource_type", "") or "") == "image":
                        is_img = True
                    if ct.startswith("image/"):
                        is_img = True
                    if "tiktokcdn" in u2.lower() and _is_img_url(u2):
                        is_img = True
                    if not is_img:
                        return None
                    cl = str(h.get("content-length") or "").strip()
                    if cl.isdigit():
                        try:
                            if int(cl) < 12000:
                                return None
                        except Exception:
                            pass
                    _add(u2, from_network=True)
                except Exception:
                    return None

            page.on("response", on_response)

            page.goto(url, wait_until="domcontentloaded", timeout=max(1, int(timeout)) * 1000)
            try:
                page.wait_for_load_state("networkidle", timeout=max(1, int(timeout)) * 1000)
            except Exception:
                pass
            try:
                _dbg(f"Página cargada. final_url={page.url}")
            except Exception:
                pass
            try:
                page.wait_for_timeout(1200)
            except Exception:
                pass

            try:
                og = page.locator('meta[property="og:image"]').first.get_attribute("content")
                if og:
                    _add(og)
            except Exception:
                pass

            _scan_dom("after-load")
            _scan_visible_big("after-load")
            _scan_inline_bg("after-load")

            def extract_blobs() -> None:
                try:
                    blob1 = page.eval_on_selector(
                        "#__UNIVERSAL_DATA_FOR_REHYDRATION__", "el => el.textContent || ''"
                    )
                    if isinstance(blob1, str) and blob1.strip():
                        stats["blob"] = int(stats.get("blob") or 0) + 1
                        urls1 = _from_blob(blob1)
                        _dbg(f"Blob __UNIVERSAL__: urls={len(urls1)}")
                        for u in urls1:
                            _add(u)
                except Exception:
                    pass
                try:
                    blob2 = page.eval_on_selector("#SIGI_STATE", "el => el.textContent || ''")
                    if isinstance(blob2, str) and blob2.strip():
                        stats["blob"] = int(stats.get("blob") or 0) + 1
                        urls2 = _from_blob(blob2)
                        _dbg(f"Blob SIGI_STATE: urls={len(urls2)}")
                        for u in urls2:
                            _add(u)
                except Exception:
                    pass

            def extract_window_state() -> None:
                uni_obj: Optional[Dict[str, Any]] = None
                try:
                    uni_raw = page.evaluate(
                        "() => { try { return JSON.stringify(window.__UNIVERSAL_DATA_FOR_REHYDRATION__ || null) } catch(e) { return null } }"
                    )
                    if isinstance(uni_raw, str) and uni_raw and uni_raw != "null":
                        try:
                            uni_obj = json.loads(uni_raw)
                            _dbg(f"UNIVERSAL_DATA encontrado. json_len={len(uni_raw)}")
                        except Exception:
                            uni_obj = None
                            _dbg("UNIVERSAL_DATA encontrado, pero no se pudo parsear JSON.stringify.")
                except Exception:
                    _dbg("UNIVERSAL_DATA: error al leer.")

                if isinstance(uni_obj, dict) and uni_obj:
                    urls_u, item_struct_found = _extract_images_from_universal_data(uni_obj)
                    stats["json"] = int(stats.get("json") or 0) + 1
                    _dbg(f"UNIVERSAL_DATA itemStruct encontrado={bool(item_struct_found)} images_from_universal={len(urls_u)}")
                    for u in urls_u:
                        _add(u)
                    if urls_u:
                        return

                try:
                    sigi = page.evaluate(
                        "() => { try { return (window.__SIGI_STATE__ && typeof window.__SIGI_STATE__ === 'object') ? window.__SIGI_STATE__ : null } catch(e) { return null } }"
                    )
                    if isinstance(sigi, dict) and sigi:
                        try:
                            item_module = sigi.get("ItemModule")
                            _dbg(
                                f"SIGI_STATE encontrado. item_id_hint={preferred_item_id or 'NA'} "
                                f"ItemModule_keys={len(item_module) if isinstance(item_module, dict) else 0}"
                            )
                        except Exception:
                            _dbg("SIGI_STATE encontrado.")

                        urls, chosen_id = _extract_images_from_sigi_state(sigi)
                        stats["json"] = int(stats.get("json") or 0) + 1
                        _dbg(f"SIGI_STATE item_id={chosen_id or 'NA'} images_from_json={len(urls)}")
                        for u in urls:
                            _add(u)
                except Exception:
                    _dbg("JSON window.__SIGI_STATE__: error")

            extract_blobs()
            extract_window_state()
            _dbg(
                f"Post-carga: photomode={len(photomode)} other={len(other)} seen={len(seen_keys)} "
                f"net_hits={stats.get('net')} blob_hits={stats.get('blob')} json_hits={stats.get('json')}"
            )
            _scan_scripts_for_urls("post-blobs")

            fast_fail = (
                len(seen_keys) == 0
                and int(stats.get("net") or 0) == 0
                and int(stats.get("dom") or 0) == 0
                and int(stats.get("json") or 0) == 0
            )
            if fast_fail:
                _dbg("Fast-fail: entorno restringido (dom=0 net=0 json=0) -> saltando carrusel.")
                max_steps = 0

            carousel_selectors = [
                'button[aria-label*="Next" i]',
                'button[aria-label*="Siguiente" i]',
                'button[aria-label*="Suivant" i]',
                'button[data-e2e*="arrow-right" i]',
                'button[data-e2e*="right" i]',
                '[data-e2e*="arrow-right" i] button',
            ]

            def _try_next_button() -> bool:
                for sel in carousel_selectors:
                    try:
                        btn = page.query_selector(sel)
                    except Exception:
                        btn = None
                    if btn:
                        _dbg(f"Carrusel: usando selector Next: {sel}")
                        try:
                            btn.click(timeout=1500)
                            return True
                        except Exception:
                            _dbg(f"Carrusel: click falló selector={sel}")
                            continue
                return False

            for step in range(max_steps):
                btn = None
                try:
                    advanced = _try_next_button()
                    if not advanced:
                        if step == 0:
                            _dbg("Carrusel: no se encontró botón Next; probando ArrowRight/scroll/click.")
                        try:
                            page.keyboard.press("ArrowRight")
                            advanced = True
                        except Exception:
                            advanced = False
                    try:
                        page.wait_for_timeout(600)
                    except Exception:
                        pass
                    try:
                        page.mouse.wheel(0, 800)
                    except Exception:
                        try:
                            page.evaluate("() => window.scrollBy(0, 800)")
                        except Exception:
                            pass
                    try:
                        page.wait_for_timeout(600)
                    except Exception:
                        pass
                    if not advanced and step == 0:
                        try:
                            page.click("body", timeout=800)
                        except Exception:
                            pass
                        try:
                            page.wait_for_timeout(800)
                        except Exception:
                            pass
                    stats["next"] = int(stats.get("next") or 0) + 1
                    _scan_dom(f"step-{stats.get('next')}")
                    _scan_visible_big(f"step-{stats.get('next')}")
                    _scan_inline_bg(f"step-{stats.get('next')}")
                    extract_blobs()
                    _scan_scripts_for_urls(f"step-{stats.get('next')}")
                    _dbg(f"Carrusel step={stats.get('next')}: photomode={len(photomode)} other={len(other)} seen={len(seen_keys)} net_hits={stats.get('net')}")
                except Exception:
                    _dbg("Carrusel: click Next falló.")
                    break

            try:
                context.close()
            except Exception:
                pass
            try:
                browser.close()
            except Exception:
                pass
    except Exception:
        return [], None, None

    final_urls = photomode if photomode else other
    _dbg(f"Final: elegido={'photomode' if photomode else 'other'} count={len(final_urls)} total_seen={len(seen_keys)}")
    candidates: List[Dict[str, str]] = []
    for u in final_urls:
        ext = _guess_ext_from_url(u, "jpg").lower()
        if ext not in IMAGE_EXTS:
            ext = "jpg"
        candidates.append({"url": u, "ext": ext})
    return candidates, None, None


def _instagram_post_probe_playwright(
    url: str,
    timeout: int = 22,
    cookies_path: Optional[Path] = None,
) -> Dict[str, Any]:
    out: Dict[str, Any] = {"media_type": "unknown", "images": [], "video_url": None}
    try:
        from playwright.sync_api import sync_playwright  # type: ignore
    except Exception:
        return out

    def _is_img_url(u: str) -> bool:
        s = str(u or "").strip()
        if not s.startswith(("http://", "https://")):
            return False
        if not re.search(r"\.(jpe?g|png|webp)(\?|#|$)", s, re.IGNORECASE):
            return False
        low = s.lower()
        try:
            p0 = urlparse(s)
            path0 = (p0.path or "").lower()
        except Exception:
            path0 = ""
        if "/rsrc.php/" in path0:
            return False
        if "cdninstagram.com" in low and not re.search(r"/t51\.[^/]*-15/", path0):
            return False
        bad = ("emoji", "icon", "sticker", "avatar", "sprite", "favicon")
        return not any(x in low for x in bad)

    def _is_video_url(u: str) -> bool:
        s = str(u or "").strip()
        if not s.startswith(("http://", "https://")):
            return False
        return bool(re.search(r"\.(mp4|m3u8)(\?|#|$)", s, re.IGNORECASE))

    def _is_instagram_thumb_or_crop(u: str) -> bool:
        low = str(u or "").lower()
        if not low:
            return True
        if "stp=c" in low:
            return True
        if "s640x640" in low or "s320x320" in low or "s150x150" in low:
            return True
        if "c288.0." in low or "c0.0." in low:
            return True
        return False

    def _size_hint(u: str) -> int:
        s = str(u or "").lower()
        m = re.search(r"[_-]s(\d{2,4})x(\d{2,4})", s)
        if not m:
            return 0
        try:
            w = int(m.group(1))
            h = int(m.group(2))
            if w > 0 and h > 0:
                return w * h
        except Exception:
            return 0
        return 0

    def _score_img(u: str, w: Optional[int] = None, h: Optional[int] = None) -> int:
        base = 0
        try:
            if w is not None and h is not None and int(w) > 0 and int(h) > 0:
                base = int(w) * int(h)
        except Exception:
            base = 0
        if base <= 0:
            base = _size_hint(u)
        if _is_instagram_thumb_or_crop(u):
            base -= 1_000_000_000
        return base

    def _key(u: str) -> str:
        try:
            p = urlparse(str(u or "").strip())
            name0 = (p.path or "").rsplit("/", 1)[-1]
            return str(name0 or "").lower().strip()
        except Exception:
            return str(u or "").split("?", 1)[0].rsplit("/", 1)[-1].lower().strip()

    vid_seen: set[str] = set()
    vid_urls: List[str] = []
    img_order: Dict[str, int] = {}
    img_best_good: Dict[str, Tuple[int, str]] = {}
    img_best_any: Dict[str, Tuple[int, str]] = {}
    order_counter = 0

    def _add_img(u: str, w: Optional[int] = None, h: Optional[int] = None) -> None:
        s = str(u or "").replace("\\u002F", "/").replace("\\/", "/").replace("&amp;", "&").strip()
        if not _is_img_url(s):
            return
        try:
            if w is not None and h is not None and int(w) > 0 and int(h) > 0 and (int(w) < 260 or int(h) < 260):
                return
        except Exception:
            pass
        k = _key(s)
        if not k:
            return
        nonlocal order_counter
        if k not in img_order:
            order_counter += 1
            img_order[k] = order_counter
        score = _score_img(s, w=w, h=h)
        prev_any = img_best_any.get(k)
        if (prev_any is None) or (score > prev_any[0]):
            img_best_any[k] = (score, s)
        if not _is_instagram_thumb_or_crop(s):
            prev_good = img_best_good.get(k)
            if (prev_good is None) or (score > prev_good[0]):
                img_best_good[k] = (score, s)

    def _add_vid(u: str) -> None:
        s = str(u or "").replace("\\u002F", "/").replace("\\/", "/").replace("&amp;", "&").strip()
        if not _is_video_url(s):
            return
        k = _key(s)
        if not k or k in vid_seen:
            return
        vid_seen.add(k)
        vid_urls.append(s)

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent=(os.environ.get("MEDIA_DOWNLOADER_UA") or _DEFAULT_UA).strip(),
                locale="en-US",
                viewport={"width": 1280, "height": 1000},
            )
            rows = _read_netscape_cookies(cookies_path)
            cookies: List[Dict[str, Any]] = []
            for row in rows:
                domain = str(row.get("domain") or "").strip()
                if "instagram.com" not in domain.lower():
                    continue
                ck: Dict[str, Any] = {
                    "name": str(row.get("name") or ""),
                    "value": str(row.get("value") or ""),
                    "domain": domain,
                    "path": str(row.get("path") or "/"),
                    "secure": str(row.get("secure") or "").upper() == "TRUE",
                }
                try:
                    exp = int(float(str(row.get("expires") or "").strip() or "0"))
                except Exception:
                    exp = 0
                if exp > 0:
                    ck["expires"] = exp
                cookies.append(ck)
            if cookies:
                try:
                    context.add_cookies(cookies)
                except Exception:
                    pass

            page = context.new_page()
            page.goto(url, wait_until="domcontentloaded", timeout=max(1, int(timeout)) * 1000)
            try:
                page.wait_for_load_state("networkidle", timeout=7000)
            except Exception:
                pass

            # Meta tags and media elements.
            try:
                og_img = page.locator('meta[property="og:image"]').first.get_attribute("content")
                if og_img:
                    _add_img(og_img)
            except Exception:
                pass
            try:
                og_vid = page.locator('meta[property="og:video"]').first.get_attribute("content")
                if og_vid:
                    _add_vid(og_vid)
            except Exception:
                pass
            try:
                medias = page.evaluate(
                    "() => ({"
                    "imgs:Array.from(document.images).map(i=>({u:(i.currentSrc||i.src||''),w:(i.naturalWidth||0),h:(i.naturalHeight||0)})),"
                    "vids:Array.from(document.querySelectorAll('video,video source')).map(v=>(v.currentSrc||v.src||v.getAttribute('src')||''))"
                    "})"
                )
                if isinstance(medias, dict):
                    imgs = medias.get("imgs")
                    if isinstance(imgs, list):
                        for it in imgs:
                            if isinstance(it, dict):
                                _add_img(str(it.get("u") or ""), w=it.get("w"), h=it.get("h"))
                    vids = medias.get("vids")
                    if isinstance(vids, list):
                        for v in vids:
                            _add_vid(str(v or ""))
            except Exception:
                pass

            # Parse hydrated blobs to capture carousel images.
            try:
                html = page.content()
                for m in re.findall(r'"display_url":"(https:[^"]+)"', html):
                    _add_img(m)
                for m in re.findall(r"https://[^\s\"']+cdninstagram\.com[^\s\"']+\.(?:jpe?g|png|webp)[^\s\"']*", html, re.IGNORECASE):
                    _add_img(m)
                pat1 = re.compile(
                    r'"url":"(https:[^"]+cdninstagram[^"]+\.(?:jpe?g|png|webp)[^"]*)"[^\\{\\}\\[\\]]{0,180}?"width":(\d{2,4})[^\\{\\}\\[\\]]{0,80}?"height":(\d{2,4})',
                    re.IGNORECASE,
                )
                pat2 = re.compile(
                    r'"width":(\d{2,4})[^\\{\\}\\[\\]]{0,80}?"height":(\d{2,4})[^\\{\\}\\[\\]]{0,180}?"url":"(https:[^"]+cdninstagram[^"]+\.(?:jpe?g|png|webp)[^"]*)"',
                    re.IGNORECASE,
                )
                for m in pat1.finditer(html):
                    ctx = html[max(0, m.start() - 90) : m.start()]
                    if "cropped" in ctx:
                        continue
                    _add_img(m.group(1), w=int(m.group(2)), h=int(m.group(3)))
                for m in pat2.finditer(html):
                    ctx = html[max(0, m.start() - 90) : m.start()]
                    if "cropped" in ctx:
                        continue
                    _add_img(m.group(3), w=int(m.group(1)), h=int(m.group(2)))
                for m in re.findall(r'"video_url":"(https:[^"]+)"', html):
                    _add_vid(m)
            except Exception:
                pass

            # Try navigating carousel.
            for _ in range(12):
                btn = None
                try:
                    btn = page.query_selector(
                        'button[aria-label*="Next" i], button[aria-label*="Siguiente" i], button[aria-label*="Suivant" i]'
                    )
                except Exception:
                    btn = None
                if not btn:
                    break
                try:
                    btn.click(timeout=1200)
                    page.wait_for_timeout(250)
                    imgs2 = page.evaluate(
                        "() => Array.from(document.images).map(i=>({u:(i.currentSrc||i.src||''),w:(i.naturalWidth||0),h:(i.naturalHeight||0)}))"
                    )
                    if isinstance(imgs2, list):
                        for it in imgs2:
                            if isinstance(it, dict):
                                _add_img(str(it.get('u') or ''), w=it.get("w"), h=it.get("h"))
                except Exception:
                    break

            try:
                context.close()
            except Exception:
                pass
            try:
                browser.close()
            except Exception:
                pass
    except Exception:
        return out

    ordered_keys = sorted(img_order.items(), key=lambda kv: kv[1])
    final_imgs: List[str] = []
    for k, _pos in ordered_keys:
        chosen = None
        if k in img_best_good:
            chosen = img_best_good[k][1]
        elif k in img_best_any:
            chosen = img_best_any[k][1]
        if chosen:
            final_imgs.append(chosen)

    try:
        bucket_counts: Dict[str, int] = {}
        bucket_of: Dict[str, str] = {}
        for u in final_imgs:
            try:
                p0 = urlparse(u)
                path0 = (p0.path or "").lower()
            except Exception:
                path0 = ""
            m = re.search(r"/(t51\.[^/]+-15)/", path0)
            b = m.group(1) if m else ""
            bucket_of[u] = b
            if b:
                bucket_counts[b] = bucket_counts.get(b, 0) + 1
        if bucket_counts:
            dominant = max(bucket_counts.items(), key=lambda kv: kv[1])[0]
            if dominant and bucket_counts.get(dominant, 0) >= 2:
                final_imgs = [u for u in final_imgs if bucket_of.get(u) == dominant]
    except Exception:
        pass

    if len(final_imgs) >= 2:
        out["media_type"] = "gallery"
    elif len(final_imgs) == 1:
        out["media_type"] = "image"
    elif vid_urls:
        out["media_type"] = "video"
        out["video_url"] = vid_urls[0]
    out["images"] = final_imgs
    return out


def _instagram_shortcode(url: str) -> Optional[str]:
    try:
        p = urlparse(url)
        host = (p.netloc or "").lower()
        path = (p.path or "")
    except Exception:
        return None
    if "instagram.com" not in host:
        return None
    m = re.search(r"/p/([^/]+)/?", path)
    if not m:
        return None
    return str(m.group(1) or "").strip() or None


def _tiktok_video_url_from_photo_url(url: str) -> Optional[str]:
    try:
        p = urlparse(url)
        host = (p.netloc or "").lower()
        path = p.path or ""
    except Exception:
        return None
    if "tiktok.com" not in host or "/photo/" not in path:
        return None
    m = re.search(r"^/(@[^/]+)/photo/(\d+)", path)
    if not m:
        return None
    user = m.group(1)
    item_id = m.group(2)
    return f"https://www.tiktok.com/{user}/video/{item_id}"


def _url_key_for_selection(u: str) -> str:
    s = str(u or "").strip()
    if not s:
        return ""
    try:
        p = urlparse(s)
        host = (p.netloc or "").lower()
        path = p.path or ""
        if host and path:
            return f"{host}{path}"
    except Exception:
        pass
    return s.split("?", 1)[0].strip()


def _normalize_tiktok_photo_image_url(u: str) -> str:
    s = str(u or "").strip()
    if not s:
        return ""
    try:
        p = urlparse(s)
        host = (p.netloc or "").lower()
        if host.endswith("muscdn.com"):
            return urlunparse(p._replace(scheme="https", netloc="p16.tiktokcdn.com"))
    except Exception:
        return s
    return s


def probe_download_options(
    raw_input: str,
    kind: str,
    proxy_url: Optional[str] = None,
    force_ipv4: Optional[bool] = True,
    cookies_path: Optional[Path] = None,
) -> Dict[str, Any]:
    urls = _split_links(raw_input)
    if not urls:
        return {}

    url = _normalize_url(urls[0])
    host = urlparse(url).netloc.lower()
    if "youtube.com" not in host and "youtu.be" not in host:
        return {}

    opts: Dict[str, Any] = {
        "quiet": True,
        "noprogress": True,
        "skip_download": True,
        "socket_timeout": 25,
        "no_warnings": True,
        "logger": _NullLogger(),
        "extractor_args": {"youtube": {"player_client": _client_order()}},
    }
    opts.update(
        _ydl_net_opts(
            proxy_url=proxy_url,
            cookies_path=cookies_path,
            force_ipv4=force_ipv4,
            set_cookiefile=True,
        )
    )

    with yt_dlp.YoutubeDL(opts) as ydl:
        info = ydl.extract_info(url, download=False)

    formats = info.get("formats") or []
    if kind == "video":
        heights = sorted(
            {
                int(fmt.get("height"))
                for fmt in formats
                if fmt.get("height") and fmt.get("vcodec") not in (None, "none")
            },
            reverse=True,
        )
        choices = [str(h) for h in heights if h > 0]
        if not choices:
            choices = ["360", "480", "720", "1080"]
        return {
            "detail_choices": choices,
            "detail_value": choices[0],
            "format_choices": ["mp4", "mkv", "webm"],
            "format_value": "mp4",
        }

    if kind == "audio":
        abrs = sorted(
            {
                int(round(float(fmt.get("abr") or fmt.get("tbr") or 0)))
                for fmt in formats
                if (fmt.get("acodec") not in (None, "none")) and (fmt.get("abr") or fmt.get("tbr"))
            },
            reverse=True,
        )
        normalized = []
        for abr in abrs:
            if abr <= 0:
                continue
            snapped = min([64, 96, 128, 160, 192, 256, 320], key=lambda x: abs(x - abr))
            normalized.append(snapped)
        choices = [str(v) for v in sorted(set(normalized), reverse=True)]
        if not choices:
            choices = ["320", "256", "192", "160", "128"]
        return {
            "detail_choices": choices,
            "detail_value": choices[0],
            "format_choices": ["mp3", "m4a", "ogg", "opus", "wav", "flac"],
            "format_value": "mp3",
        }

    return {}


def probe_media_capabilities(
    raw_input: str,
    proxy_url: Optional[str] = None,
    force_ipv4: Optional[bool] = True,
    cookies_path: Optional[Path] = None,
) -> Dict[str, Any]:
    urls = _split_links(raw_input)
    if not urls:
        return {}

    url = _normalize_url(urls[0])
    host = (urlparse(url).netloc or "").lower()
    path = (urlparse(url).path or "").lower()
    platform = "other"
    if "youtube.com" in host or "youtu.be" in host:
        platform = "youtube"
    elif "tiktok.com" in host:
        platform = "tiktok"
    elif "instagram.com" in host:
        platform = "instagram"

    if platform == "tiktok" and "/photo/" in path:
        return {
            "url": url,
            "platform": platform,
            "supports_audio": False,
            "supports_video": False,
            "supports_image": True,
            "allowed_kinds": ["imagen"],
            "detected_kind": "imagen",
            "summary": "TikTok photo: se asume imagen/carrusel",
        }

    is_instagram_reel = platform == "instagram" and "/reel/" in path
    is_instagram_post = platform == "instagram" and "/p/" in path and not is_instagram_reel

    opts: Dict[str, Any] = {
        "quiet": True,
        "noprogress": True,
        "skip_download": True,
        "socket_timeout": 25,
        "extractor_args": {"youtube": {"player_client": _client_order()}},
    }
    opts.update(
        _ydl_net_opts(
            proxy_url=proxy_url,
            cookies_path=cookies_path,
            force_ipv4=force_ipv4,
            set_cookiefile=False,
        )
    )

    supports_image = False
    supports_video = False
    supports_audio = False
    detected_kind = "audio"
    summary = "Sin detección"
    auth_required = False
    used_cookies = False

    try:
        info, used_cookies, auth_failed = _extract_info_with_cookie_fallback(url, opts, cookies_path)
        if not info:
            if is_instagram_post:
                ig_pw = _instagram_post_probe_playwright(url, timeout=22, cookies_path=cookies_path)
                ig_type = str(ig_pw.get("media_type") or "unknown").lower()
                ig_imgs = ig_pw.get("images") or []
                if ig_type in ("image", "gallery") and isinstance(ig_imgs, list) and ig_imgs:
                    supports_image, supports_video, supports_audio = True, False, False
                    detected_kind = "imagen"
                    summary = "Instagram post: imagen/carrusel detectado (fallback Playwright)"
                    return {
                        "url": url,
                        "platform": platform,
                        "supports_audio": supports_audio,
                        "supports_video": supports_video,
                        "supports_image": supports_image,
                        "allowed_kinds": ["imagen"],
                        "detected_kind": detected_kind,
                        "summary": summary,
                        "auth_required": False,
                        "used_cookies": used_cookies,
                    }
                if ig_type == "video":
                    supports_image, supports_video, supports_audio = False, True, True
                    detected_kind = "video"
                    summary = "Instagram post: video detectado (fallback Playwright)"
                    return {
                        "url": url,
                        "platform": platform,
                        "supports_audio": supports_audio,
                        "supports_video": supports_video,
                        "supports_image": supports_image,
                        "allowed_kinds": ["audio", "video"],
                        "detected_kind": detected_kind,
                        "summary": summary,
                        "auth_required": False,
                        "used_cookies": used_cookies,
                    }
            if platform == "instagram" and auth_failed:
                auth_required = True
                summary = "Instagram requiere autenticación adicional."
            raise RuntimeError("no_info")

        formats = info.get("formats") or []
        supports_video = any(
            (fmt.get("vcodec") not in (None, "none")) and (fmt.get("height") or (fmt.get("ext") or "").lower() in VIDEO_EXTS)
            for fmt in formats
        ) or (str(info.get("ext") or "").lower() in VIDEO_EXTS)

        supports_audio = any(
            (fmt.get("acodec") not in (None, "none")) for fmt in formats
        ) or supports_video

        candidates = _collect_image_candidates(info)
        supports_image = (len(candidates) > 0) and (not supports_video)
        if is_instagram_post and not supports_video and not supports_image:
            ig_pw = _instagram_post_probe_playwright(url, timeout=22, cookies_path=cookies_path)
            ig_type = str(ig_pw.get("media_type") or "unknown").lower()
            ig_imgs = ig_pw.get("images") or []
            if ig_type in ("image", "gallery") and isinstance(ig_imgs, list) and ig_imgs:
                supports_image, supports_video, supports_audio = True, False, False
            elif ig_type == "video":
                supports_image, supports_video, supports_audio = False, True, True

        if is_instagram_reel:
            supports_image = False
            supports_video, supports_audio = True, True
            detected_kind = "video"
            summary = "Instagram reel: video"
        elif is_instagram_post:
            if supports_video:
                supports_image = False
                supports_audio = True
                detected_kind = "video"
                summary = "Instagram post: video detectado"
            elif supports_image:
                supports_video, supports_audio = False, False
                detected_kind = "imagen"
                summary = "Instagram post: imagen/carrusel detectado"
            else:
                auth_required = bool(auth_failed)
                supports_image = True
                supports_video = False
                supports_audio = False
                detected_kind = "imagen"
                summary = "Instagram post: no se pudo determinar el contenido."
        else:
            if supports_image and not supports_video:
                detected_kind = "imagen"
                summary = "Post de imagen/carrusel detectado"
            elif supports_video:
                detected_kind = "video"
                summary = "Video detectado"
            elif supports_audio:
                detected_kind = "audio"
                summary = "Audio detectado"
    except Exception as e:
        if platform == "instagram" and _is_auth_error(str(e)):
            auth_required = True
        # Fallback heurístico por URL cuando el extractor no responda.
        if platform == "youtube":
            supports_video, supports_audio, supports_image = True, True, False
            detected_kind, summary = "video", "YouTube: se asume video/audio"
        elif platform == "instagram":
            if "/reel/" in path:
                supports_video, supports_audio, supports_image = True, True, False
                detected_kind, summary = "video", "Instagram reel: video/audio"
            elif "/p/" in path:
                if auth_required:
                    supports_video, supports_audio, supports_image = False, False, True
                    detected_kind, summary = "imagen", "Instagram requiere autenticación válida para acceder a este contenido."
                else:
                    supports_video, supports_audio, supports_image = True, False, True
                    detected_kind, summary = "imagen", "Instagram post: no se pudo determinar (elige IMAGE o VIDEO)."
            else:
                supports_video, supports_audio, supports_image = True, True, False
                detected_kind, summary = "video", "Instagram: se asume video/audio"
        elif platform == "tiktok":
            if "/photo/" in path:
                supports_video, supports_audio, supports_image = False, False, True
                detected_kind, summary = "imagen", "TikTok photo: se asume imagen/carrusel"
            else:
                supports_video, supports_audio, supports_image = True, True, True
                detected_kind, summary = "video", "TikTok: compatibilidad mixta"
        else:
            supports_video, supports_audio, supports_image = True, True, False
            detected_kind, summary = "video", f"Fallback por error de detección: {type(e).__name__}"

    allowed_kinds: List[str] = []
    if supports_audio:
        allowed_kinds.append("audio")
    if supports_video:
        allowed_kinds.append("video")
    if supports_image:
        allowed_kinds.append("imagen")
    if not allowed_kinds:
        allowed_kinds = ["audio", "video"]

    return {
        "url": url,
        "platform": platform,
        "supports_audio": supports_audio,
        "supports_video": supports_video,
        "supports_image": supports_image,
        "allowed_kinds": allowed_kinds,
        "detected_kind": detected_kind if detected_kind in allowed_kinds else allowed_kinds[0],
        "summary": summary,
        "auth_required": auth_required,
        "used_cookies": used_cookies,
    }


def probe_image_candidates(
    raw_input: str,
    proxy_url: Optional[str] = None,
    force_ipv4: Optional[bool] = True,
    cookies_path: Optional[Path] = None,
    max_items: int = 24,
) -> List[Dict[str, str]]:
    urls = _split_links(raw_input)
    if not urls:
        return []

    url = _normalize_url(urls[0])
    try:
        eff, _ = resolve_tiktok_url_for_detection(url)
        if eff:
            url = eff
    except Exception:
        pass
    opts: Dict[str, Any] = {
        "quiet": True,
        "noprogress": True,
        "skip_download": True,
        "socket_timeout": 25,
        "no_warnings": True,
        "ignoreerrors": True,
        "logger": _NullLogger(),
    }
    opts.update(
        _ydl_net_opts(
            proxy_url=proxy_url,
            cookies_path=cookies_path,
            force_ipv4=force_ipv4,
            set_cookiefile=False,
        )
    )

    try:
        p = urlparse(url)
        host = (p.netloc or "").lower()
        path = (p.path or "").lower()
    except Exception:
        host, path = "", ""

    if "tiktok.com" in host and "/photo/" in path:
        fallback_used = False
        try:
            use_pw = (os.environ.get("MEDIA_DOWNLOADER_TIKTOK_PHOTO_PLAYWRIGHT", "1") or "").strip().lower()
            candidates2 = []
            if use_pw not in ("0", "false", "no", "off"):
                candidates2, _, _ = _tiktok_photo_candidates_playwright(url, timeout=12, cookies_path=cookies_path)
            if not candidates2:
                candidates2, _, _ = _tiktok_photo_candidates(url, timeout=18)
        except Exception:
            candidates2 = []
        if not candidates2:
            og = _tiktok_photo_og_image(url, timeout=12)
            if og:
                candidates2 = [{"url": og}]
                fallback_used = True
        if not candidates2:
            prev = _tiktok_photo_preview_from_html(url, timeout=12)
            if prev:
                candidates2 = [{"url": prev}]
                fallback_used = True
        if not candidates2:
            try:
                use_ytdlp_fallback = (
                    os.environ.get("MEDIA_DOWNLOADER_TIKTOK_PHOTO_TRY_YTDLP_VIDEO_FALLBACK", "0") or ""
                ).strip().lower()
            except Exception:
                use_ytdlp_fallback = "0"
            if use_ytdlp_fallback not in ("0", "false", "no", "off"):
                video_url = _tiktok_video_url_from_photo_url(url)
                if video_url:
                    info2, _, _ = _extract_info_with_cookie_fallback(video_url, opts, cookies_path)
                    if info2:
                        candidates2 = _collect_image_candidates(info2)
                        fallback_used = bool(candidates2)
        out2: List[Dict[str, str]] = []
        for idx, c in enumerate(candidates2[: max(1, int(max_items))], start=1):
            nu = _normalize_tiktok_photo_image_url(str(c.get("url") or ""))
            out2.append(
                {
                    "url": nu,
                    "label": (f"PREVIEW {idx}" if fallback_used else f"IMAGE {idx}"),
                    "fallback": ("1" if fallback_used else "0"),
                }
            )
        return [c for c in out2 if c.get("url")]

    is_instagram_post = "instagram.com" in host and "/p/" in path and "/reel/" not in path
    info, _, _ = _extract_info_with_cookie_fallback(url, opts, cookies_path)
    if not info:
        if is_instagram_post:
            ig_pw = _instagram_post_probe_playwright(url, timeout=22, cookies_path=cookies_path)
            if str(ig_pw.get("media_type") or "").lower() in ("image", "gallery"):
                imgs = [str(u or "").strip() for u in (ig_pw.get("images") or []) if str(u or "").strip()]
                return [{"url": u, "label": f"IMAGE {i}"} for i, u in enumerate(imgs[: max(1, int(max_items))], start=1)]
        return []

    candidates = _collect_image_candidates(info)
    if not candidates and is_instagram_post:
        ig_pw = _instagram_post_probe_playwright(url, timeout=22, cookies_path=cookies_path)
        if str(ig_pw.get("media_type") or "").lower() in ("image", "gallery"):
            imgs = [str(u or "").strip() for u in (ig_pw.get("images") or []) if str(u or "").strip()]
            return [{"url": u, "label": f"IMAGE {i}"} for i, u in enumerate(imgs[: max(1, int(max_items))], start=1)]

    out: List[Dict[str, str]] = []
    for idx, c in enumerate(candidates[: max(1, int(max_items))], start=1):
        out.append({"url": str(c.get("url") or ""), "label": f"IMAGE {idx}"})
    return [c for c in out if c.get("url")]


def _is_tiktok_video_url(url: str) -> bool:
    try:
        p = urlparse(url)
        host = (p.netloc or "").lower()
        path = (p.path or "").lower()
    except Exception:
        return False
    if "tiktok.com" not in host:
        return False
    if "/photo/" in path:
        return False
    return "/video/" in path or bool(re.search(r"/@[^/]+/video/\d+", path))


def _tiktok_parse_user_and_id(url: str) -> Tuple[Optional[str], Optional[str]]:
    try:
        p = urlparse(url)
        path = p.path or ""
    except Exception:
        return None, None
    m = re.search(r"/@([^/]+)/video/(\d+)", path)
    if m:
        return (m.group(1) or None), (m.group(2) or None)
    m2 = re.search(r"/video/(\d+)", path)
    if m2:
        return None, (m2.group(1) or None)
    return None, None


def _tiktok_score_video_url(u: str) -> int:
    s = str(u or "").strip()
    if not s:
        return -10_000
    low = s.lower()
    score = 0
    if "playwm" in low or "watermark" in low:
        score -= 500
    if "mime_type=video_mp4" in low or "mime_type=video%2fmp4" in low:
        score += 20
    if ".mp4" in low:
        score += 10
    if "tiktokcdn.com" in low:
        score += 5
    if "bytevc1" in low or "hvc1" in low or "avc1" in low:
        score += 2
    return score


def _tiktok_pick_best_video_url(candidates: List[str]) -> Tuple[Optional[str], bool]:
    uniq: List[str] = []
    seen = set()
    for u in candidates:
        s = str(u or "").strip()
        if not s:
            continue
        if not s.startswith(("http://", "https://")):
            continue
        s = s.replace("\\u002F", "/").replace("\\/", "/").replace("&amp;", "&")
        k = s.split("#", 1)[0].strip()
        if k in seen:
            continue
        seen.add(k)
        uniq.append(s)
    if not uniq:
        return None, False
    uniq.sort(key=_tiktok_score_video_url, reverse=True)
    best = uniq[0]
    wm = ("playwm" in best.lower()) or ("watermark" in best.lower())
    return best, wm


def _tiktok_discover_video_url_playwright(
    url: str,
    cookies_path: Optional[Path],
    timeout: int = 30,
) -> Tuple[Optional[str], bool, str]:
    try:
        from playwright.sync_api import sync_playwright  # type: ignore
    except Exception:
        return None, False, "Playwright no está disponible en este entorno."

    eff = url
    try:
        resolved, _ = resolve_tiktok_url_for_detection(url)
        if resolved:
            eff = resolved
    except Exception:
        eff = url

    candidates: List[str] = []

    def _add_candidate(u: Optional[str]) -> None:
        s = str(u or "").strip()
        if not s:
            return
        if not s.startswith(("http://", "https://")):
            return
        if ".mp4" not in s.lower() and "mime_type=video" not in s.lower():
            return
        candidates.append(s)

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent=(os.environ.get("MEDIA_DOWNLOADER_UA") or _DEFAULT_UA).strip(),
                locale="en-US",
                viewport={"width": 1200, "height": 900},
            )

            if cookies_path and cookies_path.exists() and cookies_path.is_file():
                try:
                    cookies: List[Dict[str, Any]] = []
                    for line in cookies_path.read_text(encoding="utf-8", errors="ignore").splitlines():
                        t = (line or "").strip()
                        if not t or t.startswith("#"):
                            continue
                        parts = t.split("\t")
                        if len(parts) < 7:
                            continue
                        domain, _flag, path0, secure, expires, name, value = parts[:7]
                        domain = (domain or "").strip()
                        if "tiktok.com" not in domain.lower():
                            continue
                        path0 = (path0 or "/").strip() or "/"
                        secure_bool = str(secure or "").strip().upper() == "TRUE"
                        try:
                            exp = int(float(expires))
                        except Exception:
                            exp = -1
                        ck: Dict[str, Any] = {
                            "name": str(name or ""),
                            "value": str(value or ""),
                            "domain": domain,
                            "path": path0,
                            "secure": secure_bool,
                        }
                        if exp > 0:
                            ck["expires"] = exp
                        cookies.append(ck)
                    if cookies:
                        context.add_cookies(cookies)
                except Exception:
                    pass

            page = context.new_page()

            def on_response(resp: Any) -> None:
                try:
                    u2 = str(resp.url or "").strip()
                except Exception:
                    u2 = ""
                if not u2:
                    return
                try:
                    hdrs = resp.headers or {}
                    ctype = str(hdrs.get("content-type") or "").lower()
                except Exception:
                    ctype = ""
                if "video" in ctype or ".mp4" in u2.lower():
                    candidates.append(u2)

            page.on("response", on_response)

            try:
                page.goto(eff, wait_until="domcontentloaded", timeout=int(timeout * 1000))
            except Exception:
                pass

            try:
                page.wait_for_load_state("networkidle", timeout=15000)
            except Exception:
                pass

            try:
                page.wait_for_timeout(1500)
            except Exception:
                pass

            try:
                for sel in [
                    'meta[property="og:video"]',
                    'meta[property="og:video:url"]',
                    'meta[name="twitter:player:stream"]',
                    'meta[property="twitter:player:stream"]',
                ]:
                    el = page.query_selector(sel)
                    if el:
                        _add_candidate(el.get_attribute("content"))
            except Exception:
                pass

            try:
                vid = page.query_selector("video")
                if vid:
                    _add_candidate(vid.get_attribute("src"))
                    try:
                        cur = page.evaluate("(el) => el.currentSrc || ''", vid)
                        _add_candidate(cur)
                    except Exception:
                        pass
            except Exception:
                pass

            try:
                context.close()
            except Exception:
                pass
            try:
                browser.close()
            except Exception:
                pass
    except Exception as e:
        return None, False, f"Playwright falló: {type(e).__name__}"

    best, wm = _tiktok_pick_best_video_url(candidates)
    if not best:
        return None, False, "No se detectó ningún stream mp4 por Playwright."
    return best, wm, ""


def _download_binary_to_path(
    file_url: str,
    dest: Path,
    referer: str,
    user_agent: str,
    cookies_path: Optional[Path],
    proxy_url: Optional[str],
    timeout: int = 60,
) -> Tuple[bool, str]:
    try:
        headers = {
            "User-Agent": user_agent,
            "Referer": referer,
            "Accept": "video/*,*/*;q=0.8",
        }
        ck = _cookie_header_for_domain(cookies_path, "tiktok.com")
        if ck:
            headers["Cookie"] = ck

        opener_handlers: List[Any] = [urllib.request.HTTPSHandler(context=ssl.create_default_context())]
        effective_proxy = proxy_url or _env_proxy()
        if isinstance(effective_proxy, str) and effective_proxy.strip():
            opener_handlers.append(
                urllib.request.ProxyHandler({"http": effective_proxy.strip(), "https": effective_proxy.strip()})
            )
        opener = urllib.request.build_opener(*opener_handlers)

        req = urllib.request.Request(file_url, headers=headers, method="GET")
        with opener.open(req, timeout=timeout) as resp:
            ctype = str(getattr(resp, "headers", {}).get("Content-Type") or "").lower()
            dest.parent.mkdir(parents=True, exist_ok=True)
            with open(dest, "wb") as fh:
                head = resp.read(16384)
                if not head:
                    return False, "Contenido vacío."
                if ("video" not in ctype) and (b"ftyp" not in head[:4096]):
                    return False, "La respuesta no parece ser un video."
                fh.write(head)
                shutil.copyfileobj(resp, fh)
                fh.flush()
        try:
            if dest.exists() and dest.stat().st_size > 1024:
                return True, ""
        except Exception:
            pass
        return False, "Archivo descargado inválido."
    except Exception as e:
        return False, f"{type(e).__name__}"


def _try_tiktok_video_playwright_first(
    url: str,
    out_dir: Path,
    per_channel_folders: bool,
    cookies_path: Optional[Path],
    proxy_url: Optional[str],
) -> Optional[Path]:
    enable = (os.environ.get("MEDIA_DOWNLOADER_TIKTOK_PW_VIDEO", "1") or "").strip().lower()
    if enable in ("0", "false", "no", "off"):
        return None
    if not _is_tiktok_video_url(url):
        return None

    user, vid = _tiktok_parse_user_and_id(url)
    uploader = _slugify(user or "tiktok", maxlen=50)
    target_dir = out_dir / uploader if per_channel_folders else out_dir
    target_dir.mkdir(parents=True, exist_ok=True)
    base = f"tiktok_{vid}" if vid else "tiktok_video"
    dest = (target_dir / f"{base}.mp4").resolve()
    if dest.exists():
        dest = (target_dir / f"{base}_{os.urandom(3).hex()}.mp4").resolve()

    print(f"[TIKTOK-PW] Intentando extraer video por Playwright: {url}", flush=True)
    best_url, wm, reason = _tiktok_discover_video_url_playwright(url, cookies_path=cookies_path)
    if not best_url:
        print(f"[TIKTOK-PW] No se pudo detectar mp4. Fallback yt-dlp. Motivo: {reason}", flush=True)
        return None

    print(f"[TIKTOK-PW] Candidato seleccionado ({'WM' if wm else 'NO-WM'}): {best_url}", flush=True)
    ok, err = _download_binary_to_path(
        best_url,
        dest=dest,
        referer=url,
        user_agent=(os.environ.get("MEDIA_DOWNLOADER_UA") or _DEFAULT_UA).strip(),
        cookies_path=cookies_path,
        proxy_url=proxy_url,
        timeout=90,
    )
    if not ok:
        try:
            if dest.exists():
                dest.unlink()
        except Exception:
            pass
        print(f"[TIKTOK-PW] Descarga falló. Fallback yt-dlp. Error: {err}", flush=True)
        return None

    print(f"[TIKTOK-PW] Descarga OK: {dest.name}", flush=True)
    return dest.resolve()


# -------------------- download com yt-dlp --------------------
def download_with_ytdlp(
    urls: List[str],
    kind: str,
    audio_format: str,
    abr_kbps: int,
    container: str,
    max_height: int,
    out_dir: Path,
    per_channel_folders: bool,
    cookies_path: Optional[Path],
    proxy_url: Optional[str],
    force_ipv4: Optional[bool],
    sleep_between: float,
    sleep_requests: float,
    geo_country: Optional[str],
    max_retries: int,
    pl_start: Optional[int] = None,
    pl_end: Optional[int] = None,
    po_token: Optional[str] = None,
) -> Tuple[List[Path], List[Tuple[str, str]]]:

    out_dir.mkdir(parents=True, exist_ok=True)

    h = int(max_height)
    if kind == "audio":
        fmt_str = "bestaudio/best"
    elif kind == "video":
        # Prioriza formatos progresivos completos, que funcionan mejor en YouTube/TikTok/Instagram.
        fmt_str = (
            f"best[ext=mp4][height<=?{h}]/"
            f"best[height<=?{h}]/"
            f"best[ext=mp4]/"
            f"best/"
            f"bestvideo[height<=?{h}]+bestaudio/"
            f"bestvideo+bestaudio"
        )
    else:
        # Para "imagen", simplemente usa el mejor archivo disponible
        fmt_str = "best/bestvideo+bestaudio/best"

    base_tmpl = "%(title)s.%(ext)s"
    if per_channel_folders:
        base_tmpl = "%(uploader)s/" + base_tmpl

    postprocessors: List[Dict[str, Any]] = []
    merge_output_format: Optional[str] = None
    base_opts_extra: Dict[str, Any] = {}
    if kind == "audio":
        postprocessors.append({
            "key": "FFmpegExtractAudio",
            "preferredcodec": audio_format,
            "preferredquality": str(int(abr_kbps)),
        })
        postprocessors.append({"key": "FFmpegMetadata"})
        base_opts_extra["keepvideo"] = False
    elif kind == "video":
        merge_output_format = container
    else:
        # Para "imagen", no aplicar merge ni postprocesadores extra
        pass

    clients = _client_order()
    extractor_args: Dict[str, Dict[str, List[str]]] = {"youtube": {"player_client": clients}}
    if po_token:
        extractor_args["youtube"]["po_token"] = [po_token]

    ydl_logger = _YTDLPCaptureLogger(max_lines=250)
    base_opts: Dict[str, Any] = {
        "format": fmt_str,
        "outtmpl": str(out_dir / base_tmpl),
        "noplaylist": False,
        "quiet": True,
        "noprogress": True,
        "no_warnings": True,
        "logger": ydl_logger,
        "merge_output_format": merge_output_format,
        "postprocessors": postprocessors,
        "socket_timeout": 30,
        "retries": max(1, int(max_retries)),
        "extractor_retries": max(1, int(max_retries)),
        "sleep_interval": max(0.0, float(sleep_between)),
        "max_sleep_interval": max(0.0, float(sleep_between)),
        "sleep_requests": max(0.0, float(sleep_requests)),
        "max_sleep_interval_requests": max(0.0, float(sleep_requests)),
        "skip_unavailable_fragments": True,
        "extractor_args": extractor_args,
        **base_opts_extra,
    }

    base_opts.update(
        _ydl_net_opts(
            proxy_url=proxy_url,
            cookies_path=cookies_path,
            force_ipv4=force_ipv4,
            set_cookiefile=False,
        )
    )

    cookiefile = str(cookies_path) if cookies_path else None
    if geo_country:
        base_opts["geo_bypass_country"] = geo_country.upper().strip()

    ff = _ffmpeg_location()
    if ff:
        os.environ["PATH"] = f"{ff}{os.pathsep}{os.environ.get('PATH','')}"

    fails: List[Tuple[str, str]] = []
    results_before = set(p.resolve() for p in out_dir.rglob("*") if p.is_file())

    def _run_download(ydl: yt_dlp.YoutubeDL, url: str, desc: str = "") -> Optional[str]:
        """Executa download com backoff; alterna IPv4/IPv6 se erro de DNS."""
        attempts = max(1, int(max_retries))
        base_wait = 20.0
        switched_ip_stack = False
        for attempt in range(attempts):
            if attempt == 0:
                _log_cookie_state("[YTDLP download]", url, ydl.params, cookies_path)
            try:
                try:
                    _apply_youtube_cookiefile(url, ydl.params)
                except Exception:
                    pass
                try:
                    _apply_youtube_ydl_tuning(url, ydl.params, kind, h)
                except Exception:
                    pass
                ydl.download([url])
                return None
            except DownloadError as e:
                msg = str(e)
                low = msg.lower()
                if _is_youtube_url(url):
                    tail = ""
                    try:
                        tail = ydl_logger.tail(80)
                    except Exception:
                        tail = ""
                    try:
                        print(f"[YOUTUBE] yt-dlp error: {msg}", flush=True)
                        if tail:
                            print(f"[YOUTUBE] yt-dlp log tail:\n{tail}", flush=True)
                    except Exception:
                        pass

                if _is_auth_error(msg):
                    try:
                        host = (urlparse(url).netloc or "").lower()
                    except Exception:
                        host = ""
                    if "instagram.com" in host:
                        if cookiefile and not ydl.params.get("cookiefile"):
                            ydl.params["cookiefile"] = cookiefile
                            time.sleep(1.0)
                            continue
                        if not cookiefile:
                            return "Instagram requiere autenticación adicional y no se encontraron cookies válidas."
                        return "No se pudo acceder al contenido incluso usando cookies."

                if "impersonation" in low or "no impersonate target" in low:
                    return "TikTok requiere compatibilidad adicional del extractor en este entorno."

                if "unsupported url" in low:
                    return "Este tipo de enlace aún no está soportado por el descargador actual."

                if _is_dns_error(msg) and not switched_ip_stack:
                    # 1º intento: quitar restricciones de IP stack
                    ydl.params.pop("force_ipv4", None)
                    ydl.params.pop("force_ipv6", None)
                    ydl.params.pop("source_address", None)
                    switched_ip_stack = True
                    time.sleep(1.0)
                    continue

                if _is_dns_error(msg) and switched_ip_stack:
                    # Ya probamos sin restricciones, registrar error claro
                    return (
                        f"DNS falló para esta URL. Tu red no puede resolver el dominio. "
                        f"Intenta: 1) usar un proxy/VPN, 2) desactivar 'Forçar IPv4', "
                        f"3) cambiar DNS a 8.8.8.8. Error original: {msg}"
                    )

                if _is_rate_limit_error(msg) and attempt < attempts - 1:
                    wait_s = base_wait * (attempt + 1)  # 20s, 40s, 60s, ...
                    time.sleep(wait_s)
                    continue
                if _is_youtube_url(url):
                    tail = ""
                    try:
                        tail = ydl_logger.tail(80)
                    except Exception:
                        tail = ""
                    if tail:
                        return f"yt-dlp: {msg}\n{tail}"
                return f"yt-dlp: {msg}"
            except Exception as e:
                try:
                    if _is_youtube_url(url):
                        print(f"[YOUTUBE] Exception: {type(e).__name__}: {e}", flush=True)
                        print(traceback.format_exc(), flush=True)
                except Exception:
                    pass
                return f"erro: {type(e).__name__}: {e}"
        return "erro: tentativas esgotadas"

    with yt_dlp.YoutubeDL(base_opts) as ydl:
        for u in urls:
            url = _normalize_url(u)
            if _is_youtube_url(url) and not _is_yt_playlist_url(url):
                try:
                    url2 = _normalize_youtube_single_video_url(url)
                    if url2 and url2 != url:
                        print(f"[YOUTUBE] Normalized URL: {url} -> {url2}", flush=True)
                        url = url2
                except Exception:
                    pass
            try:
                if _is_yt_playlist_url(url):
                    start = int(pl_start) if pl_start else 1
                    end = int(pl_end) if pl_end else None
                    step = max(1, int(PLAYLIST_CHUNK_SIZE))
                    idx = start
                    zeros_consecutivos = 0
                    while True:
                        if end is not None and idx > end:
                            break
                        chunk_end = idx + step - 1
                        if end is not None:
                            chunk_end = min(chunk_end, end)

                        ydl.params.pop("playliststart", None)
                        ydl.params.pop("playlistend", None)
                        ydl.params["playlist_items"] = f"{idx}-{chunk_end}"

                        before = set(p.resolve() for p in out_dir.rglob("*") if p.is_file())
                        err = _run_download(ydl, url, desc=f"{idx}-{chunk_end}")
                        after = set(p.resolve() for p in out_dir.rglob("*") if p.is_file())

                        if err:
                            fails.append((url, f"{err} (itens {idx}-{chunk_end})"))

                        new_files = len(after - before)
                        if new_files == 0:
                            zeros_consecutivos += 1
                        else:
                            zeros_consecutivos = 0
                        if zeros_consecutivos >= 2:
                            break

                        ydl.params.pop("playlist_items", None)
                        time.sleep(float(sleep_between))
                        idx = chunk_end + 1
                else:
                    if pl_start is not None:
                        ydl.params["playliststart"] = int(pl_start)
                    if pl_end is not None:
                        ydl.params["playlistend"] = int(pl_end)

                    try:
                        if kind == "video" and _is_tiktok_video_url(url):
                            pw_path = _try_tiktok_video_playwright_first(
                                url,
                                out_dir=out_dir,
                                per_channel_folders=per_channel_folders,
                                cookies_path=cookies_path,
                                proxy_url=str(ydl.params.get("proxy") or "") or proxy_url,
                            )
                            if pw_path and pw_path.exists():
                                ydl.params.pop("playliststart", None)
                                ydl.params.pop("playlistend", None)
                                time.sleep(float(sleep_between))
                                continue
                    except Exception as e:
                        print(f"[TIKTOK-PW] Error inesperado, usando fallback yt-dlp: {type(e).__name__}", flush=True)

                    err = _run_download(ydl, url)
                    if err:
                        fails.append((url, err))

                    ydl.params.pop("playliststart", None)
                    ydl.params.pop("playlistend", None)
                    time.sleep(float(sleep_between))

            except Exception as e:
                fails.append((url, f"erro: {type(e).__name__}: {e}"))

    results_after = set(p.resolve() for p in out_dir.rglob("*") if p.is_file())
    generated = sorted(results_after - results_before)
    return generated, fails


# -------------------- orquestração + nome do ZIP --------------------
def process_links(
    raw_input: str,
    kind: str,
    audio_format: str,
    abr_kbps: int,
    container: str,
    max_height: int,
    per_channel_folders: bool,
    cookies_path: Optional[Path],
    proxy_url: Optional[str],
    force_ipv4: Optional[bool],
    sleep_between: float,
    sleep_requests: float,
    geo_country: Optional[str],
    max_retries: int,
    pl_start: Optional[int] = None,
    pl_end: Optional[int] = None,
    po_token: Optional[str] = None,
    selected_image_urls: Optional[List[str]] = None,
    selected_local_paths: Optional[List[Path]] = None,
) -> Tuple[str, List[Path], List[Tuple[str, str]]]:

    urls = _split_links(raw_input)
    if not urls:
        raise RuntimeError("Nenhum link válido.")

    downloads_root = Path("downloads")
    downloads_root.mkdir(exist_ok=True)
    temp_name = f"session_{datetime.now().strftime('%Y%m%d%H%M%S')}_{os.urandom(4).hex()}"
    tmp_root = downloads_root / temp_name
    out_dir = tmp_root / "out"
    out_dir.mkdir(parents=True, exist_ok=True)

    if kind == "imagen":
        generated: List[Path] = []
        failures: List[Tuple[str, str]] = []

        should_run_remote = selected_image_urls is None or (isinstance(selected_image_urls, list) and len(selected_image_urls) > 0)
        if should_run_remote:
            gen_remote, fail_remote = download_images_with_ytdlp(
                urls,
                out_dir,
                per_channel_folders,
                cookies_path,
                proxy_url,
                force_ipv4,
                sleep_between,
                sleep_requests,
                geo_country,
                max_retries,
                selected_image_urls,
            )
            generated.extend(gen_remote)
            failures.extend(fail_remote or [])

        local_list = [p for p in (selected_local_paths or []) if isinstance(p, Path)]
        if local_list:
            target_dir = out_dir
            target_dir.mkdir(parents=True, exist_ok=True)

            for idx, src in enumerate(local_list, start=1):
                try:
                    srcp = src.resolve()
                    if not srcp.exists() or not srcp.is_file():
                        failures.append((str(src), "Arquivo local não encontrado."))
                        continue

                    ext = (srcp.suffix or ".jpg").lstrip(".").lower()
                    if ext not in IMAGE_EXTS:
                        ext = "jpg"
                    name = f"preview.{ext}" if len(local_list) == 1 else f"preview_{idx:02d}.{ext}"
                    dest = (target_dir / name).resolve()
                    if dest.exists():
                        dest = (target_dir / f"preview_{os.urandom(3).hex()}.{ext}").resolve()
                    shutil.copy2(srcp, dest)
                    generated.append(dest.resolve())
                except Exception as e:
                    failures.append((str(src), f"erro: {type(e).__name__}: {e}"))
    else:
        generated, failures = download_with_ytdlp(
            urls, kind, audio_format, abr_kbps, container, max_height,
            out_dir, per_channel_folders, cookies_path, proxy_url,
            force_ipv4, sleep_between, sleep_requests, geo_country, max_retries,
            pl_start, pl_end, po_token
        )

    if not generated and not failures:
        raise RuntimeError("Nenhum arquivo foi gerado. Verifique os links e permissões.")

    n = len(generated)
    channel_part: Optional[str] = None
    if per_channel_folders and n > 0:
        chans = set()
        for p in generated:
            try:
                rel = p.relative_to(out_dir)
                if len(rel.parts) >= 2:
                    chans.add(rel.parts[0])
            except Exception:
                pass
        if len(chans) == 1:
            channel_part = _slugify(list(chans)[0], maxlen=30)

    domain_part = None
    if not channel_part:
        ds = _domains_from_urls(urls)
        if len(ds) == 1:
            domain_part = _slugify(ds[0].split(":")[0], maxlen=20)
        elif len(ds) > 1:
            domain_part = "multi"

    prefix = channel_part or domain_part or "media"
    if kind == "audio":
        spec = f"{audio_format}{int(abr_kbps)}kbps"
    elif kind == "imagen":
        spec = "images"
    else:
        spec = f"{int(max_height)}p-{container}"
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    base_name = f"{prefix}_{kind}_{spec}_{n}x_{timestamp}"

    zip_base = out_dir.parent / base_name
    zip_path = shutil.make_archive(str(zip_base), "zip", out_dir)
    return zip_path, generated, failures
