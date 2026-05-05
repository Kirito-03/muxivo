# worker_client.py — Cliente para el worker Termux via Tailscale
# Soporta extract (TikTok/Instagram) y download (YouTube/Instagram)
# con fallback seguro si el worker está apagado o no responde.
from __future__ import annotations

import json
import os
import shutil
import ssl
import time
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse


# ---------------------------------------------------------------------------
# Configuración desde variables de entorno
# ---------------------------------------------------------------------------

def _worker_base_url() -> str:
    """URL base del worker. Soporta MEDIA_WORKER_URL y legacy TIKTOK_PHOTO_WORKER_URL."""
    return (
        os.environ.get("MEDIA_WORKER_URL")
        or os.environ.get("TIKTOK_PHOTO_WORKER_URL")
        or ""
    ).strip().rstrip("/")


def _worker_token() -> str:
    return (
        os.environ.get("MEDIA_WORKER_TOKEN")
        or os.environ.get("TIKTOK_PHOTO_WORKER_TOKEN")
        or ""
    ).strip()


def _worker_timeout() -> int:
    try:
        return max(5, int(os.environ.get("MEDIA_WORKER_TIMEOUT", "30")))
    except Exception:
        return 30


def worker_enabled() -> bool:
    """True si el worker está habilitado y tiene URL configurada."""
    raw = (os.environ.get("MEDIA_WORKER_ENABLED", "1") or "").strip().lower()
    if raw in ("0", "false", "no", "off"):
        return False
    return bool(_worker_base_url())


def _log(msg: str) -> None:
    try:
        print(f"[WORKER] {msg}", flush=True)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Detección de errores de bloqueo
# ---------------------------------------------------------------------------

_BLOCKING_MARKERS = [
    # YouTube
    "sign in to confirm you're not a bot",
    "sign in to confirm you\u2019re not a bot",
    "confirm you're not a bot",
    "cookies are no longer valid",
    "cookie is no longer valid",
    "this browser or app may not be secure",
    "account verification required",
    "requested format is not available",
    "only images are available",
    "no video formats found",
    # Instagram
    "instagram api is not granting access",
    "instagram sent an empty media response",
    "empty media response",
    "login required",
    "please log in",
    "requires login",
    "checkpoint required",
    "http error 403",
    "forbidden",
    # Genérico
    "bot detection",
    "access denied",
    "captcha",
]


def is_blocking_error(error_text: str) -> bool:
    """Detecta si un error indica bloqueo/cookies/bot que justifica llamar al worker."""
    low = (error_text or "").lower()
    return any(m in low for m in _BLOCKING_MARKERS)


# ---------------------------------------------------------------------------
# POST /extract — para TikTok photo e Instagram gallery
# ---------------------------------------------------------------------------

def call_worker_extract(
    url: str,
    platform: str = "",
    timeout: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Llama POST /extract al worker. Retorna lista de items o [] si falla."""
    base = _worker_base_url()
    if not base or not worker_enabled():
        _log("unavailable -> local fallback")
        return []

    effective_timeout = timeout or _worker_timeout()
    endpoint = f"{base}/extract"
    payload = json.dumps({"url": str(url or "").strip()}).encode("utf-8")

    headers: Dict[str, str] = {
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    token = _worker_token()
    if token:
        headers["Authorization"] = f"Bearer {token}"

    _log(f"extract platform={platform or 'unknown'} url={url}")

    try:
        req = urllib.request.Request(endpoint, data=payload, headers=headers, method="POST")
        ctx = ssl.create_default_context()
        with urllib.request.urlopen(req, timeout=effective_timeout, context=ctx) as resp:
            raw = resp.read(5_000_000)
        data = json.loads(raw.decode("utf-8", errors="ignore") or "{}")
    except Exception as e:
        _log(f"extract error: {type(e).__name__}: {e}")
        _log("unavailable -> local fallback")
        return []

    if not isinstance(data, dict) or not data.get("ok"):
        _log(f"extract response not ok: {data.get('message', '')}")
        return []

    items = data.get("items")
    if not isinstance(items, list) or not items:
        _log("extract items=0")
        return []

    # Validar que cada item tenga URL válida
    valid: List[Dict[str, Any]] = []
    for it in items:
        if not isinstance(it, dict):
            continue
        u = str(it.get("url") or "").strip()
        if not u.startswith(("http://", "https://")):
            continue
        valid.append(it)

    _log(f"extract items={len(valid)}")
    return valid


# ---------------------------------------------------------------------------
# POST /download — para YouTube video/audio e Instagram
# ---------------------------------------------------------------------------

def call_worker_download(
    url: str,
    kind: str = "video",
    fmt: str = "mp4",
    quality: str = "720",
    timeout: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Llama POST /download al worker. Retorna lista de files o [] si falla."""
    base = _worker_base_url()
    if not base or not worker_enabled():
        _log("unavailable -> local fallback")
        return []

    effective_timeout = timeout or _worker_timeout()
    endpoint = f"{base}/download"
    payload_dict = {
        "url": str(url or "").strip(),
        "kind": str(kind or "video").strip().lower(),
        "format": str(fmt or "mp4").strip().lower(),
        "quality": str(quality or "best").strip(),
    }
    payload = json.dumps(payload_dict).encode("utf-8")

    headers: Dict[str, str] = {
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    token = _worker_token()
    if token:
        headers["Authorization"] = f"Bearer {token}"

    platform = "unknown"
    try:
        host = (urlparse(url).netloc or "").lower()
        if "youtube" in host or "youtu.be" in host:
            platform = "youtube"
        elif "tiktok" in host:
            platform = "tiktok"
        elif "instagram" in host:
            platform = "instagram"
    except Exception:
        pass

    _log(f"download platform={platform} kind={kind} quality={quality}")

    try:
        req = urllib.request.Request(endpoint, data=payload, headers=headers, method="POST")
        ctx = ssl.create_default_context()
        with urllib.request.urlopen(req, timeout=effective_timeout, context=ctx) as resp:
            raw = resp.read(5_000_000)
        data = json.loads(raw.decode("utf-8", errors="ignore") or "{}")
    except Exception as e:
        _log(f"download error: {type(e).__name__}: {e}")
        _log("unavailable -> local fallback")
        return []

    if not isinstance(data, dict) or not data.get("ok"):
        _log(f"download response not ok: {data.get('message', '')}")
        return []

    files = data.get("files")
    if not isinstance(files, list) or not files:
        _log("download files=0")
        return []

    # Validar cada file
    valid: List[Dict[str, Any]] = []
    for f in files:
        if not isinstance(f, dict):
            continue
        u = str(f.get("url") or "").strip()
        if not u.startswith(("http://", "https://")):
            continue
        valid.append(f)

    _log(f"download files={len(valid)}")
    return valid


# ---------------------------------------------------------------------------
# Descargar archivos del worker al VPS (proxy de archivos)
# ---------------------------------------------------------------------------

def download_worker_files_to_local(
    worker_files: List[Dict[str, Any]],
    out_dir: Path,
    timeout: int = 60,
) -> Tuple[List[Path], List[Tuple[str, str]]]:
    """Descarga archivos del worker (URLs Tailscale) al directorio local del VPS.

    Retorna (generated_paths, failures).
    Los archivos quedan en out_dir, accesibles via muxivo.art.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    generated: List[Path] = []
    failures: List[Tuple[str, str]] = []

    token = _worker_token()
    headers_base: Dict[str, str] = {
        "User-Agent": "Muxivo-VPS/1.0",
        "Accept": "*/*",
    }
    if token:
        headers_base["Authorization"] = f"Bearer {token}"

    for f in worker_files:
        remote_url = str(f.get("url") or "").strip()
        name = str(f.get("name") or "").strip()
        if not remote_url:
            continue
        if not name:
            # Extraer nombre del URL
            try:
                from urllib.parse import unquote
                name = Path(unquote(urlparse(remote_url).path)).name or f"worker_file_{os.urandom(3).hex()}"
            except Exception:
                name = f"worker_file_{os.urandom(3).hex()}"

        # Sanitizar nombre local: reemplazar espacios y caracteres especiales
        import re as _re
        name = name.replace(" ", "_")
        name = _re.sub(r'[<>:"|?*()\[\]]', "", name)
        name = _re.sub(r"[^\w.\-]", "_", name)
        name = _re.sub(r"_+", "_", name).strip("_")
        if not name:
            name = f"worker_file_{os.urandom(3).hex()}"

        dest = (out_dir / name).resolve()
        # Evitar colisiones
        if dest.exists():
            stem = dest.stem
            suffix = dest.suffix
            dest = (out_dir / f"{stem}_{os.urandom(3).hex()}{suffix}").resolve()

        _log(f"downloading {remote_url} -> {dest.name}")

        try:
            req = urllib.request.Request(remote_url, headers=headers_base, method="GET")
            ctx = ssl.create_default_context()
            with urllib.request.urlopen(req, timeout=timeout, context=ctx) as resp:
                dest.parent.mkdir(parents=True, exist_ok=True)
                with open(dest, "wb") as fh:
                    shutil.copyfileobj(resp, fh)
                    fh.flush()

            # Validar que se descargó algo
            if dest.exists() and dest.stat().st_size > 100:
                generated.append(dest.resolve())
                _log(f"downloaded ok: {dest.name} ({dest.stat().st_size} bytes)")
            else:
                if dest.exists():
                    dest.unlink(missing_ok=True)
                failures.append((remote_url, "Archivo descargado vacío o inválido"))
                _log(f"downloaded empty: {remote_url}")
        except Exception as e:
            _log(f"download failed: {remote_url} -> {type(e).__name__}: {e}")
            failures.append((remote_url, f"Error al descargar: {type(e).__name__}"))
            try:
                if dest.exists():
                    dest.unlink(missing_ok=True)
            except Exception:
                pass

    return generated, failures


# ---------------------------------------------------------------------------
# Helpers de alto nivel para integración en media_tools / app
# ---------------------------------------------------------------------------

def worker_extract_tiktok_photos(url: str, timeout: int = 20) -> List[Dict[str, str]]:
    """Wrapper que llama extract y normaliza items para TikTok photo.
    Retorna lista de dicts con {url, label, kind}."""
    items = call_worker_extract(url, platform="tiktok", timeout=timeout)
    out: List[Dict[str, str]] = []
    for it in items:
        u = str(it.get("url") or "").strip()
        if not u:
            continue
        out.append({
            "url": u,
            "thumb": str(it.get("thumb") or "").strip() or u,
            "label": str(it.get("label") or "").strip() or f"IMAGE {len(out) + 1}",
            "kind": str(it.get("kind") or "image").strip().lower(),
        })
    return out


def worker_extract_instagram(url: str, timeout: int = 25) -> List[Dict[str, str]]:
    """Wrapper que llama extract y normaliza items para Instagram gallery."""
    items = call_worker_extract(url, platform="instagram", timeout=timeout)
    out: List[Dict[str, str]] = []
    for it in items:
        u = str(it.get("url") or "").strip()
        if not u:
            continue
        out.append({
            "url": u,
            "thumb": str(it.get("thumb") or "").strip() or u,
            "label": str(it.get("label") or "").strip() or f"IMAGE {len(out) + 1}",
            "kind": str(it.get("kind") or "image").strip().lower(),
        })
    return out


def worker_download_youtube(
    url: str,
    kind: str = "video",
    fmt: str = "mp4",
    quality: str = "720",
    timeout: int = 60,
) -> List[Dict[str, Any]]:
    """Wrapper para descargar YouTube via worker."""
    return call_worker_download(url, kind=kind, fmt=fmt, quality=quality, timeout=timeout)


def worker_download_instagram(
    url: str,
    kind: str = "video",
    fmt: str = "auto",
    quality: str = "best",
    timeout: int = 45,
) -> List[Dict[str, Any]]:
    """Wrapper para descargar Instagram via worker."""
    return call_worker_download(url, kind=kind, fmt=fmt, quality=quality, timeout=timeout)


# ---------------------------------------------------------------------------
# Log de estado al iniciar
# ---------------------------------------------------------------------------

def log_worker_status() -> None:
    """Imprime el estado del worker al log del servidor."""
    enabled = worker_enabled()
    base = _worker_base_url()
    _log(f"enabled={enabled} url={base or 'N/A'}")
    if enabled:
        timeout_val = _worker_timeout()
        has_token = bool(_worker_token())
        _log(f"timeout={timeout_val}s token={'yes' if has_token else 'no'}")
