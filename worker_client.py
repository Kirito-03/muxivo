"""
worker_client.py — Cliente del worker de Termux (worker_universal.py)
======================================================================
Lee las variables de entorno:
  MEDIA_WORKER_URL       URL base del worker, ej: http://100.70.78.80:5001
  MEDIA_WORKER_TOKEN     Token Bearer opcional
  MEDIA_WORKER_ENABLED   "0" para deshabilitar (default: habilitado si hay URL)
  MEDIA_WORKER_TIMEOUT   Timeout HTTP en segundos (default: 30)

  TIKTOK_PHOTO_WORKER_URL    Alias legacy (usa MEDIA_WORKER_URL si no está)
  TIKTOK_PHOTO_WORKER_TOKEN  Alias legacy

Expone las funciones que app.py importa:
  worker_enabled()
  is_blocking_error(msg)
  call_worker_extract(url, timeout)
  call_worker_download(url, kind, fmt, quality, timeout)
  download_worker_files_to_local(files, out_dir, timeout)
  worker_extract_tiktok_photos(url, timeout)
  worker_extract_instagram(url, timeout)
  worker_download_youtube(url, kind, fmt, quality, timeout)
  worker_download_instagram(url, kind, fmt, quality, timeout)
  worker_download_tiktok(url, kind, fmt, quality, timeout)
  log_worker_status()
"""
from __future__ import annotations

import json
import os
import shutil
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


# ---------------------------------------------------------------------------
# Configuración desde entorno
# ---------------------------------------------------------------------------

def _worker_base() -> str:
    """Devuelve la URL base del worker sin trailing slash, o '' si no está configurado."""
    url = (
        os.environ.get("MEDIA_WORKER_URL")
        or os.environ.get("TIKTOK_PHOTO_WORKER_URL")
        or ""
    ).strip().rstrip("/")
    return url


def _worker_token() -> str:
    return (
        os.environ.get("MEDIA_WORKER_TOKEN")
        or os.environ.get("TIKTOK_PHOTO_WORKER_TOKEN")
        or ""
    ).strip()


def _worker_timeout() -> int:
    try:
        return max(5, int(os.environ.get("MEDIA_WORKER_TIMEOUT") or "30"))
    except Exception:
        return 30


def worker_enabled() -> bool:
    """True si el worker está configurado y no fue deshabilitado explícitamente."""
    enabled_env = (os.environ.get("MEDIA_WORKER_ENABLED") or "1").strip().lower()
    if enabled_env in ("0", "false", "no", "off"):
        return False
    return bool(_worker_base())


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def _post_json(endpoint: str, payload: Dict[str, Any], timeout: int) -> Dict[str, Any]:
    """Realiza POST JSON al worker. Lanza excepción si falla."""
    data = json.dumps(payload).encode("utf-8")
    headers: Dict[str, str] = {"Content-Type": "application/json"}
    token = _worker_token()
    if token:
        headers["Authorization"] = f"Bearer {token}"

    req = urllib.request.Request(endpoint, data=data, headers=headers, method="POST")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = resp.read()
    result = json.loads(body)
    if not isinstance(result, dict):
        raise ValueError(f"Worker returned non-dict: {body[:200]}")
    return result


def _get_json(endpoint: str, timeout: int) -> Dict[str, Any]:
    """Realiza GET al worker. Lanza excepción si falla."""
    headers: Dict[str, str] = {}
    token = _worker_token()
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(endpoint, headers=headers, method="GET")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = resp.read()
    result = json.loads(body)
    if not isinstance(result, dict):
        raise ValueError(f"Worker returned non-dict: {body[:200]}")
    return result


# ---------------------------------------------------------------------------
# Detección de errores bloqueantes (para decidir si llamar al worker)
# ---------------------------------------------------------------------------

_BLOCKING_MARKERS = (
    "sign in",
    "login required",
    "please log in",
    "checkpoint",
    "cookie",
    "consent",
    "private video",
    "requires login",
    "not available",
    "403",
    "http error 403",
    "http error 429",
    "too many requests",
    "impersonation",
    "no impersonate",
    "unsupported url",
    "this video is private",
    "tiktok requires",
    "bot",
    "captcha",
    "rate limit",
)


def is_blocking_error(msg: str) -> bool:
    """True si el mensaje de error sugiere bloqueo por plataforma (candidato a worker fallback)."""
    low = (msg or "").lower()
    return any(m in low for m in _BLOCKING_MARKERS)


# ---------------------------------------------------------------------------
# /extract — extrae imágenes de una galería (TikTok, Instagram, Facebook)
# ---------------------------------------------------------------------------

def call_worker_extract(url: str, timeout: int = 25) -> List[Dict[str, str]]:
    """
    Llama a POST /extract en el worker.
    Devuelve lista de items [{url, thumb, label, kind}] o [] en caso de fallo.
    """
    base = _worker_base()
    if not base:
        return []
    endpoint = f"{base}/extract"
    print(f"[WORKER-CLIENT] extract url={url} endpoint={endpoint}", flush=True)
    try:
        result = _post_json(endpoint, {"url": url}, timeout=timeout)
        if not result.get("ok"):
            print(f"[WORKER-CLIENT] extract ok=false msg={result.get('message', '')}", flush=True)
            return []
        items = result.get("items") or []
        if not isinstance(items, list):
            return []
        print(f"[WORKER-CLIENT] extract items={len(items)}", flush=True)
        return [it for it in items if isinstance(it, dict) and it.get("url")]
    except Exception as exc:
        print(f"[WORKER-CLIENT] extract failed: {exc}", flush=True)
        return []


# ---------------------------------------------------------------------------
# /download — descarga audio/video
# ---------------------------------------------------------------------------

def call_worker_download(
    url: str,
    kind: str = "video",
    fmt: str = "mp4",
    quality: str = "720",
    timeout: int = 90,
) -> List[Dict[str, Any]]:
    """
    Llama a POST /download en el worker.
    Devuelve lista de files [{name, url, kind, size, ext}] o [] en caso de fallo.
    """
    base = _worker_base()
    if not base:
        return []
    endpoint = f"{base}/download"
    payload = {"url": url, "kind": kind, "format": fmt, "quality": quality}
    print(f"[WORKER-CLIENT] download url={url} kind={kind} fmt={fmt} q={quality}", flush=True)
    try:
        result = _post_json(endpoint, payload, timeout=timeout)
        if not result.get("ok"):
            print(f"[WORKER-CLIENT] download ok=false msg={result.get('message', '')}", flush=True)
            return []
        files = result.get("files") or []
        if not isinstance(files, list):
            return []
        print(f"[WORKER-CLIENT] download files={len(files)}", flush=True)
        return [f for f in files if isinstance(f, dict) and f.get("url")]
    except Exception as exc:
        print(f"[WORKER-CLIENT] download failed: {exc}", flush=True)
        return []


# ---------------------------------------------------------------------------
# Helpers de plataforma (aliases que usa app.py)
# ---------------------------------------------------------------------------

def worker_extract_tiktok_photos(url: str, timeout: int = 20) -> List[Dict[str, str]]:
    """Extrae imágenes de una galería TikTok (/photo/) via worker."""
    return call_worker_extract(url, timeout=timeout)


def worker_extract_instagram(url: str, timeout: int = 25) -> List[Dict[str, str]]:
    """Extrae imágenes de un carousel de Instagram via worker."""
    return call_worker_extract(url, timeout=timeout)


def worker_download_youtube(
    url: str,
    kind: str = "video",
    fmt: str = "mp4",
    quality: str = "720",
    timeout: int = 90,
) -> List[Dict[str, Any]]:
    """Descarga video/audio de YouTube via worker."""
    return call_worker_download(url, kind=kind, fmt=fmt, quality=quality, timeout=timeout)


def worker_download_instagram(
    url: str,
    kind: str = "video",
    fmt: str = "mp4",
    quality: str = "720",
    timeout: int = 60,
) -> List[Dict[str, Any]]:
    """Descarga video/audio de Instagram via worker."""
    return call_worker_download(url, kind=kind, fmt=fmt, quality=quality, timeout=timeout)


def worker_download_tiktok(
    url: str,
    kind: str = "video",
    fmt: str = "mp4",
    quality: str = "720",
    timeout: int = 60,
) -> List[Dict[str, Any]]:
    """Descarga video/audio de TikTok via worker."""
    return call_worker_download(url, kind=kind, fmt=fmt, quality=quality, timeout=timeout)


# ---------------------------------------------------------------------------
# Descarga de archivos del worker al servidor local
# ---------------------------------------------------------------------------

def download_worker_files_to_local(
    files: List[Dict[str, Any]],
    out_dir: Path,
    timeout: int = 90,
) -> Tuple[List[Path], List[Tuple[str, str]]]:
    """
    Descarga los archivos del worker (que están en el celular) al servidor local.

    Parámetros:
      files    — lista de {url, name, ...} devuelta por el worker
      out_dir  — directorio local donde guardar los archivos
      timeout  — timeout HTTP por archivo

    Devuelve: (paths_ok, failures)
      paths_ok  — lista de Path de archivos descargados correctamente
      failures  — lista de (name, reason) para los que fallaron
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    paths_ok: List[Path] = []
    failures: List[Tuple[str, str]] = []

    token = _worker_token()
    headers: Dict[str, str] = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    for f in files:
        if not isinstance(f, dict):
            continue
        file_url = str(f.get("url") or "").strip()
        file_name = str(f.get("name") or "").strip()
        if not file_url or not file_name:
            print(f"[WORKER-CLIENT] skip file: no url or name: {f}", flush=True)
            continue

        # Sanitizar nombre
        safe_name = Path(file_name).name
        if not safe_name:
            safe_name = f"worker_file_{os.urandom(4).hex()}"
        dest = (out_dir / safe_name).resolve()

        # Evitar colisiones
        if dest.exists():
            stem = dest.stem
            ext = dest.suffix
            dest = (out_dir / f"{stem}_{os.urandom(3).hex()}{ext}").resolve()

        tmp = dest.with_suffix(dest.suffix + f".tmp{os.urandom(3).hex()}")
        print(f"[WORKER-CLIENT] downloading {file_url} -> {dest.name}", flush=True)
        try:
            req = urllib.request.Request(file_url, headers=headers, method="GET")
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                with open(tmp, "wb") as fh:
                    shutil.copyfileobj(resp, fh)

            if not tmp.exists() or tmp.stat().st_size < 100:
                try:
                    tmp.unlink(missing_ok=True)  # type: ignore[arg-type]
                except Exception:
                    pass
                reason = "Archivo vacío o muy pequeño"
                print(f"[WORKER-CLIENT] {file_name} FAILED: {reason}", flush=True)
                failures.append((file_name, reason))
                continue

            tmp.replace(dest)
            paths_ok.append(dest)
            print(f"[WORKER-CLIENT] {dest.name} ok size={dest.stat().st_size}", flush=True)

        except (urllib.error.URLError, urllib.error.HTTPError) as exc:
            try:
                tmp.unlink(missing_ok=True)  # type: ignore[arg-type]
            except Exception:
                pass
            reason = f"HTTP error: {exc}"
            print(f"[WORKER-CLIENT] {file_name} FAILED: {reason}", flush=True)
            failures.append((file_name, reason))

        except Exception as exc:
            try:
                tmp.unlink(missing_ok=True)  # type: ignore[arg-type]
            except Exception:
                pass
            reason = f"{type(exc).__name__}: {exc}"
            print(f"[WORKER-CLIENT] {file_name} FAILED: {reason}", flush=True)
            failures.append((file_name, reason))

    return paths_ok, failures


# ---------------------------------------------------------------------------
# log_worker_status — diagnóstico en startup
# ---------------------------------------------------------------------------

def log_worker_status() -> None:
    """
    Imprime el estado de conexión con el worker al arrancar el servidor.
    Llama a GET /health del worker para verificar conectividad.
    """
    base = _worker_base()
    if not base:
        print("[WORKER-CLIENT] Worker NOT configured (MEDIA_WORKER_URL not set)", flush=True)
        return

    enabled = worker_enabled()
    print(f"[WORKER-CLIENT] Worker URL={base} enabled={enabled}", flush=True)
    if not enabled:
        return

    try:
        result = _get_json(f"{base}/health", timeout=8)
        ytdlp = result.get("yt_dlp", False)
        ytdlp_ver = result.get("yt_dlp_version", "?")
        ffmpeg = result.get("ffmpeg", False)
        caps = result.get("capabilities") or {}
        print(
            f"[WORKER-CLIENT] Worker REACHABLE — "
            f"yt-dlp={ytdlp}({ytdlp_ver}) ffmpeg={ffmpeg} caps={caps}",
            flush=True,
        )
    except Exception as exc:
        print(f"[WORKER-CLIENT] Worker UNREACHABLE at {base}/health: {exc}", flush=True)
