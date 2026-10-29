#!/usr/bin/env python3
"""
share_download.py — Descargador directo para Android "Compartir -> Termux" (Muvixo)
===================================================================================
Usa la misma lógica de extracción de worker.py (incluyendo fotos/carruseles de
Instagram, TikTok Photo, Pinterest, X/Twitter y videos/audios con yt-dlp + cookies)
y guarda los archivos directamente en /sdcard/Download/Muvixo, actualizando la
Galería de Android con termux-media-scan.
"""
from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path
from urllib.parse import urlparse

# Asegurar que podamos importar worker.py desde la raíz del repositorio
REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import worker as wk  # noqa: E402


def _extract_first_url(raw: str) -> str:
    """Extrae la primera URL http/https limpia (apps como Pinterest o X agregan texto al compartir)."""
    m = re.search(r"https?://[^\s\"'<>]+", str(raw or ""))
    if not m:
        return ""
    return m.group(0).rstrip(".,;!)>\"'")


def _resolve_output_dir() -> Path:
    """Busca la carpeta Descargas de Android (/sdcard/Download/Muvixo)."""
    candidates = [
        Path("/sdcard/Download/Muvixo"),
        Path.home() / "storage" / "downloads" / "Muvixo",
        Path("/storage/emulated/0/Download/Muvixo"),
        REPO_ROOT / "downloads" / "Muvixo",
    ]
    for c in candidates:
        try:
            if c.parent.exists():
                c.mkdir(parents=True, exist_ok=True)
                return c
        except Exception:
            continue
    fallback = REPO_ROOT / "worker_downloads"
    fallback.mkdir(parents=True, exist_ok=True)
    return fallback


def _scan_media(files: list[Path]) -> None:
    """Notifica a la Galería de Android para que las fotos/videos aparezcan al instante."""
    if not files:
        return
    scanner = shutil.which("termux-media-scan")
    if scanner:
        try:
            subprocess.run([scanner] + [str(f) for f in files], timeout=10, check=False)
            return
        except Exception:
            pass
    # Fallback con broadcast de Android si am está disponible
    am = shutil.which("am")
    if am:
        for f in files:
            try:
                subprocess.run(
                    [
                        am, "broadcast",
                        "-a", "android.intent.action.MEDIA_SCANNER_SCAN_FILE",
                        "-d", f"file://{f}",
                    ],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    timeout=5,
                    check=False,
                )
            except Exception:
                pass


def _toast(msg: str) -> None:
    toast_bin = shutil.which("termux-toast")
    if toast_bin:
        try:
            subprocess.run([toast_bin, "-g", "bottom", msg], timeout=4, check=False)
        except Exception:
            pass


def _download_images_to_gallery(url: str, out_dir: Path, platform: str) -> list[Path]:
    img_urls = wk._extract_image_urls(url)
    if not img_urls:
        return []
    print(f"📸 Se detectaron {len(img_urls)} imagen(es) de {platform.capitalize()}. Descargando...")
    saved: list[Path] = []
    ts = int(time.time())
    for idx, img_u in enumerate(img_urls[:35], start=1):
        ext = Path(urlparse(img_u).path).suffix.lstrip(".").lower()
        if ext not in wk._IMAGE_EXTS:
            ext = "jpg"
        fname = f"Muvixo_{platform}_{ts}_{idx:02d}.{ext}"
        dest = out_dir / fname
        res = wk._download_binary(img_u, dest)
        if res and res.exists():
            print(f"  ✅ Guardada: {res.name} ({res.stat().st_size // 1024} KB)")
            saved.append(res)
    return saved


def _download_ytdlp_to_gallery(url: str, out_dir: Path, mode: str, platform: str) -> list[Path]:
    if platform == "pinterest":
        url = wk._normalize_pinterest_url(url)
    elif platform == "youtube":
        url = wk.normalize_youtube_url(url) or url

    _, _, ffmpeg_ok = wk._detect_tools()
    out_tmpl = str(out_dir / "%(title).100B [%(id)s].%(ext)s")

    cmd = [
        sys.executable, "-m", "yt_dlp",
        "--no-playlist", "--no-warnings",
        "--restrict-filenames",
        "--socket-timeout", "30",
        "--retries", "3",
        "-o", out_tmpl,
    ] + wk._cookies_args()

    if mode == "audio":
        cmd += ["-f", "bestaudio[ext=m4a]/bestaudio/best"]
        if ffmpeg_ok:
            cmd += ["--extract-audio", "--audio-format", "mp3", "--audio-quality", "0"]
    else:
        if ffmpeg_ok:
            cmd += [
                "-f", "bv*[ext=mp4]+ba[ext=m4a]/bv*+ba/b[ext=mp4]/best",
                "--merge-output-format", "mp4",
            ]
        else:
            cmd += ["-f", "b[ext=mp4]/best[ext=mp4]/best"]

    before = {f.name for f in out_dir.iterdir() if f.is_file()} if out_dir.exists() else set()
    cutoff = time.time() - 2

    proc = subprocess.run(cmd + [url], check=False)
    if proc.returncode != 0 and platform == "youtube":
        print("⚠️ Reintentando YouTube con cliente TV Embedded...")
        proc = subprocess.run(
            cmd + ["--extractor-args", "youtube:player_client=tv_embedded,web", url],
            check=False,
        )

    if proc.returncode != 0:
        return []

    after_files = [
        f for f in out_dir.iterdir()
        if f.is_file() and (f.name not in before or f.stat().st_mtime >= cutoff)
        and not wk._is_temp_file(f)
    ]
    return sorted(after_files, key=lambda p: p.stat().st_mtime, reverse=True)


def main() -> int:
    raw_arg = " ".join(sys.argv[1:]).strip()
    mode = "auto"
    if raw_arg.startswith("--audio "):
        mode = "audio"
        raw_arg = raw_arg[len("--audio "):].strip()
    elif raw_arg.startswith("--video "):
        mode = "video"
        raw_arg = raw_arg[len("--video "):].strip()

    url = _extract_first_url(raw_arg)
    if not url:
        print("❌ No se encontró un enlace válido en el texto compartido.")
        return 1

    out_dir = _resolve_output_dir()
    platform = wk.detect_platform(url)
    cookies_file = wk._resolve_cookies_path()

    print("==========================================")
    print("🚀 Muvixo Mobile — Descarga Directa")
    print(f"🔗 Enlace:     {url}")
    print(f"🌐 Plataforma: {platform.upper()}")
    print(f"🍪 Cookies:    {cookies_file.name if cookies_file else 'No configuradas (modo público)'}")
    print(f"📂 Destino:    {out_dir}")
    print("==========================================")

    saved_files: list[Path] = []

    # 1. Si el usuario pidió audio explícitamente -> yt-dlp audio
    if mode == "audio":
        print("🎵 Descargando Audio MP3...")
        saved_files = _download_ytdlp_to_gallery(url, out_dir, "audio", platform)
    else:
        # 2. Si es un link evidente de fotos/galería (TikTok /photo/, Instagram /p/, imagen directa)
        path_low = (urlparse(url).path or "").lower()
        is_ig_post = platform == "instagram" and "/p/" in path_low and "/reel/" not in path_low
        is_tiktok_photo = platform == "tiktok" and "/photo/" in path_low

        if wk.is_direct_image_url(url) or is_tiktok_photo:
            saved_files = _download_images_to_gallery(url, out_dir, platform)
        elif is_ig_post:
            # En Instagram /p/ puede ser foto, carrusel o video. Probamos video/yt-dlp y galería.
            print("🔍 Analizando publicación de Instagram...")
            saved_files = _download_images_to_gallery(url, out_dir, platform)
            if not saved_files:
                saved_files = _download_ytdlp_to_gallery(url, out_dir, "video", platform)
        else:
            # Intentar primero como video con yt-dlp; si falla (ej. Pin de imagen o Tweet de foto), extraer imágenes
            print("🎬 Intentando descarga de video en máxima calidad...")
            saved_files = _download_ytdlp_to_gallery(url, out_dir, "video", platform)
            if not saved_files:
                print("🖼️ No es video o requiere extractor de galería, buscando imágenes...")
                saved_files = _download_images_to_gallery(url, out_dir, platform)

    if saved_files:
        _scan_media(saved_files)
        print("\n✅ ¡Descarga completada!")
        for f in saved_files:
            print(f"   📁 {f}")
        _toast(f"Muvixo: {len(saved_files)} archivo(s) guardado(s) en Galería")
        return 0

    print("\n❌ No se pudo descargar el contenido.")
    if not cookies_file and platform in ("instagram", "tiktok", "youtube"):
        print("💡 Tip: Si el post es privado o pide iniciar sesión, guarda tus cookies en ~/cookies.txt")
    _toast("Muvixo: Error al descargar el enlace")
    return 1


if __name__ == "__main__":
    sys.exit(main())
