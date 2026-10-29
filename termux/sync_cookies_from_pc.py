#!/usr/bin/env python3
"""
sync_cookies_from_pc.py — Extrae cookies de tu navegador en PC y las sincroniza (Opción B)
==========================================================================================
Extrae automáticamente las sesiones activas de Instagram, YouTube, TikTok, X/Twitter,
Pinterest y Facebook desde Brave / Chrome / Firefox en tu PC, guarda las cookies locales
y las envía a tu Termux (vía HTTP POST /cookies o SCP).
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import requests
from yt_dlp.cookies import YoutubeDLCookieJar, extract_cookies_from_browser

REPO_ROOT = Path(__file__).resolve().parent.parent

DOMAINS = (
    "instagram.com",
    "youtube.com",
    "google.com",
    "tiktok.com",
    "x.com",
    "twitter.com",
    "pinterest.com",
    "facebook.com",
)

BROWSER_CANDIDATES = [
    ("brave", Path.home() / ".var/app/com.brave.Browser/config/BraveSoftware/Brave-Browser"),
    ("brave", Path.home() / ".config/BraveSoftware/Brave-Browser"),
    ("chrome", Path.home() / ".var/app/com.google.Chrome/config/google-chrome"),
    ("chrome", Path.home() / ".config/google-chrome"),
    ("chromium", Path.home() / ".var/app/org.chromium.Chromium/config/chromium"),
    ("firefox", Path.home() / ".var/app/org.mozilla.firefox/.mozilla/firefox"),
    ("firefox", Path.home() / ".mozilla/firefox"),
]


def _save_filtered_jar(source_jar, dest: Path, domain_filters: tuple[str, ...]) -> int:
    dest.parent.mkdir(parents=True, exist_ok=True)
    out_jar = YoutubeDLCookieJar(str(dest))
    count = 0
    for c in source_jar:
        if any(d in (c.domain or "") for d in domain_filters):
            out_jar.set_cookie(c)
            count += 1
    out_jar.save(ignore_discard=True, ignore_expires=True)
    return count


def main() -> int:
    found_jar = None
    used_browser = ""
    for b_name, b_path in BROWSER_CANDIDATES:
        if not b_path.exists():
            continue
        try:
            jar = extract_cookies_from_browser(b_name, profile=str(b_path))
            if any("instagram.com" in (c.domain or "") and c.name == "sessionid" for c in jar):
                found_jar = jar
                used_browser = f"{b_name} ({b_path})"
                break
            if found_jar is None and len(jar) > 0:
                found_jar = jar
                used_browser = f"{b_name} ({b_path})"
        except Exception:
            continue

    if found_jar is None:
        print("❌ No se encontraron cookies en los navegadores de la PC.")
        return 1

    # 1. Guardar archivo combinado para Termux
    out_path = Path("/tmp/muvixo_cookies.txt")
    count = _save_filtered_jar(found_jar, out_path, DOMAINS)
    print(f"✅ Extraídas {count} cookies desde {used_browser} ({out_path.stat().st_size} bytes)")

    # 2. Guardar también en la estructura local de Muvixo (cookies/<platform>/current.txt y www.<platform>.com_cookies.txt)
    _save_filtered_jar(found_jar, REPO_ROOT / "cookies" / "instagram" / "current.txt", ("instagram.com", "facebook.com"))
    _save_filtered_jar(found_jar, REPO_ROOT / "cookies" / "youtube" / "current.txt", ("youtube.com", "google.com"))
    _save_filtered_jar(found_jar, REPO_ROOT / "cookies" / "tiktok" / "current.txt", ("tiktok.com",))
    _save_filtered_jar(found_jar, REPO_ROOT / "www.instagram.com_cookies.txt", ("instagram.com", "facebook.com"))
    _save_filtered_jar(found_jar, REPO_ROOT / "www.youtube.com_cookies.txt", ("youtube.com", "google.com"))
    _save_filtered_jar(found_jar, REPO_ROOT / "www.tiktok.com_cookies.txt", ("tiktok.com",))
    print("📂 Actualizadas cookies locales en ./cookies/ y ./www.*_cookies.txt")

    # 3. Intentar sincronizar con Termux vía HTTP (/cookies) o SCP
    worker_url = os.getenv("MEDIA_WORKER_URL", "http://100.95.206.57:5001").rstrip("/")
    try:
        r = requests.post(
            f"{worker_url}/cookies",
            json={"cookies_text": out_path.read_text(encoding="utf-8")},
            timeout=5,
        )
        if r.status_code == 200 and (r.json() or {}).get("ok"):
            print(f"🎉 ¡Cookies sincronizadas en Termux vía HTTP ({worker_url}/cookies)!")
            return 0
    except Exception:
        pass

    target = sys.argv[1] if len(sys.argv) > 1 else "termux:./cookies.txt"
    print(f"📤 Enviando cookies por SCP a {target} ...")
    proc = subprocess.run(["scp", str(out_path), target], check=False)
    if proc.returncode == 0:
        print("🎉 ¡Cookies sincronizadas en tu Termux (~/cookies.txt)!")
        return 0

    print(f"⚠️ El archivo combinado quedó listo en {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
