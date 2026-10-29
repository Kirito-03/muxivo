#!/usr/bin/env python3
"""
ig_login.py — Generador de cookies.txt directo en Termux (Opción A)
===================================================================
Permite autenticar tu cuenta de Instagram directamente desde Termux (con la IP
móvil de tu celular) y guarda ~/cookies.txt en formato Netscape para que tanto
el Worker como el botón "Compartir -> Termux" descarguen posts privados o +18.

Uso en Termux:
  python ~/ig_login.py
"""
from __future__ import annotations

import getpass
import http.cookiejar
import sys
import time
from pathlib import Path
from urllib.parse import unquote

import requests

UA_MOBILE = (
    "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36"
)
IG_APP_ID = "936619743392459"


def _save_requests_cookies_to_netscape(session: requests.Session, dest: Path) -> None:
    """Guarda las cookies de la sesión en formato Netscape (conservando cookies de otras plataformas si existen)."""
    jar = http.cookiejar.MozillaCookieJar(str(dest))
    if dest.exists():
        try:
            jar.load(ignore_discard=True, ignore_expires=True)
        except Exception:
            pass

    for c in session.cookies:
        domain = c.domain or ".instagram.com"
        if not domain.startswith(".") and "instagram.com" in domain:
            domain = "." + domain
        cookie = http.cookiejar.Cookie(
            version=0,
            name=c.name,
            value=c.value,
            port=None,
            port_specified=False,
            domain=domain,
            domain_specified=True,
            domain_initial_dot=domain.startswith("."),
            path=c.path or "/",
            path_specified=True,
            secure=bool(c.secure),
            expires=int(c.expires) if c.expires else int(time.time()) + 365 * 86400,
            discard=False,
            comment=None,
            comment_url=None,
            rest={},
            rfc2109=False,
        )
        jar.set_cookie(cookie)

    jar.save(ignore_discard=True, ignore_expires=True)


def _verify_session(session: requests.Session) -> bool:
    try:
        r = session.get(
            "https://www.instagram.com/api/v1/feed/saved/posts/",
            headers={
                "User-Agent": UA_MOBILE,
                "X-IG-App-ID": IG_APP_ID,
                "X-Requested-With": "XMLHttpRequest",
            },
            timeout=12,
        )
        return r.status_code == 200 and "items" in (r.json() or {})
    except Exception:
        return False


def login_with_credentials(dest: Path) -> int:
    print("\n--- Login directo con Usuario y Contraseña ---")
    username = input("👤 Usuario de Instagram: ").strip()
    if not username:
        print("❌ Usuario vacío.")
        return 1
    password = getpass.getpass("🔑 Contraseña de Instagram: ").strip()
    if not password:
        print("❌ Contraseña vacía.")
        return 1

    s = requests.Session()
    s.headers.update({
        "User-Agent": UA_MOBILE,
        "X-IG-App-ID": IG_APP_ID,
        "X-Requested-With": "XMLHttpRequest",
        "Referer": "https://www.instagram.com/accounts/login/",
        "Origin": "https://www.instagram.com",
    })

    print("⏳ Conectando con Instagram desde tu IP móvil...")
    s.get("https://www.instagram.com/accounts/login/", timeout=15)
    csrf = s.cookies.get("csrftoken") or "missing"
    s.headers["X-CSRFToken"] = csrf

    ts = int(time.time())
    payload = {
        "enc_password": f"#PWD_INSTAGRAM_BROWSER:0:{ts}:{password}",
        "username": username,
        "queryParams": "{}",
        "optIntoOneTap": "false",
    }
    r = s.post("https://www.instagram.com/api/v1/web/accounts/login/ajax/", data=payload, timeout=15)
    try:
        data = r.json()
    except Exception:
        print(f"❌ Respuesta inesperada (HTTP {r.status_code}). Usa la opción [2] (sessionid).")
        return 1

    if data.get("two_factor_required"):
        tf_info = data.get("two_factor_info") or {}
        identifier = tf_info.get("two_factor_identifier")
        code = input("📲 Ingresa el código 2FA (WhatsApp/SMS/App): ").strip()
        r2 = s.post(
            "https://www.instagram.com/api/v1/web/accounts/login/ajax/two_factor/",
            data={
                "username": username,
                "verificationCode": code,
                "identifier": identifier,
                "queryParams": "{}",
            },
            timeout=15,
        )
        data = r2.json() if r2.text else {}

    if data.get("authenticated") or "sessionid" in s.cookies:
        _save_requests_cookies_to_netscape(s, dest)
        print(f"\n✅ ¡Sesión iniciada con éxito! Cookies guardadas en: {dest}")
        return 0

    msg = data.get("message") or "No se pudo autenticar (verifica contraseña o checkpoint en la app)."
    print(f"\n❌ Error de login: {msg}")
    print("💡 Tip: Si tu cuenta tiene protección extra, elige la opción [2] y pega tu sessionid.")
    return 1


def login_with_sessionid(dest: Path) -> int:
    print("\n--- Configurar mediante cookie 'sessionid' ---")
    raw_sid = input("🍪 Pega el valor de 'sessionid': ").strip().strip('"').strip("'")
    if not raw_sid:
        print("❌ Valor vacío.")
        return 1

    s = requests.Session()
    s.cookies.set("sessionid", raw_sid, domain=".instagram.com", path="/")
    uid = unquote(raw_sid).split(":")[0]
    if uid.isdigit():
        s.cookies.set("ds_user_id", uid, domain=".instagram.com", path="/")

    print("⏳ Verificando sesión con Instagram...")
    if _verify_session(s):
        _save_requests_cookies_to_netscape(s, dest)
        print(f"\n✅ ¡Sesión válida y verificada! Guardada en: {dest}")
        return 0

    _save_requests_cookies_to_netscape(s, dest)
    print(f"\n⚠️ Se guardó en {dest}, aunque Instagram no confirmó la sesión.")
    return 0


def main() -> int:
    dest = Path.home() / "cookies.txt"
    print("==============================================")
    print("   🔐 MUVIXO — LOGIN DE INSTAGRAM EN TERMUX   ")
    print("==============================================")
    print(f"Archivo destino: {dest}")
    print("")
    print("  [1] Iniciar sesión con Usuario y Contraseña (Recomendado)")
    print("  [2] Pegar 'sessionid' manualmente")
    print("")
    choice = input("Selecciona [1/2] (Enter = 1): ").strip() or "1"
    if choice == "2":
        return login_with_sessionid(dest)
    return login_with_credentials(dest)


if __name__ == "__main__":
    sys.exit(main())
