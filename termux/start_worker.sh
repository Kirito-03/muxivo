#!/data/data/com.termux/files/usr/bin/bash
# =============================================================================
# start_worker.sh — Inicia el Worker de Muvixo en Termux (Puerto 5001)
# =============================================================================
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_DIR"

# 1. Activar wake-lock para evitar que Android duerma Termux al apagar pantalla
if command -v termux-wake-lock >/dev/null 2>&1; then
    termux-wake-lock
    echo "🔒 termux-wake-lock activado (Android no cerrará el worker en segundo plano)."
fi

# 2. Detectar cookies automáticamente
if [ -z "${WORKER_COOKIES_FILE:-}" ]; then
    if [ -f "$HOME/cookies.txt" ]; then
        export WORKER_COOKIES_FILE="$HOME/cookies.txt"
    elif [ -f "$REPO_DIR/cookies.txt" ]; then
        export WORKER_COOKIES_FILE="$REPO_DIR/cookies.txt"
    fi
fi

export LISTEN_HOST="${LISTEN_HOST:-0.0.0.0}"
export LISTEN_PORT="${LISTEN_PORT:-5001}"

# 3. Mostrar IP de Tailscale si está activa
echo "=================================================="
echo "🚀 Iniciando Muvixo Residential Worker (Termux)"
echo "📂 Directorio: $REPO_DIR"
if [ -n "${WORKER_COOKIES_FILE:-}" ]; then
    echo "🍪 Cookies:    $WORKER_COOKIES_FILE"
else
    echo "🍪 Cookies:    No encontradas (coloca ~/cookies.txt si necesitas login en IG)"
fi

TAILSCALE_IP=""
if command -v ifconfig >/dev/null 2>&1; then
    TAILSCALE_IP=$(ifconfig 2>/dev/null | grep -Eo '100\.[0-9]+\.[0-9]+\.[0-9]+' | head -n 1 || true)
fi
if [ -z "$TAILSCALE_IP" ] && command -v ip >/dev/null 2>&1; then
    TAILSCALE_IP=$(ip -4 addr 2>/dev/null | grep -Eo '100\.[0-9]+\.[0-9]+\.[0-9]+' | head -n 1 || true)
fi

if [ -n "$TAILSCALE_IP" ]; then
    echo "🌐 Tailscale detectado: http://${TAILSCALE_IP}:${LISTEN_PORT}"
    echo "   👉 Pon esto en el .env de tu VPS:"
    echo "      MEDIA_WORKER_ENABLED=true"
    echo "      MEDIA_WORKER_URL=http://${TAILSCALE_IP}:${LISTEN_PORT}"
else
    echo "🌐 Escuchando en: http://0.0.0.0:${LISTEN_PORT}"
fi
echo "=================================================="

if [ "${1:-}" = "--daemon" ] || [ "${1:-}" = "-d" ]; then
    pkill -f "python.*worker.py" 2>/dev/null || true
    nohup python "$REPO_DIR/worker.py" > "$REPO_DIR/worker.log" 2>&1 &
    echo "✅ Worker corriendo en segundo plano (PID: $!)."
    echo "📄 Para ver los logs en vivo: tail -f $REPO_DIR/worker.log"
else
    exec python "$REPO_DIR/worker.py"
fi

