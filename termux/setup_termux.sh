#!/data/data/com.termux/files/usr/bin/bash
# =============================================================================
# setup_termux.sh — Instalador todo-en-uno para Termux (Opción 1 + Opción 2)
# =============================================================================
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "=================================================="
echo "📱 Configurando Muvixo en Termux (Worker + Share)"
echo "=================================================="

echo "📦 [1/4] Instalando paquetes base en Termux..."
pkg update -y
pkg install -y python ffmpeg git termux-api termux-tools

echo "🐍 [2/4] Instalando librerías Python (flask, requests, yt-dlp)..."
pip install --upgrade flask requests yt-dlp

echo "📂 [3/4] Configurando acceso al almacenamiento de Android..."
if [ ! -d "$HOME/storage" ]; then
    echo "👉 Acepta el permiso de almacenamiento en la ventana de Android..."
    termux-setup-storage
    sleep 3
fi
mkdir -p /sdcard/Download/Muvixo 2>/dev/null || mkdir -p "$HOME/storage/downloads/Muvixo" 2>/dev/null || true

echo "🔗 [4/4] Activando botón 'Compartir -> Termux' (termux-url-opener)..."
mkdir -p "$HOME/bin"
cp "$SCRIPT_DIR/termux-url-opener" "$HOME/bin/termux-url-opener"
sed -i "s|^REPO_DIR=.*|REPO_DIR=\"$REPO_DIR\"|" "$HOME/bin/termux-url-opener"
chmod +x "$HOME/bin/termux-url-opener"
chmod +x "$SCRIPT_DIR/start_worker.sh"
chmod +x "$SCRIPT_DIR/share_download.py"

echo ""
echo "✅ ¡Instalación completada con éxito!"
echo "--------------------------------------------------"
echo "1️⃣ Para encender el Worker conectado a tu VPS por Tailscale:"
echo "   bash termux/start_worker.sh"
echo "   (o en segundo plano: bash termux/start_worker.sh --daemon)"
echo ""
echo "2️⃣ Para usar 'Compartir -> Termux':"
echo "   Abre Instagram, TikTok, Pinterest, X o YouTube en tu celular,"
echo "   toca Compartir -> elige Termux ¡y se guardará en tu Galería!"
echo ""
echo "3️⃣ (Opcional) Si quieres que descargue posts privados/restringidos de tu cuenta:"
echo "   Guarda tus cookies exportadas en: ~/cookies.txt"
echo "--------------------------------------------------"
