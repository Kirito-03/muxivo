<p align="center">
  <img src="icon.png" alt="Muxivo" width="80" />
</p>

<h1 align="center">Muxivo</h1>

<p align="center">
  <strong>Descargador de medios self-hosted con soporte multi-plataforma</strong><br/>
  Audio · Video · Imágenes — YouTube, TikTok, Instagram y más
</p>

<p align="center">
  <img src="https://img.shields.io/badge/python-3.10+-3776AB?logo=python&logoColor=white" alt="Python" />
  <img src="https://img.shields.io/badge/flask-3.0-000000?logo=flask&logoColor=white" alt="Flask" />
  <img src="https://img.shields.io/badge/docker-ready-2496ED?logo=docker&logoColor=white" alt="Docker" />
  <img src="https://img.shields.io/badge/yt--dlp-latest-FF0000?logo=youtube&logoColor=white" alt="yt-dlp" />
  <img src="https://img.shields.io/badge/license-private-lightgrey" alt="License" />
</p>

---

## Descripción

**Muxivo** es una aplicación web self-hosted para descargar audio, video e imágenes desde múltiples plataformas sociales. Funciona como una PWA instalable con interfaz dark premium, detección automática de contenido, selección de calidad dinámica y reproducción in-app.

El backend usa **Flask** + **yt-dlp** + **Playwright** dentro de un contenedor Docker basado en la imagen oficial de Playwright, garantizando compatibilidad con plataformas que requieren JavaScript rendering o cookies de autenticación.

---

## Características

| Categoría | Detalle |
|---|---|
| **Plataformas** | YouTube, TikTok, Instagram, y cualquier sitio soportado por yt-dlp |
| **Tipos de media** | Audio (MP3, M4A, OPUS, OGG, WAV, FLAC), Video (MP4, WebM, MKV), Imágenes (JPG, PNG, WebP) |
| **Detección inteligente** | Análisis automático de URLs: identifica plataforma, tipo de contenido y calidades disponibles |
| **Galerías de imágenes** | Extracción de galerías completas de TikTok `/photo/` e Instagram `/p/` con selector visual |
| **Calidad dinámica** | Probing en tiempo real de resoluciones/bitrates disponibles vía yt-dlp |
| **Worker remoto TikTok** | Fallback a un worker externo (Termux/Tailscale) para galerías de fotos no accesibles en VPS |
| **Gestión de cookies** | Sistema de selección automática de cookies por plataforma con validación, fallback y rotación |
| **Sesiones aisladas** | Historial y archivos separados por dispositivo usando `session_id` en localStorage |
| **Auto-limpieza** | Daemon de auto-destrucción configurable que limpia sesiones, previews y archivos servidos |
| **Rate limiting** | Límite de requests por IP por minuto, límite de descargas concurrentes y tope de almacenamiento |
| **PWA** | Instalable como app nativa en móvil/desktop con Service Worker y manifest |
| **Playback in-app** | Reproductor integrado de audio, video e imágenes con selector de archivos |
| **ZIP automático** | Empaquetado automático cuando se descargan múltiples archivos |

---

## Stack tecnológico

- **Backend:** Python 3.10+ · Flask 3.0 · Gunicorn
- **Extractor:** yt-dlp (con Deno para resolver nsig/signature challenges de YouTube)
- **Browser engine:** Playwright (headless Chromium para rendering JS)
- **Procesamiento:** FFmpeg (conversión de formatos, merge de streams)
- **Frontend:** Vanilla JS + CSS (Inter font, glassmorphism, dark theme)
- **Contenedor:** Docker (imagen base `mcr.microsoft.com/playwright/python:v1.49.1-jammy`)
- **Reverse proxy:** Caddy (HTTPS automático, gzip/zstd)

---

## Estructura del proyecto

```
muxivo/
├── app.py                  # Servidor Flask principal (API + rutas)
├── media_tools.py          # Motor de descarga (yt-dlp, ffmpeg, cookies, proxies)
├── worker.py               # Worker independiente para galerías TikTok (Termux)
├── Dockerfile              # Imagen Docker con Playwright + Deno + FFmpeg
├── docker-compose.yml      # Orquestación del servicio
├── entrypoint.sh           # Script de entrada (permisos, usuario pwuser)
├── requirements.txt        # Dependencias Python
├── packages.txt            # Paquetes de sistema (ffmpeg)
├── manifest.webmanifest    # Manifest PWA
├── service-worker.js       # Service Worker para funcionalidad offline
├── templates/
│   └── index.html          # Template principal (Jinja2)
├── static/
│   ├── css/styles.css      # Estilos (dark premium, glassmorphism)
│   └── js/app.js           # Lógica del frontend (detección, descarga, playback)
├── deploy/
│   └── Caddyfile           # Configuración del reverse proxy
├── .env.example            # Variables de entorno de ejemplo
└── .gitignore
```

---

## Requisitos previos

- **Docker** y **Docker Compose** instalados
- Archivos de cookies Netscape para plataformas que requieren autenticación (YouTube, Instagram, TikTok)
- (Opcional) Dominio con Caddy para HTTPS

---

## Instalación y despliegue

### 1. Clonar el repositorio

```bash
git clone https://github.com/tu-usuario/muxivo.git
cd muxivo
```

### 2. Configurar variables de entorno

```bash
cp .env.example .env
```

Editar `.env` según necesidad:

```env
APP_DOMAIN=muxivo.art
MAX_SESSION_COUNT=5
MAX_SESSION_AGE_SECONDS=1800
MAX_DOWNLOADS_SIZE_MB=2048
MAX_CONCURRENT_DOWNLOADS=2
MAX_REQ_PER_MIN=60
HISTORY_MAX_ITEMS=30
```

### 3. Colocar archivos de cookies

Exportar cookies en formato Netscape desde el navegador y colocarlas en la raíz del proyecto:

```
www.youtube.com_cookies.txt
www.instagram.com_cookies.txt
www.tiktok.com_cookies.txt
```

O usar la estructura organizada por plataforma en el directorio `cookies/`:

```
cookies/
├── youtube/current.txt
├── instagram/current.txt
└── tiktok/current.txt
```

### 4. Build y arrancar

```bash
docker compose up -d --build
```

El servicio estará disponible en `http://localhost:4005`.

### 5. (Opcional) Reverse proxy con Caddy

Configurar el dominio en `deploy/Caddyfile` y asegurar que la variable `APP_DOMAIN` esté definida.

---

## API Endpoints

| Método | Ruta | Descripción |
|---|---|---|
| `GET` | `/` | Interfaz web principal |
| `GET` | `/api/options` | Obtiene formatos y calidades disponibles para una URL |
| `GET` | `/api/detect` | Analiza una URL: plataforma, tipo de contenido, galería de imágenes |
| `GET` | `/api/history` | Historial de descargas por sesión |
| `GET` | `/api/thumb` | Proxy de thumbnails de CDNs externos (TikTok, Instagram) |
| `POST` | `/api/download` | Descarga media (audio/video/imagen) |
| `GET` | `/files/<path>` | Sirve archivos descargados |
| `GET` | `/manifest.webmanifest` | PWA manifest |
| `GET` | `/service-worker.js` | Service Worker |

---

## Worker TikTok (Opcional)

El archivo `worker.py` es un servicio Flask independiente diseñado para correr en un dispositivo con acceso directo a TikTok (ej: smartphone con Termux conectado vía Tailscale). Extrae galerías completas de fotos de posts `/photo/` que no son accesibles desde un VPS.

```bash
# En Termux/dispositivo local
pip install flask requests
python worker.py
```

**Endpoint:** `POST /extract` con body `{"url": "https://vt.tiktok.com/..."}`

Configurar en `docker-compose.yml`:

```yaml
TIKTOK_PHOTO_WORKER_URL: http://100.70.78.80:5001
TIKTOK_PHOTO_WORKER_TOKEN: tu-token-secreto
```

---

## Variables de entorno

| Variable | Default | Descripción |
|---|---|---|
| `PORT` | `7860` | Puerto interno del servidor |
| `MAX_SESSION_COUNT` | `5` | Máximo de sesiones simultáneas |
| `MAX_SESSION_AGE_SECONDS` | `1800` | Tiempo de vida de sesiones (segundos) |
| `MAX_DOWNLOADS_SIZE_MB` | `2048` | Límite total de almacenamiento de descargas (MB) |
| `MAX_CONCURRENT_DOWNLOADS` | `2` | Descargas simultáneas permitidas |
| `MAX_REQ_PER_MIN` | `60` | Requests por IP por minuto |
| `HISTORY_MAX_ITEMS` | `30` | Ítems máximos en historial por sesión |
| `AUTODESTRUCT_INTERVAL_SECONDS` | `600` | Intervalo del daemon de limpieza |
| `AUTODESTRUCT_MAX_AGE_SECONDS` | `1800` | Edad máxima de archivos antes de auto-eliminación |
| `TIKTOK_PHOTO_WORKER_URL` | — | URL del worker remoto de TikTok |
| `TIKTOK_PHOTO_WORKER_TOKEN` | — | Token de autenticación del worker |
| `GUNICORN_WORKERS` | `2` | Workers de Gunicorn |
| `GUNICORN_TIMEOUT` | `180` | Timeout de Gunicorn (segundos) |

---

## Desarrollo local

```bash
# Crear entorno virtual
python -m venv env
source env/bin/activate  # Linux/Mac
env\Scripts\activate     # Windows

# Instalar dependencias
pip install -r requirements.txt
playwright install chromium

# Ejecutar
python app.py
```

---

## Licencia

Proyecto privado. Todos los derechos reservados.
