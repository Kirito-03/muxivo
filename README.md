---
title: Media Downloader (YouTube / TikTok / Instagram / Spotify*)
emoji: 🎧
colorFrom: gray
colorTo: gray
sdk: gradio
sdk_version: 5.44.1
app_file: app.py
pinned: false
license: mit
---

# Media Downloader (YouTube / TikTok / Instagram / Spotify*)

[![Python](https://img.shields.io/badge/Python-3.10%2B-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![Gradio](https://img.shields.io/badge/Gradio-4.x-FF6F61)](https://www.gradio.app/)
[![yt-dlp](https://img.shields.io/badge/yt--dlp-latest-FFEE58)](https://github.com/yt-dlp/yt-dlp)
[![FFmpeg](https://img.shields.io/badge/FFmpeg-required-2ECC71)](https://ffmpeg.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

Baixe **áudio** ou **vídeo** de múltiplas plataformas (YouTube, TikTok, Instagram…).  
Suporte **opcional** a **Spotify** via [`spotdl`](https://github.com/spotDL/spotify-downloader) para modo **áudio**.  
Interface em **Gradio 4** com tema escuro padronizado, pronta para embed no portfólio.

> \* Spotify funciona apenas no modo **áudio** e **se** `spotdl` estiver instalado/acessível no PATH.

---

## ⚠️ Aviso legal

Use apenas em conteúdos que você **tem direito** de baixar.  
Algumas plataformas proíbem download via termos de uso; respeite **ToS** e **copyright**.

---

## ✨ Recursos

- **Áudio** (`mp3`, `m4a`, `opus`) com bitrate configurável (ex.: 192 kbps)
- **Vídeo** (`mp4`, `webm`) com **Altura Máxima (p)** e remux automático
- **Pasta por canal/artista** (organização automática por origem)
- **Cookies** (formato Netscape), **proxy**, **geo-country** (bypass regional)
- **Playlist slice**: baixar **apenas** parte de uma playlist (início/fim 1-based)
- **PO token** (YouTube) opcional para cenários que exigem parâmetros adicionais
- **ID3** + **capa** em MP3 (quando disponível)
- **Resumo** no fim e **JSON de falhas** (URL → motivo)

---

## 🧩 Como funciona (visão geral)

A UI chama `process_links(...)` (em `media_tools.py`), que:
1. Separa links **Spotify** e **não-Spotify**.
2. **Spotify** (opcional): usa `spotdl` se habilitado (apenas **áudio**).
3. **Demais**: usa `yt-dlp` com as opções escolhidas (formato, container, resolução).
4. Organiza saída por **canal/artista** (se marcado), aplica **ID3/capa** em MP3.
5. Gera um **.zip** final com todos os arquivos e um **resumo** (sucessos/falhas).

---

## 📦 Requisitos

- **Python 3.10+**
- **FFmpeg** (obrigatório para conversão/pos-processamento)
- Pacotes (pip) mínimos:
  ```txt
  gradio>=4.0.0
  yt-dlp
  mutagen
  pillow
  requests
  ```
- *(Opcional)* **spotdl** (para Spotify): **recomendado via `pipx`**
  ```bash
  pipx install spotdl
  # verifique:
  spotdl --version
  ```

> No **Hugging Face**, crie `packages.txt` com:  
> ```txt
> ffmpeg
> ```

---

## ▶️ Como rodar localmente

### Windows (PowerShell)
```powershell
# 1) Clonar
git clone https://github.com/wallacetcbrasil/media_downloader.git
cd media_downloader

# 2) Virtualenv
python -m venv .venv
.\.venv\Scriptsctivate

# 3) Dependências
python -m pip install -U pip
python -m pip install -r requirements.txt

# (Opcional) Spotify
pipx install spotdl

# 4) Executar
python app.py
```

### Linux / macOS
```bash
git clone https://github.com/wallacetcbrasil/media_downloader.git
cd media_downloader

python -m venv .venv
source .venv/bin/activate

python -m pip install -U pip
pip install -r requirements.txt

# (Opcional) Spotify
pipx install spotdl

python app.py
```

Abra o link do Gradio no terminal (geralmente `http://127.0.0.1:7860`).

---

## 🖱️ Uso (UI)

1. **Cole os links** (um por linha ou separados por vírgula).  
2. Selecione **Modo** (**áudio** ou **vídeo**):
   - Áudio: formato (`mp3`/`m4a`/`opus`) e **bitrate**.
   - Vídeo: container (`mp4`/`webm`) e **altura máxima**.
3. (Opcional) **Pasta por canal/artista** para organizar saídas.
4. (Opcional) **Cookies** em formato Netscape (`.txt`), **proxy** e **geo-country** (ex.: `US`).
5. (Opcional) **Playlist início/fim** (1-based) para baixar **parte** de uma playlist.
6. (Opcional) **PO token (YouTube)** se necessário.
7. Clique **Baixar** → receba o **.zip**, o **resumo** e o **JSON de falhas**.

> Dica: para bloqueios regionais, tente **cookies**, **proxy** e/ou outro **geo-country**.

---

## 🗂️ Estrutura do projeto

```
media_downloader/
├─ app.py             # Interface Gradio (tema escuro, inputs organizados)
├─ media_tools.py     # Orquestração com yt-dlp/spotdl, ID3/capas, zip final
├─ requirements.txt   # Dependências pip
├─ packages.txt       # ffmpeg (para Hugging Face)
├─ README.md          # Este arquivo
└─ (opcional) assets/, samples/
```

`.gitignore` sugerido:
```gitignore
.venv/
__pycache__/
*.pyc
*.log
*.zip
*.mp3
*.m4a
*.opus
*.mp4
*.webm
.gradio/
.vscode/
.DS_Store
```

---

## ☁️ Deploy no Hugging Face (opcional)

1. Crie um **Space (Gradio)**.  
2. Envie **`app.py`**, **`media_tools.py`**, **`requirements.txt`**, **`packages.txt`** e este **`README.md`**.  
3. Commit → o Space sobe automaticamente.

**Front-matter** (adicione no topo do README se for usar este arquivo também no Space):
```md
---
title: Media Downloader (YouTube / TikTok / Instagram / Spotify*)
emoji: 🎧
colorFrom: gray
colorTo: gray
sdk: gradio
sdk_version: "4.0.0"
app_file: app.py
pinned: false
license: mit
---
```

> Para **embed** no portfólio use o domínio `*.hf.space`:  
> `https://<Owner>-<SpaceName>.hf.space/?__theme=dark`

---

## 🔐 Boas práticas

- Respeite **direitos autorais** e **termos de uso** das plataformas.  
- Prefira **conteúdo próprio** ou com permissão explícita.  
- Para bibliotecas longas, ajuste **retries** e **pausa entre itens**.

---

## 📝 Licença

MIT © 2025 **Wallace Corrêa Brasil** — veja `LICENSE`.

---

## 🙌 Créditos

- Downloads: **yt-dlp**  
- Áudio/ID3: **FFmpeg**, **Mutagen**  
- UI: **Gradio 4**