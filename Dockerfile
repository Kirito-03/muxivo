FROM mcr.microsoft.com/playwright/python:v1.49.1-jammy

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PORT=7860 \
    DENO_INSTALL=/usr/local

WORKDIR /app

COPY packages.txt ./
RUN apt-get update \
    && tr -d '\r' < packages.txt | xargs -r apt-get install -y --no-install-recommends \
    && apt-get install -y --no-install-recommends curl ca-certificates unzip \
    && rm -rf /var/lib/apt/lists/*

# Deno — runtime JS requerido por yt-dlp para resolver nsig/signature challenges de YouTube
RUN curl -fsSL https://deno.land/install.sh | sh \
    && ln -sf /usr/local/bin/deno /usr/bin/deno \
    && deno --version

COPY requirements.txt ./
RUN python -m pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p /app/downloads /app/preview_cache /app/served_files /app/cookies \
    && chown -R pwuser:pwuser /app

COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

EXPOSE 7860

USER root

ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["sh", "-c", "gunicorn -w ${GUNICORN_WORKERS:-2} -b 0.0.0.0:${PORT:-7860} --timeout ${GUNICORN_TIMEOUT:-180} --access-logfile - --error-logfile - app:app"]
