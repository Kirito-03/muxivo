#!/bin/sh
set -eu

dirs="/app/downloads /app/preview_cache /app/served_files /app/cookies"

for d in $dirs; do
  mkdir -p "$d"
done

pwuid="$(id -u pwuser 2>/dev/null || echo 1000)"
pwgid="$(id -g pwuser 2>/dev/null || echo 1000)"

for d in $dirs; do
  if [ -e "$d" ]; then
    duid="$(stat -c %u "$d" 2>/dev/null || echo 0)"
    dgid="$(stat -c %g "$d" 2>/dev/null || echo 0)"
    if [ "$duid" != "$pwuid" ] || [ "$dgid" != "$pwgid" ]; then
      chown -R pwuser:pwuser "$d" || true
    fi
  fi
done

if command -v runuser >/dev/null 2>&1; then
  exec runuser -u pwuser -- "$@"
fi

exec su -s /bin/sh pwuser -c "$*"
