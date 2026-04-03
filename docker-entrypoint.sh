#!/bin/sh
set -eu

mkdir -p /data /data/assets /data/logs /data/plugins

if [ "$(id -u)" = "0" ]; then
  chown -R froguser:froguser /data
  exec su -s /bin/sh froguser -c "$*"
fi

if [ ! -w /data ]; then
  echo "/data is not writable by $(id -un). Fix the host volume permissions and try again." >&2
  exit 1
fi

exec "$@"
