#!/usr/bin/env sh
# Run a command inside python:3.14-slim with the repo mounted at /app and the .venv from `make venv-docker`.
set -eu
exec docker run --rm -i -u "$(id -u):$(id -g)" -v "$PWD:/app" -w /app -e HOME=/tmp -e PATH=/app/.venv/bin:/usr/local/bin:/usr/bin:/bin \
  python:3.14-slim "$@"
