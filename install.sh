#!/usr/bin/env bash
# ============================================================================
#  HITEK + Truecaller Data Gateway - one-shot installer / non-stop runner
# ----------------------------------------------------------------------------
#  Usage:
#      chmod +x install.sh
#      ./install.sh                  # install + run (systemd if root, else loop)
#      PORT=17712 ./install.sh       # custom port (default = code port 17712)
#      INSTALL_SERVICE=0 ./install.sh  # skip systemd, run foreground loop
#
#  What it does in one go:
#    1. checks python3
#    2. creates .venv
#    3. upgrades pip + installs requirements.txt
#    4. installs a systemd service with Restart=always (as root) OR
#       runs a foreground auto-restart loop (as a normal user)
# ============================================================================
set -euo pipefail

APP_NAME="${APP_NAME:-hitek-truecaller}"
HOST="${HOST:-0.0.0.0}"
PORT="${PORT:-17712}"
HF_TOKEN="${HF_TOKEN:-}"
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV="$DIR/.venv"
PY="${PYTHON:-python3}"

cd "$DIR"

say() { printf '\033[1;36m[install]\033[0m %s\n' "$*"; }
die() { printf '\033[1;31m[error]\033[0m %s\n' "$*" >&2; exit 1; }

# --- 1. Python ----------------------------------------------------------------
command -v "$PY" >/dev/null 2>&1 || die "python3 nahi mila. Pehle python3 install karo."
say "Python: $($PY --version 2>&1)"

# --- 2. venv ------------------------------------------------------------------
if [ ! -d "$VENV" ]; then
  say "Virtual environment bana raha hoon: $VENV"
  "$PY" -m venv "$VENV" || die "venv create fail (python3-venv installed hai?)"
else
  say "VenV already exists: $VENV"
fi

# shellcheck disable=SC1091
source "$VENV/bin/activate"

# --- 3. pip + requirements ----------------------------------------------------
say "pip upgrade..."
python -m pip install --upgrade pip setuptools wheel

if [ -f "$DIR/requirements.txt" ]; then
  say "Requirements install..."
  pip install -r "$DIR/requirements.txt"
else
  say "requirements.txt nahi mila, direct install..."
  pip install fastapi "uvicorn[standard]" duckdb
fi

# --- 3b. optional one-time local DB index (no token needed) -------------------
if [ "${BUILD_INDEX:-0}" = "1" ]; then
  say "Local DB index build (one time, ~12GB download). Thoda time lagega..."
  python "$DIR/main.py" --build-index || die "index build fail hua"
  say "Local index ready: $DIR/data/idx_phone.parquet"
fi

# --- 4. systemd service (non-stop) -------------------------------------------
if [ "${INSTALL_SERVICE:-1}" = "1" ] && [ "$(id -u)" = "0" ] && command -v systemctl >/dev/null 2>&1; then
  say "systemd service install kar raha hoon: ${APP_NAME}.service"
  HF_ENV=""
  if [ -n "$HF_TOKEN" ]; then HF_ENV="Environment=HF_TOKEN=$HF_TOKEN"; fi
  cat > "/etc/systemd/system/${APP_NAME}.service" <<EOF
[Unit]
Description=HITEK + Truecaller Data Gateway
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=$DIR
Environment=HOST=$HOST
Environment=PORT=$PORT
$HF_ENV
ExecStart=$VENV/bin/uvicorn main:app --host $HOST --port $PORT
Restart=always
RestartSec=3
LimitNOFILE=65535

[Install]
WantedBy=multi-user.target
EOF
  systemctl daemon-reload
  systemctl enable "$APP_NAME"
  systemctl restart "$APP_NAME"
  sleep 2
  say "Deployed non-stop on ${HOST}:${PORT}"
  systemctl --no-pager --lines=8 status "$APP_NAME" || true
  say "Logs: journalctl -u ${APP_NAME} -f"
  exit 0
fi

# --- 5. foreground auto-restart loop -----------------------------------------
say "systemd available nahi (ya INSTALL_SERVICE=0). Foreground loop start."
say "Server: http://${HOST}:${PORT}  (Ctrl+C to stop)"
while true; do
  "$VENV/bin/uvicorn" main:app --host "$HOST" --port "$PORT" || true
  say "Process exit hua, 3s mein restart..."
  sleep 3
done
