#!/usr/bin/env bash
#
# Continuous J2208 live collector for heart rate and SpO2.
#
#   tools/j2208_collect_live.sh
#
# Run in the foreground (or under systemd/tmux) and stop with Ctrl-C. The
# daemon keeps one BLE connection and writes every decoded sensor reading to
# the `live` table. History sync continues on its normal daemon schedule
# (--history-every is not overridden), so only live mode is selected here.
#
# Environment is loaded from, in order:
#   1. $J2208_ENV_FILE, if set
#   2. $XDG_CONFIG_HOME/j2208/env (usually ~/.config/j2208/env)
#   3. <repo>/.env
#
# Required in that file:
#   J2208_BAND_ADDRESS=AA:BB:CC:DD:EE:FF
#
# PostgreSQL settings (omit the password for peer auth over the Unix socket):
#   J2208_PG_SCHEMA=j2208
#   J2208_PG_DATABASE=bob
#   J2208_PG_USER=bob
#   J2208_PG_HOST=/var/run/postgresql
#
# Optional:
#   J2208_DB_BACKEND=postgres       # default: postgres
#   J2208_LIVE_MEASURE_TYPE=3       # 3=SpO2 (also reports HR); 2=heart only
#   J2208_LIVE_BURST=0              # 0 keeps the sensor on continuously
#   J2208_LIVE_LOG=~/.local/state/j2208/live.log
#   J2208_LIVE_LOCK=/run/user/1000/j2208-live.lock
#   J2208_PYTHON=<repo>/.venv/bin/python
#
# The script serialises runs with flock: a second instance exits instead of
# fighting over the band's single BLE connection.
#
# Battery note: live mode powers the optical sensor continuously and drains
# the band much faster than the history collector. Ctrl-C shuts down cleanly.

set -euo pipefail
PATH=/usr/local/bin:/usr/bin:/bin

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_DIR=$(cd -- "$SCRIPT_DIR/.." && pwd)

ENV_FILE=${J2208_ENV_FILE:-${XDG_CONFIG_HOME:-$HOME/.config}/j2208/env}
if [[ ! -f $ENV_FILE && -f $REPO_DIR/.env ]]; then
    ENV_FILE=$REPO_DIR/.env
fi
if [[ ! -f $ENV_FILE ]]; then
    echo "j2208: environment file not found: $ENV_FILE" >&2
    exit 2
fi

set -a
# shellcheck disable=SC1090
. "$ENV_FILE"
set +a

ADDRESS=${1:-${J2208_BAND_ADDRESS:-}}
if [[ -z $ADDRESS ]]; then
    echo "j2208: set J2208_BAND_ADDRESS in $ENV_FILE or pass an address" >&2
    exit 2
fi

PYTHON=${J2208_PYTHON:-$REPO_DIR/.venv/bin/python}
BACKEND=${J2208_DB_BACKEND:-postgres}
MEASURE_TYPE=${J2208_LIVE_MEASURE_TYPE:-3}
BURST=${J2208_LIVE_BURST:-0}
LOCK=${J2208_LIVE_LOCK:-${XDG_RUNTIME_DIR:-/tmp}/j2208-live.lock}
LOG=${J2208_LIVE_LOG:-}

if [[ ! -x $PYTHON ]]; then
    echo "j2208: Python not executable: $PYTHON" >&2
    exit 2
fi

mkdir -p -- "$(dirname -- "$LOCK")"
if [[ -n $LOG ]]; then
    mkdir -p -- "$(dirname -- "$LOG")"
    exec > >(tee -a -- "$LOG") 2>&1
fi

perms=$(stat -c '%a' "$ENV_FILE" 2>/dev/null || echo '?')
case $perms in
    400 | 600) ;;
    *) echo "j2208: warning: $ENV_FILE permissions are $perms (expected 600)" >&2 ;;
esac

exec 9>"$LOCK"
if ! flock -n 9; then
    echo "j2208: another live collector is already running" >&2
    exit 0
fi

cd -- "$REPO_DIR"
echo "j2208: starting live collection: address=$ADDRESS backend=$BACKEND" \
    "measure_type=$MEASURE_TYPE burst=$BURST"

exec "$PYTHON" -u "$REPO_DIR/tools/j2208_daemon.py" "$ADDRESS" \
    --backend "$BACKEND" \
    --mode live \
    --measure-type "$MEASURE_TYPE" \
    --burst "$BURST"
