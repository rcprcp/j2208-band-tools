#!/usr/bin/env bash
#
# One-shot J2208 history collector, intended for cron.
#
#   */30 * * * * /home/bob/j2208-band-tools/tools/j2208_collect_once.sh
#
# Environment is loaded from, in order:
#   1. $J2208_ENV_FILE, if set
#   2. $XDG_CONFIG_HOME/j2208/env (usually ~/.config/j2208/env)
#   3. <repo>/.env
#
# Required in that file:
#   J2208_BAND_ADDRESS=AA:BB:CC:DD:EE:FF
#   J2208_PG_SCHEMA=j2208
#   J2208_PG_DATABASE=bob
#   J2208_PG_USER=bob
#   J2208_PG_HOST=127.0.0.1
#   J2208_PG_PASSWORD=...          # omit for peer auth over the Unix socket
#
# Optional:
#   J2208_DB_BACKEND=postgres       # default: postgres
#   J2208_COLLECT_TYPES="hr temp_axil spo2_auto hrv steps total sleep"
#   J2208_COLLECT_LOG=~/.local/state/j2208/collect.log
#   J2208_COLLECT_LOCK=/run/user/1000/j2208-collect.lock
#   J2208_COLLECT_TIMEOUT=600       # seconds; 0 disables the timeout wrapper
#   J2208_PYTHON=<repo>/.venv/bin/python
#
# The script is safe to run from cron: it serialises overlapping runs with
# flock, appends a timestamped log, and only writes to stderr on failure.

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
TYPES=${J2208_COLLECT_TYPES:-hr temp_axil spo2_auto hrv steps total sleep}
LOG=${J2208_COLLECT_LOG:-${XDG_STATE_HOME:-$HOME/.local/state}/j2208/collect.log}
LOCK=${J2208_COLLECT_LOCK:-${XDG_RUNTIME_DIR:-/tmp}/j2208-collect.lock}
COLLECT_TIMEOUT=${J2208_COLLECT_TIMEOUT:-600}

if [[ ! -x $PYTHON ]]; then
    echo "j2208: Python not executable: $PYTHON" >&2
    exit 2
fi

mkdir -p -- "$(dirname -- "$LOG")"
mkdir -p -- "$(dirname -- "$LOCK")"

log() {
    printf '%s %s\n' "$(date -Is)" "$*" >>"$LOG"
}

perms=$(stat -c '%a' "$ENV_FILE" 2>/dev/null || echo '?')
case $perms in
    400 | 600) ;;
    *) log "warning: $ENV_FILE permissions are $perms (expected 600)" ;;
esac

exec 9>"$LOCK"
if ! flock -n 9; then
    log "another collection is already running; skipping"
    exit 0
fi

read -r -a TYPE_ARGS <<<"$TYPES"
CMD=(
    "$PYTHON" -u "$REPO_DIR/tools/j2208_daemon.py" "$ADDRESS"
    --backend "$BACKEND"
    --mode history
    --once
    --types "${TYPE_ARGS[@]}"
)
if ((COLLECT_TIMEOUT > 0)); then
    CMD=(timeout --signal=INT --kill-after=30 "$COLLECT_TIMEOUT" "${CMD[@]}")
fi

cd -- "$REPO_DIR"
log "starting: address=$ADDRESS backend=$BACKEND types=$TYPES"
if "${CMD[@]}" >>"$LOG" 2>&1; then
    log "completed"
    exit 0
else
    status=$?
    log "failed with status $status"
    echo "j2208: collection failed (status $status); see $LOG" >&2
    exit "$status"
fi
