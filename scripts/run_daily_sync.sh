#!/bin/bash
# Wrapper bash dla uruchamiania skryptów Python z venv przez cron
# Użycie: ./run_daily_sync.sh

set -euo pipefail  # Exit on error, undefined var, pipe failure

# === KONFIGURACJA ===
PROJECT_ROOT="/home/ubuntu/ZUS"
VENV_PYTHON="${PROJECT_ROOT}/.venv/bin/python"
SCRIPT="${PROJECT_ROOT}/scripts/daily_sync.py"
LOG_DIR="${PROJECT_ROOT}/logs"

# === WALIDACJA ===
if [[ ! -f "$VENV_PYTHON" ]]; then
    echo "BŁĄD: Python venv nie istnieje: $VENV_PYTHON" >&2
    exit 1
fi

if [[ ! -f "$SCRIPT" ]]; then
    echo "BŁĄD: Skrypt nie istnieje: $SCRIPT" >&2
    exit 1
fi

# === UTWORZENIE KATALOGU LOGÓW ===
mkdir -p "$LOG_DIR"

# === URUCHOMIENIE ===
echo "[$(date +'%Y-%m-%d %H:%M:%S')] START: run_daily_sync.sh" >&2

# Przekaż wszystkie argumenty do skryptu Python
exec "$VENV_PYTHON" "$SCRIPT" "$@"
