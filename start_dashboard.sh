#!/usr/bin/env bash
# Start Dashboard Script
# Runs the Streamlit interactive dashboard on localhost:8501

set -eo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${SCRIPT_DIR}"
source "${PROJECT_ROOT}/config/project.env"

LOG_FILE="${PROJECT_ROOT}/output/logs/dashboard.log"
mkdir -p "${PROJECT_ROOT}/output/logs"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting Streamlit Dashboard on http://localhost:8501 ..." | tee -a "${LOG_FILE}"

setsid "${PROJECT_ROOT}/.venv/bin/streamlit" run "${PROJECT_ROOT}/dashboard/app.py" \
    --server.port 8501 \
    --server.address 127.0.0.1 \
    --server.headless true >> "${LOG_FILE}" 2>&1 &

sleep 3
if pgrep -f "streamlit.*app.py" >/dev/null; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Dashboard is running successfully at http://localhost:8501" | tee -a "${LOG_FILE}"
else
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: Dashboard failed to start. Check ${LOG_FILE}"
    exit 1
fi
