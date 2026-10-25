#!/usr/bin/env bash
# PySpark Processing Runner Script

set -eo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "${SCRIPT_DIR}")"
source "${PROJECT_ROOT}/config/project.env"

LOG_FILE="${PROJECT_ROOT}/output/logs/spark.log"
mkdir -p "${PROJECT_ROOT}/output/logs"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting PySpark Preprocessing Pipeline..." | tee -a "${LOG_FILE}"
"${PROJECT_ROOT}/.venv/bin/python" "${PROJECT_ROOT}/spark/preprocessing.py" 2>&1 | tee -a "${LOG_FILE}"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] PySpark Preprocessing Completed Successfully." | tee -a "${LOG_FILE}"
