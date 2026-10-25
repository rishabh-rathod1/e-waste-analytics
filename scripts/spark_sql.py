#!/usr/bin/env bash
# Spark SQL Runner Script

set -eo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "${SCRIPT_DIR}")"
source "${PROJECT_ROOT}/config/project.env"

LOG_FILE="${PROJECT_ROOT}/output/logs/sql.log"
mkdir -p "${PROJECT_ROOT}/output/logs"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting Spark SQL Analytical Stage..." | tee -a "${LOG_FILE}"
"${PROJECT_ROOT}/.venv/bin/python" "${PROJECT_ROOT}/spark/sql_queries.py" 2>&1 | tee -a "${LOG_FILE}"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Spark SQL Completed Successfully." | tee -a "${LOG_FILE}"
