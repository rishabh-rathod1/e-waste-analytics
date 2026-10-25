#!/usr/bin/env bash
# HDFS Ingestion Script
# Automatically discovers local WEEE CSV and ingests into HDFS

set -eo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "${SCRIPT_DIR}")"
source "${PROJECT_ROOT}/config/project.env"

LOG_FILE="${PROJECT_ROOT}/output/logs/hdfs.log"
mkdir -p "${PROJECT_ROOT}/output/logs"

log() {
    local msg="[$(date '+%Y-%m-%d %H:%M:%S')] $1"
    echo "$msg" | tee -a "${LOG_FILE}"
}

log "=== Starting HDFS Ingestion Phase ==="

# 1. Automatic Dataset Discovery
DATASET_PATH="${HOME}/Downloads/DATASET/WEEE_SF_cmp_version2.csv"
if [ ! -f "${DATASET_PATH}" ]; then
    log "Finding primary WEEE CSV in ${DATASET_DIR}..."
    DATASET_PATH=$(find "${DATASET_DIR}" -type f -name "WEEE_SF_*.csv" | head -n 1)
fi

if [ -z "${DATASET_PATH}" ] || [ ! -f "${DATASET_PATH}" ]; then
    log "ERROR: Could not locate primary WEEE CSV dataset!"
    exit 1
fi

DATASET_SIZE=$(ls -lh "${DATASET_PATH}" | awk '{print $5}')
log "Discovered Dataset: ${DATASET_PATH} (${DATASET_SIZE})"

# 2. Check HDFS service availability
log "Checking HDFS connectivity..."
if ! hdfs dfs -ls / >/dev/null 2>&1; then
    log "HDFS is not responding. Please start HDFS services first (e.g. ./start_services.sh)."
    exit 1
fi

# 3. Create Required HDFS Directory Hierarchy
log "Creating HDFS directory structure..."
hdfs dfs -mkdir -p /ewaste/raw/futuram
hdfs dfs -mkdir -p /ewaste/processed/parquet
hdfs dfs -mkdir -p /ewaste/output/mapreduce
hdfs dfs -mkdir -p /ewaste/results
hdfs dfs -mkdir -p /ewaste/warehouse

# 4. Upload Dataset to HDFS (avoid re-uploading if already exists and matches)
HDFS_TARGET="/ewaste/raw/futuram/$(basename "${DATASET_PATH}")"
if hdfs dfs -test -e "${HDFS_TARGET}"; then
    log "Dataset already present in HDFS at: ${HDFS_TARGET}"
else
    log "Uploading ${DATASET_PATH} -> ${HDFS_TARGET} ..."
    hdfs dfs -put -f "${DATASET_PATH}" "${HDFS_TARGET}"
    log "Upload completed successfully."
fi

# 5. Verification
log "Verifying HDFS files:"
hdfs dfs -ls -h /ewaste/raw/futuram | tee -a "${LOG_FILE}"
log "Checking HDFS disk usage:"
hdfs dfs -du -h /ewaste/raw/futuram | tee -a "${LOG_FILE}"

log "=== HDFS Ingestion Completed Successfully ==="
