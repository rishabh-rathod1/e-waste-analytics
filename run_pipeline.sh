#!/usr/bin/env bash
# ==============================================================================
# Master Big Data Pipeline Execution Script
# Project: Electronic Waste Generation Analysis & Recycling Infrastructure
# ==============================================================================

set -eo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${SCRIPT_DIR}"
source "${PROJECT_ROOT}/config/project.env"

LOG_DIR="${PROJECT_ROOT}/output/logs"
RESULTS_DIR="${PROJECT_ROOT}/output/results"
mkdir -p "${LOG_DIR}" "${RESULTS_DIR}"

PIPELINE_LOG="${LOG_DIR}/pipeline.log"

log() {
    local msg="[$(date '+%Y-%m-%d %H:%M:%S')] $1"
    echo "$msg" | tee -a "${PIPELINE_LOG}"
}

log "======================================================================"
log "STARTING MASTER E-WASTE BIG DATA PIPELINE"
log "======================================================================"

# Stage 1: Verify and Start Services
log ">>> STAGE 1: Verifying Big Data Cluster Services (HDFS, YARN, MongoDB)..."
"${PROJECT_ROOT}/start_services.sh" >> "${PIPELINE_LOG}" 2>&1
log "✓ Services verified and active."

# Stage 2: Ingest Dataset into HDFS
log ">>> STAGE 2: Discovering Dataset and Ingesting into HDFS..."
"${PROJECT_ROOT}/scripts/ingest_hdfs.sh" 2>&1 | tee -a "${LOG_DIR}/hdfs.log"
log "✓ HDFS Ingestion completed."

# Stage 3: Dataset Profiling & Data Quality Audit
log ">>> STAGE 3: Profiling Dataset & Generating Data Quality Metrics..."
if [ ! -f "${RESULTS_DIR}/data_quality.csv" ]; then
    "${PROJECT_ROOT}/.venv/bin/python" "${PROJECT_ROOT}/scripts/profile_dataset.py" 2>&1 | tee -a "${LOG_DIR}/profile.log"
else
    log "Data quality metrics already computed at ${RESULTS_DIR}/data_quality.csv"
fi
log "✓ Data Quality Audit completed."

# Stage 4: PySpark Preprocessing Pipeline & Parquet Generation
log ">>> STAGE 4: Executing PySpark Preprocessing (Cleaning & Parquet Generation)..."
"${PROJECT_ROOT}/scripts/spark_process.py" 2>&1 | tee -a "${LOG_DIR}/spark.log"
log "✓ PySpark Preprocessing & Parquet generation completed."

# Stage 5: Spark SQL Analytical Engine
log ">>> STAGE 5: Executing Spark SQL Analytical Queries (A through E)..."
"${PROJECT_ROOT}/scripts/spark_sql.py" 2>&1 | tee -a "${LOG_DIR}/sql.log"
log "✓ Spark SQL Analytics completed."

# Stage 6: Hadoop MapReduce on YARN
log ">>> STAGE 6: Executing Hadoop MapReduce Streaming Job on YARN..."
"${PROJECT_ROOT}/scripts/run_mapreduce.sh" 2>&1 | tee -a "${LOG_DIR}/mapreduce.log"
log "✓ Hadoop MapReduce completed."

# Stage 7: Baseline MLlib Forecasting
log ">>> STAGE 7: Executing Spark MLlib Baseline Regression Forecasting..."
"${PROJECT_ROOT}/scripts/forecasting.py" 2>&1 | tee -a "${LOG_DIR}/forecast.log"
log "✓ Spark MLlib Forecasting completed."

# Stage 8: Seed MongoDB Infrastructure Database
log ">>> STAGE 8: Seeding MongoDB Infrastructure Database..."
"${PROJECT_ROOT}/.venv/bin/python" "${PROJECT_ROOT}/mongodb/seed/seed_facilities.py" 2>&1 | tee -a "${LOG_DIR}/mongodb.log"
log "✓ MongoDB Infrastructure repository updated."

# Stage 9: Launch/Verify Dashboard
log ">>> STAGE 9: Launching Streamlit Interactive Dashboard..."
"${PROJECT_ROOT}/start_dashboard.sh" 2>&1 | tee -a "${LOG_DIR}/dashboard.log"
log "✓ Dashboard running at http://localhost:8501"

log "======================================================================"
log "MASTER PIPELINE COMPLETED SUCCESSFULLY"
log "All analytical outputs, Parquet tables, and logs generated."
log "======================================================================"
