#!/usr/bin/env bash
# MapReduce Execution Script
# Runs Hadoop Streaming Job over WEEE data on YARN / HDFS

set -eo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "${SCRIPT_DIR}")"
source "${PROJECT_ROOT}/config/project.env"

LOG_FILE="${PROJECT_ROOT}/output/logs/mapreduce.log"
mkdir -p "${PROJECT_ROOT}/output/logs" "${PROJECT_ROOT}/output/results"

log() {
    local msg="[$(date '+%Y-%m-%d %H:%M:%S')] $1"
    echo "$msg" | tee -a "${LOG_FILE}"
}

log "=== Starting Hadoop MapReduce Stage ==="

# 1. Locate Hadoop Streaming Jar
STREAMING_JAR=$(find "${HADOOP_HOME}/share/hadoop/tools/lib" -name "hadoop-streaming-*.jar" 2>/dev/null | head -n 1)
if [ -z "${STREAMING_JAR}" ] || [ ! -f "${STREAMING_JAR}" ]; then
    log "ERROR: Hadoop streaming jar not found under ${HADOOP_HOME}/share/hadoop/tools/lib"
    exit 1
fi
log "Located Hadoop Streaming JAR: ${STREAMING_JAR}"

# 2. Input and Output Paths
INPUT_HDFS="/ewaste/raw/futuram/WEEE_SF_cmp_version2.csv"
OUTPUT_HDFS="/ewaste/output/mapreduce/location_totals"

# Check if input exists in HDFS
if ! hdfs dfs -test -e "${INPUT_HDFS}"; then
    log "ERROR: Input file ${INPUT_HDFS} does not exist in HDFS. Run ./scripts/ingest_hdfs.sh first."
    exit 1
fi

# Clean previous output
hdfs dfs -rm -r -f "${OUTPUT_HDFS}" >/dev/null 2>&1 || true

log "Submitting MapReduce Job..."
log "Mapper:  ${PROJECT_ROOT}/mapreduce/mapper.py"
log "Reducer: ${PROJECT_ROOT}/mapreduce/reducer.py"
log "Input:   ${INPUT_HDFS}"
log "Output:  ${OUTPUT_HDFS}"

# Execute MapReduce Streaming
hadoop jar "${STREAMING_JAR}" \
    -D mapreduce.job.name="EWaste-Location-Totals" \
    -D mapreduce.job.reduces=1 \
    -files "${PROJECT_ROOT}/mapreduce/mapper.py","${PROJECT_ROOT}/mapreduce/reducer.py" \
    -mapper "python3 mapper.py" \
    -reducer "python3 reducer.py" \
    -input "${INPUT_HDFS}" \
    -output "${OUTPUT_HDFS}" 2>&1 | tee -a "${LOG_FILE}"

log "MapReduce Job finished. Inspecting results..."

# Verify output in HDFS
hdfs dfs -ls "${OUTPUT_HDFS}" | tee -a "${LOG_FILE}"

# Export to local results directory
LOCAL_OUTPUT="${PROJECT_ROOT}/output/results/mapreduce_location_totals.csv"
echo "Location,Total_WEEE_Generated_Mg" > "${LOCAL_OUTPUT}"
hdfs dfs -cat "${OUTPUT_HDFS}/part-*" | tr '\t' ',' >> "${LOCAL_OUTPUT}"

log "MapReduce Results saved to: ${LOCAL_OUTPUT}"
log "Top 10 Locations from MapReduce Output:"
sort -t',' -k2 -nr "${LOCAL_OUTPUT}" | head -n 11 | tee -a "${LOG_FILE}"

log "=== Hadoop MapReduce Stage Completed Successfully ==="
