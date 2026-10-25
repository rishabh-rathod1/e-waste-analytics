#!/usr/bin/env bash
# ==============================================================================
# Master Big Data Pipeline Shutdown Script
# Gracefully stops the Streamlit dashboard, Spark jobs, YARN, HDFS, and MongoDB
# ==============================================================================

set -eo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${SCRIPT_DIR}"
source "${PROJECT_ROOT}/config/project.env"

LOG_DIR="${PROJECT_ROOT}/output/logs"
mkdir -p "${LOG_DIR}"
PIPELINE_LOG="${LOG_DIR}/pipeline.log"

log() {
    local msg="[$(date '+%Y-%m-%d %H:%M:%S')] $1"
    echo "$msg" | tee -a "${PIPELINE_LOG}"
}

log "======================================================================"
log "STARTING MASTER E-WASTE PIPELINE & SERVICES SHUTDOWN"
log "======================================================================"

# 1. Stop Streamlit Dashboard
log ">>> STAGE 1: Stopping Streamlit Web Dashboard..."
if pgrep -f "streamlit.*app.py" >/dev/null; then
    pkill -15 -f "streamlit.*app.py" || true
    sleep 2
    if pgrep -f "streamlit.*app.py" >/dev/null; then
        log "Force-stopping lingering dashboard processes..."
        pkill -9 -f "streamlit.*app.py" || true
    fi
    log "✓ Streamlit Dashboard stopped (Port 8501 released)."
else
    log "Streamlit Dashboard was not running."
fi

# 2. Stop Any Active Spark / PySpark Processes
log ">>> STAGE 2: Checking and stopping active Spark workloads..."
if pgrep -f "org.apache.spark.deploy.SparkSubmit" >/dev/null; then
    pkill -15 -f "org.apache.spark.deploy.SparkSubmit" || true
    sleep 1
    log "✓ Active Spark applications terminated."
else
    log "No active Spark workloads found."
fi

# 3. Gracefully Stop YARN Daemons
log ">>> STAGE 3: Stopping YARN Daemons..."
if pgrep -f "proc_nodemanager" >/dev/null; then
    log "Stopping YARN NodeManager..."
    "${HADOOP_HOME}/bin/yarn" --daemon stop nodemanager || true
fi

if pgrep -f "proc_resourcemanager" >/dev/null; then
    log "Stopping YARN ResourceManager..."
    "${HADOOP_HOME}/bin/yarn" --daemon stop resourcemanager || true
fi
log "✓ YARN services stopped."

# 4. Gracefully Stop HDFS Daemons
log ">>> STAGE 4: Stopping HDFS Daemons..."
if pgrep -f "proc_secondarynamenode" >/dev/null; then
    log "Stopping HDFS SecondaryNameNode..."
    "${HADOOP_HOME}/bin/hdfs" --daemon stop secondarynamenode || true
fi

if pgrep -f "proc_datanode" >/dev/null; then
    log "Stopping HDFS DataNode..."
    "${HADOOP_HOME}/bin/hdfs" --daemon stop datanode || true
fi

if pgrep -f "proc_namenode" >/dev/null; then
    log "Stopping HDFS NameNode..."
    "${HADOOP_HOME}/bin/hdfs" --daemon stop namenode || true
fi
log "✓ HDFS services stopped."

# 5. Gracefully Stop MongoDB
log ">>> STAGE 5: Stopping MongoDB Database Daemon..."
if pgrep -f "mongod.*27017" >/dev/null; then
    # Graceful shutdown allowing lock file cleanup and journal flush
    pkill -15 -f "mongod.*27017" || true
    sleep 2
    if pgrep -f "mongod.*27017" >/dev/null; then
        pkill -9 -f "mongod.*27017" || true
    fi
    log "✓ MongoDB stopped (Port 27017 released)."
else
    log "MongoDB was not running."
fi

# 6. Post-Shutdown Verification
log ">>> STAGE 6: Verifying Clean Shutdown Status..."
sleep 1

REMAINING_JAVA=$("${JAVA_HOME}/bin/jps" 2>/dev/null | grep -E "NameNode|DataNode|SecondaryNameNode|ResourceManager|NodeManager" || true)
if [ -z "${REMAINING_JAVA}" ]; then
    log "✓ All Hadoop / YARN cluster daemons successfully shut down."
else
    log "WARNING: Lingering Java processes detected:"
    echo "${REMAINING_JAVA}" | tee -a "${PIPELINE_LOG}"
    log "Terminating lingering Hadoop daemons..."
    kill -9 $(echo "${REMAINING_JAVA}" | awk '{print $1}') 2>/dev/null || true
fi

log "Remaining Java processes:"
"${JAVA_HOME}/bin/jps" 2>/dev/null | tee -a "${PIPELINE_LOG}"

# Check port releases
PORTS_IN_USE=""
for port in 8501 9000 8088 27017 9870 9864; do
    if ss -tlnp 2>/dev/null | grep -q ":${port} "; then
        PORTS_IN_USE="${PORTS_IN_USE} ${port}"
    fi
done

if [ -z "${PORTS_IN_USE}" ]; then
    log "✓ All project ports (8501, 9000, 8088, 27017, 9870, 9864) are completely released."
else
    log "Note: Ports still in use:${PORTS_IN_USE}"
fi

log "======================================================================"
log "ALL E-WASTE PIPELINE SERVICES & DASHBOARD STOPPED SUCCESSFULLY"
log "======================================================================"
