#!/usr/bin/env bash
# Stop Services Script
# Gracefully stops Streamlit dashboard, YARN, HDFS, and MongoDB daemons

set -eo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${SCRIPT_DIR}"
source "${PROJECT_ROOT}/config/project.env"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

log "=== Stopping Big Data Cluster Services & Dashboard ==="

# 1. Stop Dashboard
if pgrep -f "streamlit.*app.py" >/dev/null; then
    log "Stopping Streamlit Dashboard..."
    pkill -15 -f "streamlit.*app.py" || true
    sleep 1
    pkill -9 -f "streamlit.*app.py" 2>/dev/null || true
fi

# 2. Stop YARN
log "Stopping YARN NodeManager..."
"${HADOOP_HOME}/bin/yarn" --daemon stop nodemanager 2>/dev/null || true

log "Stopping YARN ResourceManager..."
"${HADOOP_HOME}/bin/yarn" --daemon stop resourcemanager 2>/dev/null || true

# 3. Stop HDFS
log "Stopping HDFS SecondaryNameNode..."
"${HADOOP_HOME}/bin/hdfs" --daemon stop secondarynamenode 2>/dev/null || true

log "Stopping HDFS DataNode..."
"${HADOOP_HOME}/bin/hdfs" --daemon stop datanode 2>/dev/null || true

log "Stopping HDFS NameNode..."
"${HADOOP_HOME}/bin/hdfs" --daemon stop namenode 2>/dev/null || true

# 4. Stop MongoDB
log "Stopping MongoDB..."
pkill -15 -f "mongod.*27017" || true
sleep 1
pkill -9 -f "mongod.*27017" 2>/dev/null || true

sleep 1
log "Active Java Processes remaining:"
"${JAVA_HOME}/bin/jps" 2>/dev/null || true

log "=== All Services & Dashboard Stopped Gracefully ==="
