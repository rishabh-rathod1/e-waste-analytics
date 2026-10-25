#!/usr/bin/env bash
# Start Services Script
# Launches HDFS, YARN, and MongoDB daemons with session detachment (setsid)

set -eo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${SCRIPT_DIR}"
source "${PROJECT_ROOT}/config/project.env"

mkdir -p "${PROJECT_ROOT}/output/logs/hadoop"
mkdir -p "${PROJECT_ROOT}/output/logs/yarn"
mkdir -p "${BIGDATA_DATA}/hdfs/namenode"
mkdir -p "${BIGDATA_DATA}/hdfs/datanode"
mkdir -p "${BIGDATA_DATA}/hdfs/tmp"
mkdir -p "${MONGODB_DATA}"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

log "=== Starting Big Data Cluster Services ==="

# 1. Format NameNode if not already formatted
if [ ! -d "${BIGDATA_DATA}/hdfs/namenode/current" ]; then
    log "Formatting NameNode for first-time setup..."
    "${HADOOP_HOME}/bin/hdfs" namenode -format -force -nonInteractive
fi

# 2. Start HDFS Daemons using setsid
if ! pgrep -f "proc_namenode" >/dev/null; then
    log "Starting HDFS NameNode..."
    setsid "${HADOOP_HOME}/bin/hdfs" --daemon start namenode
fi

if ! pgrep -f "proc_datanode" >/dev/null; then
    log "Starting HDFS DataNode..."
    setsid "${HADOOP_HOME}/bin/hdfs" --daemon start datanode
fi

if ! pgrep -f "proc_secondarynamenode" >/dev/null; then
    log "Starting HDFS SecondaryNameNode..."
    setsid "${HADOOP_HOME}/bin/hdfs" --daemon start secondarynamenode
fi

# 3. Start YARN Daemons using setsid
if ! pgrep -f "proc_resourcemanager" >/dev/null; then
    log "Starting YARN ResourceManager..."
    setsid "${HADOOP_HOME}/bin/yarn" --daemon start resourcemanager
fi

if ! pgrep -f "proc_nodemanager" >/dev/null; then
    log "Starting YARN NodeManager..."
    setsid "${HADOOP_HOME}/bin/yarn" --daemon start nodemanager
fi

# 4. Start MongoDB
if ! pgrep -f "mongod.*27017" >/dev/null; then
    log "Starting MongoDB on localhost:27017..."
    setsid "${MONGODB_HOME}/bin/mongod" --dbpath "${MONGODB_DATA}" --logpath "${MONGODB_LOG}" --fork --bind_ip 127.0.0.1 --port 27017
else
    log "MongoDB is already running."
fi

# 5. Allow cluster initialization and verify
sleep 3
log "Active Java Processes (jps):"
"${JAVA_HOME}/bin/jps"

log "Testing HDFS root listing:"
"${HADOOP_HOME}/bin/hdfs" dfs -ls / || true

log "Testing YARN nodes:"
"${HADOOP_HOME}/bin/yarn" node -list || true

log "=== All Services Successfully Started ==="
