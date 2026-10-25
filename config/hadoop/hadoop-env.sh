# Hadoop Environment Script
export JAVA_HOME="/home/rishabh/bigdata/software/jdk-17"
export HADOOP_HOME="/home/rishabh/bigdata/software/hadoop"
export HADOOP_CONF_DIR="/home/rishabh/ewaste-bigdata/config/hadoop"
export HADOOP_LOG_DIR="/home/rishabh/ewaste-bigdata/output/logs/hadoop"
export YARN_LOG_DIR="/home/rishabh/ewaste-bigdata/output/logs/yarn"

# JVM options for Java 17 compatibility with Hadoop 3
export HADOOP_OPTS="$HADOOP_OPTS --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED --add-opens java.base/java.util.concurrent=ALL-UNNAMED --add-opens java.base/java.net=ALL-UNNAMED --add-opens java.base/java.text=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/java.nio.channels=ALL-UNNAMED"
