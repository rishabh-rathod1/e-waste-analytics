# Demonstration & Presentation Screenshots Guide

This guide lists the exact 11 terminal commands and browser views to capture for project submission and university presentations.

---

### Screenshot 1: Dataset Discovery in Downloads
**Command**:
```bash
find ~/Downloads/DATASET -maxdepth 2 -type f -printf '%p (%k KB)\n'
```
*Expected*: Shows `WEEE_SF_cmp_version2.csv` (2,173,661 KB / ~2.22 GB).

---

### Screenshot 2: HDFS File Listing
**Command**:
```bash
source ~/ewaste-bigdata/config/project.env
hdfs dfs -ls -h /ewaste/raw/futuram
hdfs dfs -du -h /ewaste/raw/futuram
```
*Expected*: Shows `2.1 G /ewaste/raw/futuram/WEEE_SF_cmp_version2.csv`.

---

### Screenshot 3: Hadoop Cluster & YARN Nodes
**Command**:
```bash
source ~/ewaste-bigdata/config/project.env
jps
yarn node -list
```
*Expected*: Shows NameNode, DataNode, SecondaryNameNode, ResourceManager, NodeManager, and Total Nodes: 1 (RUNNING).

---

### Screenshot 4: Spark Schema & Total Records
**Command**:
```bash
cat ~/ewaste-bigdata/output/logs/spark.log | grep -A 20 "Dataset Schema"
```
*Expected*: Prints explicit schema and `Total Raw Records Read: 17,521,915`.

---

### Screenshot 5: PySpark Transformation & Parquet Generation
**Command**:
```bash
source ~/ewaste-bigdata/config/project.env
hdfs dfs -ls -h /ewaste/processed/parquet
```
*Expected*: Lists `yearly_ewaste`, `location_ewaste`, `category_ewaste`, `scenario_ewaste`, `flow_ewaste`.

---

### Screenshot 6: MapReduce Job Execution (100% Map / 100% Reduce)
**Command**:
```bash
cat ~/ewaste-bigdata/output/logs/mapreduce.log | grep -E "map [0-9]+% reduce|completed successfully|Top 10"
```
*Expected*: Shows `map 100% reduce 100%`, `Job completed successfully`, and Top 10 countries.

---

### Screenshot 7: Spark SQL Analytical Results
**Command**:
```bash
head -n 25 ~/ewaste-bigdata/output/logs/sql.log
```
*Expected*: Shows Query A (yearly generation) and Query B (high-waste nations).

---

### Screenshot 8: Spark MLlib Regression Forecast
**Command**:
```bash
cat ~/ewaste-bigdata/output/logs/forecast.log | grep -A 15 "MLlib Baseline Model Summary"
```
*Expected*: Shows Formula, Training Period, RMSE (313,674.50 Mg), R² (0.8410), and projections.

---

### Screenshot 9: MongoDB Recycling Infrastructure
**Command**:
```bash
head -n 25 ~/ewaste-bigdata/output/logs/mongodb.log
```
*Expected*: Shows 18 European treatment facilities inserted with indexes created.

---

### Screenshot 10: Streamlit Interactive Dashboard
**Browser URL**: `http://localhost:8501`  
*Action*: Capture Page 1 (Overview & KPIs) showing the KPI cards, generation trend, top 10 countries, and category pie chart.

---

### Screenshot 11: Pipeline & System Lineage
**Browser URL**: `http://localhost:8501`  
*Action*: Navigate to Page 8 (`8. Data & Pipeline Architecture`) showing the live status badges (HDFS ACTIVE, Spark ACTIVE, MapReduce COMPLETED, MongoDB CONNECTED) and the ASCII data lineage diagram.
