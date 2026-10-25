# End-to-End Walkthrough & Execution Guide

**Project Title**: Big Data Analytics for Electronic Waste Generation Analysis & Recycling Infrastructure Optimization  
**Location**: `~/ewaste-bigdata`  
**Dataset**: FutuRaM European WEEE Stock & Flow Database (`~2.22 GB`, `17,521,915` records)

---

## Table of Contents
1. [Prerequisites & Environment Setup](#1-prerequisites--environment-setup)
2. [Quick Start (One-Command Run)](#2-quick-start-one-command-run)
3. [Step 1: Starting Cluster Services (HDFS, YARN, MongoDB)](#3-step-1-starting-cluster-services)
4. [Step 2: Ingesting Raw Dataset into HDFS](#4-step-2-ingesting-raw-dataset-into-hdfs)
5. [Step 3: Dataset Profiling & Data Quality Audit](#5-step-3-dataset-profiling--data-quality-audit)
6. [Step 4: PySpark ETL & Snappy Parquet Generation](#6-step-4-pyspark-etl--snappy-parquet-generation)
7. [Step 5: Distributed Hadoop MapReduce on YARN](#7-step-5-distributed-hadoop-mapreduce-on-yarn)
8. [Step 6: Spark SQL Analytical Engine](#8-step-6-spark-sql-analytical-engine)
9. [Step 7: Spark MLlib Baseline Forecasting](#9-step-7-spark-mllib-baseline-forecasting)
10. [Step 8: Seeding MongoDB Recycling Infrastructure](#10-step-8-seeding-mongodb-recycling-infrastructure)
11. [Step 9: Interactive Streamlit Dashboard & Maps](#11-step-9-interactive-streamlit-dashboard--maps)
12. [Step 10: Stopping Services](#12-step-10-stopping-services)
13. [Troubleshooting & Verification Cheat Sheet](#13-troubleshooting--verification-cheat-sheet)

---

## 1. Prerequisites & Environment Setup

All software is self-contained in user-space directories (`~/bigdata/software/` and `~/ewaste-bigdata/`):
- **Java 17 LTS**: `~/bigdata/software/jdk-17`
- **Hadoop 3.5.0**: `~/bigdata/software/hadoop`
- **Spark 4.2.0**: `~/bigdata/software/spark`
- **MongoDB 7.0**: `~/bigdata/software/mongodb`
- **Python 3.14**: Virtual environment in `~/ewaste-bigdata/.venv`

### Load Environment Variables
Before running any standalone command in a new terminal, always source the environment configuration:
```bash
cd ~/ewaste-bigdata
source config/project.env
```
This automatically configures `JAVA_HOME`, `HADOOP_HOME`, `SPARK_HOME`, `PATH`, and python paths.

---

## 2. Quick Start (One-Command Run)

To automatically verify services, ingest data, run PySpark, generate Parquet, run Spark SQL, execute MapReduce, generate MLlib forecasts, seed MongoDB, and launch the web dashboard:

```bash
cd ~/ewaste-bigdata
./run_pipeline.sh
```

Or using `make`:
```bash
make run
```

Then open your browser at:
👉 **[http://localhost:8501](http://localhost:8501)**

---

## 3. Step 1: Starting Cluster Services

Launches HDFS (NameNode, DataNode, SecondaryNameNode), YARN (ResourceManager, NodeManager), and MongoDB daemon using detached background sessions (`setsid`).

### Command:
```bash
./start_services.sh
```
*(Or `make start`)*

### Verification:
```bash
# 1. Verify Java processes
jps
```
**Expected Output**:
```text
NameNode
DataNode
SecondaryNameNode
ResourceManager
NodeManager
```

```bash
# 2. Verify YARN node status
yarn node -list
```
**Expected Output**:
```text
Total Nodes: 1
Node-Id: omarchy:44829  Node-State: RUNNING  Node-Http-Address: omarchy:8042
```

```bash
# 3. Verify MongoDB
pgrep -fl mongod
```

---

## 4. Step 2: Ingesting Raw Dataset into HDFS

Auto-detects the 2.22 GB CSV in `~/Downloads/DATASET/WEEE_SF_cmp_version2.csv` and uploads it into HDFS block storage.

### Command:
```bash
./scripts/ingest_hdfs.sh
```
*(Or `make ingest`)*

### Behind the Scenes:
- Creates HDFS directory hierarchy: `/ewaste/raw/futuram`, `/ewaste/processed/parquet`, `/ewaste/output/mapreduce`.
- Uploads the file via `hdfs dfs -put`.

### Verification:
```bash
hdfs dfs -ls -h /ewaste/raw/futuram
hdfs dfs -du -h /ewaste/raw/futuram
```
**Expected Output**:
```text
-rw-r--r--   1 rishabh supergroup      2.1 G /ewaste/raw/futuram/WEEE_SF_cmp_version2.csv
```

---

## 5. Step 3: Dataset Profiling & Data Quality Audit

Streams through all 17,521,915 rows chunk-by-chunk to audit data types, nulls, invalid numerics, distinct locations, categories, and scenarios without running out of RAM.

### Command:
```bash
.venv/bin/python scripts/profile_dataset.py
```
*(Or `make profile`)*

### Verification:
```bash
cat output/results/data_quality.csv
```
**Key Audit Metrics**:
- `total_records`: **17,521,915**
- `invalid_numeric_records`: **0**
- `year_range`: **2010 – 2050**
- `location_count`: **31 European Nations**
- `scenario_count`: **4 (OBS, BAU, REC, CIR)**
- `stock_flow_count`: **8 flows**
- `category_count`: **7 EU WEEE Categories**

---

## 6. Step 4: PySpark ETL & Snappy Parquet Generation

Reads the raw CSV from HDFS, applies schema validation, handles the hierarchical structure by filtering Level-1 summary rows (`Layer 2` is blank), and exports optimized Snappy-compressed columnar Parquet tables.

### Command:
```bash
./scripts/spark_process.py
```
*(Or `make spark`)*

### Behind the Scenes:
- Reads `hdfs://localhost:9000/ewaste/raw/futuram/WEEE_SF_cmp_version2.csv`.
- Isolates 139,963 Level-1 summary rows, completely eliminating double counting between equipment and sub-assemblies.
- Writes partitioned Parquet datasets to HDFS and mirrors them locally to `data/processed/`.

### Verification:
```bash
hdfs dfs -ls -h /ewaste/processed/parquet
ls -lah data/processed/
```
**Expected Output Tables**:
- `yearly_ewaste`: Aggregated yearly generation, collection, and POM.
- `location_ewaste`: Generation figures per country.
- `category_ewaste`: Breakdown across 7 EU WEEE categories.
- `scenario_ewaste`: Trajectories across BAU, REC, and CIR.
- `flow_ewaste`: Stock and flow mass balance.

---

## 7. Step 5: Distributed Hadoop MapReduce on YARN

Executes a classic distributed MapReduce job using Hadoop Streaming over YARN to compute `Location → Total Generated WEEE`.

### Command:
```bash
./scripts/run_mapreduce.sh
```
*(Or `make mapreduce`)*

### Components:
- **Mapper** ([`mapreduce/mapper.py`](file:///home/rishabh/ewaste-bigdata/mapreduce/mapper.py)): Parses CSV rows, filters for generation flows at Layer 1 level, and emits `Location \t Value`.
- **Reducer** ([`mapreduce/reducer.py`](file:///home/rishabh/ewaste-bigdata/mapreduce/reducer.py)): Aggregates total metric tons (Mg) per country.

### Verification:
```bash
# Check terminal output logs for 100% completion
cat output/logs/mapreduce.log | grep -E "map 100% reduce|Job completed"

# View top 10 locations generated by MapReduce
cat output/results/mapreduce_location_totals.csv | head -n 11
```
**Expected Results**:
```text
Location,Total_WEEE_Generated_Mg
GBR,302467302.2682
DEU,299730670.3343
FRA,233664421.2335
ITA,191617288.7156
ESP,158197115.7138
POL,81572628.7004
```

---

## 8. Step 6: Spark SQL Analytical Engine

Registers SQL views over the Parquet datasets and executes relational queries to solve analytical questions A through E.

### Command:
```bash
./scripts/spark_sql.py
```
*(Or `make sql`)*

### Analytical Queries Executed:
- **Query A**: Year-wise WEEE Generation (Historical 2010–2022 vs Projected).
- **Query B**: Top 10 High-Waste Generating Nations.
- **Query C**: Category Distribution (Percentage shares of EU Categories 1–6).
- **Query D**: Scenario Comparison (Cumulative impact of BAU vs REC vs CIR).
- **Query E**: Stock/Flow Balance Analysis.

### Verification:
```bash
cat output/results/sql_top_locations.csv
cat output/results/sql_category_distribution.csv
cat output/results/sql_scenario_comparison.csv
```

---

## 9. Step 7: Spark MLlib Baseline Forecasting

Fits a distributed Linear Regression model using `VectorAssembler` on historical Observed generation (2010–2022) to establish baseline projections up to 2035.

### Command:
```bash
./scripts/forecasting.py
```
*(Or `make forecast`)*

### Model Summary:
- **Algorithm**: Spark MLlib `LinearRegression`
- **Formula**: $\text{Total\_WEEE\_Mg} = 192,770.41 \times \text{Year} - 374,025,097.97$
- **Goodness of Fit ($R^2$)**: **0.8410**
- **RMSE**: **313,674.50 Mg**

### Verification:
```bash
cat output/results/forecast.csv | tail -n 12
```

---

## 10. Step 8: Seeding MongoDB Recycling Infrastructure

Populates MongoDB collection `facilities` in database `ewaste_db` with European specialized treatment plants, shredding centers, smelters, and precious metal recovery hubs.

### Command:
```bash
.venv/bin/python mongodb/seed/seed_facilities.py
```
*(Or `make mongo`)*

### Verification:
```bash
# Query seeded facilities directly from MongoDB
.venv/bin/python -c "
from pymongo import MongoClient
c = MongoClient('mongodb://localhost:27017')
db = c['ewaste_db']
print('Total facilities in MongoDB:', db.facilities.count_documents({}))
for doc in db.facilities.find({}, {'name': 1, 'city': 1, 'annual_capacity_mg': 1}).limit(5):
    print(f\"  - {doc['name']} ({doc['city']}): {doc['annual_capacity_mg']:,.0f} Mg/yr\")
c.close()
"
```
**Expected Output**:
- Total facilities: **18**
- Total capacity registered: **1,605,000 Mg/year**

---

## 11. Step 9: Interactive Streamlit Dashboard & Maps

Launches the professional multi-page Streamlit web analytics platform.

### Command:
```bash
./start_dashboard.sh
```
*(Or `make dashboard`)*

### Access URL:
Open your browser to: **[http://localhost:8501](http://localhost:8501)**

---

### 🗺️ Exploring the 8 Dashboard Pages & Interactive Maps:

#### Page 1 — Overview & KPIs
- Top metric cards: 17.52M HDFS raw records, 31 European nations, 2010–2050 time horizon.
- Interactive multi-line chart comparing Generation vs Collection vs Put-on-Market.
- Horizontal bar chart of top 10 generating nations.
- Donut chart of EU WEEE category shares.

#### Page 2 — Geographic & Location Analysis (European Waste Map)
- **Interactive European Map**: Features an interactive Plotly geographical map displaying country bubble markers sized and colored by total WEEE generation tonnage.
- Hover over any European country (e.g. Germany, UK, France, Italy, Spain) to inspect exact tonnage and ISO code.
- Filter by policy scenario (`OBS`, `BAU`, `REC`, `CIR`) and sort rankings ascending or descending.
- Comprehensive national ranking data table with formatted metric tonnages.

#### Page 3 — Category Distribution
- Horizontal ranking of all 7 EU Layer 1 categories.
- Detailed percentage share indicators.
- Time-series stacked area chart displaying the historical and projected evolution of each category from 2010 to 2050.

#### Page 4 — Scenario Comparison (BAU vs REC vs CIR)
- Comparative trajectories for Business-As-Usual, Recycling Targets, and Circular Economy.
- Cumulative impact cards illustrating that Circular Economy policy avoids **161.46 Million Mg** of waste between 2023 and 2050.

#### Page 5 — Stock/Flow Mass Balance
- Distribution bar chart of active equipment in use (`WEEE_EEE_stock`: 23.37 Billion Mg), Put-on-Market sales (`WEEE_EEEPOM`: 2.0 Billion Mg), dedicated collection, and complementary streams (shredder metal scrap, undocumented, MSW, exports).
- Complete taxonomy definition cards.

#### Page 6 — MLlib Baseline Forecast
- Visualization comparing observed historical actuals (2010–2022) with the Spark MLlib linear regression line (2023–2035).
- Metric cards showing RMSE (**313,674 Mg**) and $R^2$ (**0.8410**).
- Searchable forecast projection table.

#### Page 7 — Recycling Infrastructure (MongoDB Facility Map)
- **Interactive Treatment Facility Map**: Displays 18 major European e-waste recycling, smelting, and depollution plants loaded dynamically from MongoDB.
- Markers are colored by `facility_type` (Smelters, Mechanical Shredders, Metal Recovery, Battery Recycling, Cooling Depollution) and sized by annual capacity.
- Sidebar filters by Country, Facility Type, and minimum capacity.
- Complete facility metadata table showing cities, capacities, and operational statuses.

#### Page 8 — Data & Pipeline Architecture
- Live system status cards:
  - `HDFS 3.5.0`: **ACTIVE**
  - `Spark 4.2.0`: **ACTIVE**
  - `MapReduce`: **COMPLETED**
  - `MongoDB 7.0`: **CONNECTED**
- ASCII Data Lineage pipeline diagram.
- Live log viewer to inspect terminal outputs from HDFS ingestion, PySpark, MapReduce, Spark SQL, and MLlib.

---

## 12. Step 10: Graceful Pipeline & Services Shutdown

To gracefully shut down the **entire platform** — including the Streamlit dashboard (port 8501), any active Spark jobs, YARN daemons, HDFS daemons, and MongoDB (port 27017) — execute the master shutdown script:

### Command:
```bash
./stop_pipeline.sh
```
*(Or `make stop`)*

### Behind the Scenes:
1. **Shuts down Streamlit Web Dashboard**: Sends `SIGTERM` followed by clean port 8501 release.
2. **Terminates Active Spark Jobs**: Checks for any background PySpark / SparkSubmit processes and closes them.
3. **Stops YARN Cluster**: Shuts down NodeManager and ResourceManager safely.
4. **Stops HDFS Cluster**: Shuts down SecondaryNameNode, DataNode, and NameNode, writing clean checkpoint states.
5. **Stops MongoDB Daemon**: Sends graceful termination allowing journal commit and lockfile cleanup.
6. **Port & Process Audit**: Checks ports (8501, 9000, 8088, 27017, 9870, 9864) and `jps` to confirm clean exit.

### Verification:
```bash
# Check that all Hadoop and Spark Java processes have exited
jps

# Check that MongoDB is stopped
pgrep -fl mongod

# Check that Streamlit is stopped
pgrep -fl streamlit
```
All services and the dashboard will be shut down gracefully with zero orphan processes.

---

## 13. Troubleshooting & Verification Cheat Sheet

| Task / Problem | Verification Command | Solution / Remedy |
|---|---|---|
| **Check running services** | `jps` | Must show `NameNode`, `DataNode`, `SecondaryNameNode`, `ResourceManager`, `NodeManager`. If missing, run `./start_services.sh`. |
| **Check YARN cluster** | `yarn node -list` | Confirms active NodeManagers. Check `output/logs/yarn/` if zero nodes reported. |
| **Check HDFS raw file** | `hdfs dfs -ls -h /ewaste/raw/futuram/` | Must show `2.1 G WEEE_SF_cmp_version2.csv`. If missing, run `./scripts/ingest_hdfs.sh`. |
| **Check Parquet tables** | `hdfs dfs -ls /ewaste/processed/parquet/` | Lists 5 parquet directories. If missing, run `./scripts/spark_process.py`. |
| **Check MongoDB status** | `pgrep -fl mongod` | Ensure mongod is running on port 27017. If not, run `./start_services.sh`. |
| **Dashboard port conflict** | `pkill -f "streamlit.*app.py"` | Kills any existing dashboard session, then run `./start_dashboard.sh`. |
| **Check Stage Logs** | `ls -la output/logs/` | Detailed logs: `hdfs.log`, `spark.log`, `mapreduce.log`, `sql.log`, `forecast.log`, `mongodb.log`, `dashboard.log`. |
| **Reboot Recovery** | `cd ~/ewaste-bigdata && ./start_services.sh && ./start_dashboard.sh` | Restarts all background services and opens the UI immediately after system reboot. |
