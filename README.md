# Big Data Analytics for Electronic Waste Generation Analysis & Recycling Infrastructure Optimization

A production-grade Big Data engineering and analytics platform built from scratch on Omarchy Linux to analyze large-scale European WEEE (Waste Electrical and Electronic Equipment) material flows and optimize recycling infrastructure.

---

## 📌 Project Overview
- **Domain**: Environmental Big Data Analytics & Circular Economy Infrastructure
- **Dataset**: European FutuRaM WEEE Material Flow Database (~2.22 GB, 17,521,915 records)
- **Geographic Coverage**: 31 European nations (EU27 + GBR, CHE, NOR, ISL)
- **Time Horizon**: 2010 – 2050 (Historical observed data + BAU, REC, CIR policy projections)
- **Primary Engines**: Apache Hadoop 3.5.0 (HDFS & YARN), Apache Spark 4.2.0 (PySpark, Spark SQL, MLlib), MongoDB 7.0 Community, Streamlit UI.

---

## 🏛️ System Architecture

```
                  ┌──────────────────────────────┐
                  │ FutuRaM Raw WEEE CSV         │ ~/Downloads/DATASET/WEEE_SF_cmp_version2.csv
                  │ (2.12 GB, 17,521,915 rows)   │
                  └──────────────┬───────────────┘
                                 │
                                 │ Ingestion (hdfs dfs -put)
                                 ▼
                  ┌──────────────────────────────┐
                  │ Hadoop HDFS 3.5.0 Storage    │ hdfs://localhost:9000/ewaste/raw/futuram/
                  └──────────────┬───────────────┘
                                 │
         ┌───────────────────────┴───────────────────────┐
         ▼                                               ▼
┌─────────────────────────────┐           ┌─────────────────────────────┐
│ Hadoop MapReduce on YARN    │           │ PySpark Preprocessing       │
│ Hadoop Streaming            │           │ Explicit Schema & Cleaning  │
│ mapper.py / reducer.py      │           │ Hierarchical Deduplication  │
│ Location → Total WEEE       │           │ Snappy Columnar Parquet     │
└──────────────┬──────────────┘           └──────────────┬──────────────┘
               │                                         │
               ▼                                         ▼
┌─────────────────────────────┐           ┌─────────────────────────────┐
│ MapReduce Output Table      │           │ Parquet Analytical Views    │
│ /ewaste/output/mapreduce/   │           │ /ewaste/processed/parquet/  │
└─────────────────────────────┘           └──────────────┬──────────────┘
                                                         │
                         ┌───────────────────────────────┼───────────────────────────────┐
                         ▼                               ▼                               ▼
          ┌─────────────────────────────┐ ┌─────────────────────────────┐ ┌─────────────────────────────┐
          │ Spark SQL Analytics         │ │ Spark MLlib Forecasting     │ │ MongoDB 7.0 Database        │
          │ Queries A, B, C, D, E       │ │ Linear Regression           │ │ Recycling Facilities        │
          │ Cross-country & Category    │ │ Projection 2023 - 2035      │ │ Geospatial 2dsphere index   │
          └──────────────┬──────────────┘ └──────────────┬──────────────┘ └──────────────┬──────────────┘
                         │                               │                               │
                         └───────────────────────┬───────┴───────────────────────────────┘
                                                 ▼
                                  ┌─────────────────────────────┐
                                  │ Interactive Streamlit UI    │ http://localhost:8501
                                  │ 8 Multi-Page Analytics      │
                                  └─────────────────────────────┘
```

---

## 💻 Prerequisites & Environment

This project is self-contained in user-space (`~/bigdata/software/` and `~/ewaste-bigdata/`):
- **OS**: Linux (Omarchy / Arch-based, x86_64)
- **Java**: Eclipse Temurin OpenJDK 17 LTS (`jdk-17`)
- **Hadoop**: Apache Hadoop 3.5.0 (HDFS, YARN, MapReduce)
- **Spark**: Apache Spark 4.2.0 (PySpark, Spark SQL, MLlib)
- **Database**: MongoDB 7.0.14 Community Server
- **Python**: Python 3.14+ Virtual Environment (`.venv`)

---

## 🚀 Quick Start: Running the Complete Pipeline

Run the master pipeline script to execute all stages end-to-end:
```bash
cd ~/ewaste-bigdata
./run_pipeline.sh
```
Or via Makefile:
```bash
make run
```

### Starting the Interactive Dashboard
```bash
./start_dashboard.sh
```
Open **[http://localhost:8501](http://localhost:8501)** in your web browser.

---

## 🛠️ Step-by-Step Stage Breakdown

### 1. Service Control
Start all cluster daemons (HDFS NameNode, DataNode, SecondaryNameNode, YARN ResourceManager, NodeManager, MongoDB):
```bash
./start_services.sh
```
Stop all cluster services and dashboard gracefully:
```bash
./stop_pipeline.sh
# (or: make stop)
```

### 2. Dataset Ingestion into HDFS
Auto-discovers `~/Downloads/DATASET/WEEE_SF_cmp_version2.csv` and uploads it to HDFS:
```bash
./scripts/ingest_hdfs.sh
hdfs dfs -ls -h /ewaste/raw/futuram
```

### 3. Dataset Profiling & Data Quality Audit
Streams through all 17.5M records and audits schema, null counts, and distinct dimensions:
```bash
.venv/bin/python scripts/profile_dataset.py
cat output/results/data_quality.csv
```

### 4. PySpark Preprocessing & Parquet Generation
Reads raw CSV from HDFS, applies hierarchical deduplication (filtering for Level-1 summary rows), and writes Snappy-compressed Parquet tables:
```bash
./scripts/spark_process.py
hdfs dfs -ls -h /ewaste/processed/parquet
```

### 5. Spark SQL Analytical Queries
Executes analytical SQL queries across yearly, country, category, and scenario views:
```bash
./scripts/spark_sql.py
```

### 6. Hadoop MapReduce on YARN
Runs a distributed Hadoop Streaming MapReduce job computing Location $\rightarrow$ Total WEEE Generation:
```bash
./scripts/run_mapreduce.sh
```

### 7. Spark MLlib Baseline Forecasting
Fits a linear regression model on historical OBS generation (2010–2022) and forecasts to 2035:
```bash
./scripts/forecasting.py
```

### 8. MongoDB Infrastructure Repository
Seeds European treatment, smelting, depollution, and metal recovery facilities:
```bash
.venv/bin/python mongodb/seed/seed_facilities.py
```

---

## 📊 Analytical Findings Summary

1. **Total Waste Processed**: 17,521,915 rows cleanly ingested with 0 invalid records.
2. **Top High-Waste Nations**:
   - 🇬🇧 **United Kingdom (GBR)**: 302.47 Million Mg
   - 🇩🇪 **Germany (DEU)**: 299.73 Million Mg
   - 🇫🇷 **France (FRA)**: 233.66 Million Mg
   - 🇮🇹 **Italy (ITA)**: 191.62 Million Mg
   - 🇪🇸 **Spain (ESP)**: 158.20 Million Mg
3. **Category Breakdown**:
   - `WEEE_Cat4a` (Large Household Appliances): **38.53%**
   - `WEEE_Cat5` (Small Equipment): **27.58%**
   - `WEEE_Cat1` (Temperature Exchange): **16.04%**
   - `WEEE_Cat6` (Small IT & Telecom): **6.51%**
   - `WEEE_Cat4b` (Photovoltaic Panels): **5.48%**
   - `WEEE_Cat2` (Screens & Monitors): **5.07%**
   - `WEEE_Cat3` (Lamps): **0.80%**
4. **Policy Scenario Impact (2023 – 2050)**:
   - **BAU** (Business As Usual): 616.99 Million Mg
   - **REC** (Recycling Targets): 518.64 Million Mg
   - **CIR** (Circular Economy): 455.52 Million Mg (**161.46 Million Mg waste avoided!**)
5. **MLlib Regression Baseline**:
   - Formula: $\text{Total\_WEEE} = 192,770.41 \times \text{Year} - 374,025,097.97$
   - $R^2 = 0.8410$, $\text{RMSE} = 313,674.50\text{ Mg}$

---

## 📁 Directory Structure
```
ewaste-bigdata/
├── config/
│   ├── project.env                  # Central environment variables
│   ├── hadoop/                      # core-site, hdfs-site, yarn-site, mapred-site
│   └── spark/                       # spark-defaults.conf, spark-env.sh
├── data/
│   ├── raw/                         # Raw local data references
│   └── processed/                   # Local Parquet mirrors for dashboard
├── scripts/
│   ├── inspect_dataset.py           # Auto-detection of local datasets
│   ├── ingest_hdfs.sh               # HDFS ingestion automation
│   ├── profile_dataset.py           # High-throughput data quality profiler
│   ├── spark_process.py             # PySpark preprocessing launcher
│   ├── spark_sql.py                 # Spark SQL query launcher
│   ├── run_mapreduce.sh             # Hadoop MapReduce streaming launcher
│   └── forecasting.py               # MLlib regression launcher
├── mapreduce/
│   ├── mapper.py                    # MapReduce Python mapper
│   └── reducer.py                   # MapReduce Python reducer
├── spark/
│   ├── preprocessing.py             # PySpark ETL and deduplication pipeline
│   ├── sql_queries.py               # Analytical Spark SQL queries A-E
│   └── forecasting.py               # Spark MLlib regression model
├── dashboard/
│   └── app.py                       # 8-Page interactive Streamlit web dashboard
├── mongodb/
│   ├── schema/facility_schema.json  # JSON Schema for recycling facilities
│   └── seed/seed_facilities.py      # MongoDB infrastructure seeder
├── output/
│   ├── logs/                        # Detailed logs for all pipeline stages
│   ├── results/                     # Exported analytical CSV reports
│   └── screenshots/GUIDE.md         # Capture instructions for presentations
├── docs/
│   ├── architecture.md              # In-depth architectural blueprint
│   ├── data_dictionary.md           # Schema, flow taxonomy, and category mappings
│   ├── DA2_results.md               # Digital Assignment 2 experimental report
│   └── pipeline.md                  # Data lineage and execution guide
├── run_pipeline.sh                  # Single-command end-to-end runner
├── start_services.sh                # Cluster launch script
├── stop_services.sh                 # Cluster shutdown script
├── start_dashboard.sh               # Streamlit launch script
├── requirements.txt                 # Frozen Python dependencies
├── Makefile                         # Unified make targets
└── README.md                        # Master project documentation
```

---

## 🔧 Troubleshooting & Common Commands

| Issue | Diagnostic & Resolution |
|---|---|
| **HDFS connection refused on port 9000** | Run `./start_services.sh` to ensure NameNode & DataNode are active. Check `output/logs/hadoop/`. |
| **YARN ResourceManager not ready** | Check `output/logs/yarn/` and verify node status with `yarn node -list`. |
| **MongoDB connection failed** | Ensure mongod is running on port 27017: `pgrep -fl mongod`. Start via `./start_services.sh`. |
| **Spark memory issues** | Memory is configured for 4GB driver / 4GB executor in `config/spark/spark-defaults.conf`. Adjust if running on smaller machines. |
| **Port 8501 in use** | Kill existing dashboard instances: `pkill -f "streamlit.*app.py"`, then re-run `./start_dashboard.sh`. |
