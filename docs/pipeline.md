# End-to-End Pipeline Execution & Data Lineage Guide

This document specifies the operational lifecycle, data lineage, execution dependencies, and log locations for the WEEE Big Data Analytics Platform.

---

## 1. Data Lineage Flow

```
Raw CSV (~2.22 GB)
  │
  ├─► [HDFS Ingestion] ────────► /ewaste/raw/futuram/WEEE_SF_cmp_version2.csv
  │
  ├─► [Profiling & Audit] ─────► output/results/data_quality.csv
  │
  ├─► [Hadoop MapReduce] ──────► /ewaste/output/mapreduce/ ──► output/results/mapreduce_location_totals.csv
  │
  └─► [PySpark Preprocessing] ──► /ewaste/processed/parquet/
                                   ├─ yearly_ewaste   ──► output/results/yearly_ewaste.csv
                                   ├─ location_ewaste ──► output/results/top_locations.csv
                                   ├─ category_ewaste ──► output/results/category_summary.csv
                                   ├─ scenario_ewaste ──► output/results/scenario_summary.csv
                                   └─ flow_ewaste     ──► output/results/flow_summary.csv
                                           │
                                           ├─► [Spark SQL Engine] ──► output/results/sql_*.csv
                                           │
                                           └─► [Spark MLlib Model] ──► output/results/forecast.csv
                                                       │
MongoDB (Facilities Database) ─────────────────────────┼─► [Streamlit UI (localhost:8501)]
```

---

## 2. Stage-by-Stage Execution Reference

| Stage | Script / Command | Primary Input | Primary Output | Log File |
|---|---|---|---|---|
| **0. Service Init** | `./start_services.sh` | Cluster configs | Active HDFS, YARN, Mongo | `output/logs/hadoop/`, `output/logs/yarn/` |
| **1. Ingestion** | `./scripts/ingest_hdfs.sh` | Local CSV (`~/Downloads`) | HDFS `/ewaste/raw/` | `output/logs/hdfs.log` |
| **2. Profiling** | `scripts/profile_dataset.py` | Local CSV | `output/results/data_quality.csv` | `output/logs/profile.log` |
| **3. Preprocessing** | `./scripts/spark_process.py` | HDFS `/ewaste/raw/` | HDFS `/ewaste/processed/parquet/` | `output/logs/spark.log` |
| **4. Spark SQL** | `./scripts/spark_sql.py` | HDFS Parquet | `output/results/sql_*.csv` | `output/logs/sql.log` |
| **5. MapReduce** | `./scripts/run_mapreduce.sh` | HDFS `/ewaste/raw/` | `/ewaste/output/mapreduce/` | `output/logs/mapreduce.log` |
| **6. Forecasting** | `./scripts/forecasting.py` | HDFS Parquet | `output/results/forecast.csv` | `output/logs/forecast.log` |
| **7. Mongo Seed** | `mongodb/seed/seed_facilities.py` | Seed JSON | MongoDB `facilities` | `output/logs/mongodb.log` |
| **8. Dashboard** | `./start_dashboard.sh` | Parquet / CSV / Mongo | Web UI (`localhost:8501`) | `output/logs/dashboard.log` |

---

## 3. One-Click Execution
The entire multi-stage pipeline is executed sequentially using:
```bash
./run_pipeline.sh
```
Or via Makefile:
```bash
make run
```
