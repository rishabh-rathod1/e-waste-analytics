# Digital Assignment 2 (DA2) — Experimental Results & Execution Report

**Project Title**: Big Data Analytics for Electronic Waste Generation Analysis and Recycling Infrastructure Optimization  
**Student Environment**: Omarchy Linux (Kernel 7.2.3, 12 CPU cores, 14 GB RAM)  
**Execution Timestamp**: September 2026  

---

## 1. Project Overview
This project presents an industrial-scale Big Data platform implementing distributed ingestion, storage, processing, querying, predictive forecasting, and visualization of Waste Electrical and Electronic Equipment (WEEE) across 31 European nations. Built completely from scratch without external managed services, the pipeline demonstrates Apache Hadoop HDFS, Hadoop YARN Streaming MapReduce, Apache Spark, Spark SQL, Spark MLlib, MongoDB, and an interactive Streamlit UI.

---

## 2. Dataset Audit & Characteristics
- **Primary Source**: FutuRaM European WEEE Stock and Flow Database (`WEEE_SF_cmp_version2.csv`)
- **File Size**: 2.22 GB (2,225,829,198 bytes)
- **Total Records Processed**: 17,521,915 rows
- **Schema Columns (12)**: Waste Stream, Location, Year, Scenario, additionalSpecification, Stock/Flow ID, Layer 1, Layer 2, Layer 3, Layer 4, Value, Unit
- **Invalid Numeric Rows**: 0
- **Time Horizon**: 2010 – 2050 (Historical OBS: 2010–2022; Policy Scenarios: 2023–2050)
- **Geographic Scope**: 31 countries (EU27 + GBR + CHE + NOR + ISL)

---

## 3. Environment & Software Stack Setup
All services were installed and executed in clean, user-space directories (`~/bigdata/software/` and `~/ewaste-bigdata/`):
- **Java**: Eclipse Temurin OpenJDK 17.0.12+7 (`JAVA_HOME=/home/rishabh/bigdata/software/jdk-17`)
- **Hadoop**: Apache Hadoop 3.5.0 (`HADOOP_HOME=/home/rishabh/bigdata/software/hadoop`)
- **Spark**: Apache Spark 4.2.0 (`SPARK_HOME=/home/rishabh/bigdata/software/spark`)
- **MongoDB**: MongoDB Community Server 7.0.14 (`port 27017`)
- **Python**: Python 3.14.7 Virtual Environment (`.venv`) with PySpark, PyArrow, Pandas, Plotly, Streamlit, PyMongo, Scikit-Learn.

---

## 4. HDFS Ingestion Phase
- **Input Path**: `/home/rishabh/Downloads/DATASET/WEEE_SF_cmp_version2.csv` (2.12 GB)
- **HDFS Target**: `hdfs://localhost:9000/ewaste/raw/futuram/WEEE_SF_cmp_version2.csv`
- **Replication Factor**: 1 (optimized for single-node cluster)
- **Ingestion Time**: ~20 seconds
- **Verification Command**:
  ```bash
  hdfs dfs -ls -h /ewaste/raw/futuram/WEEE_SF_cmp_version2.csv
  # Output: -rw-r--r-- 1 rishabh supergroup 2.1 G /ewaste/raw/futuram/WEEE_SF_cmp_version2.csv
  ```

---

## 5. Hadoop MapReduce Execution
- **Implementation**: Hadoop Streaming (`hadoop-streaming-3.5.0.jar`)
- **Mapper**: `mapreduce/mapper.py` (filters WEEE generation at Layer 1 level, emits `Location \t Value`)
- **Reducer**: `mapreduce/reducer.py` (sums total generated WEEE per location)
- **Execution Log Highlights**:
  - `Job ID`: `job_1790171098973_0001`
  - `Input Splits`: 17 splits over 2.22 GB
  - `Map input records`: 17,521,916
  - `Map output records`: 77,420
  - `Reduce output records`: 31 countries
  - `Status`: `map 100% reduce 100% completed successfully`
- **Top Locations Produced by MapReduce**:
  1. GBR: 302,467,302.27 Mg
  2. DEU: 299,730,670.33 Mg
  3. FRA: 233,664,421.23 Mg
  4. ITA: 191,617,288.72 Mg
  5. ESP: 158,197,115.71 Mg
  6. POL: 81,572,628.70 Mg
  7. NLD: 67,043,714.76 Mg
  8. ROU: 40,873,837.61 Mg
  9. BEL: 40,263,669.72 Mg
  10. CHE: 35,515,035.04 Mg

---

## 6. PySpark Preprocessing & Parquet Generation
- **Script**: `spark/preprocessing.py`
- **Elapsed Time**: 93.44 seconds (17.52 million rows)
- **Hierarchical Deduplication**: Isolated 139,963 Level-1 summary rows where `Layer 2` is blank.
- **Generated Parquet Tables** (Snappy-compressed in `/ewaste/processed/parquet/`):
  1. `yearly_ewaste`
  2. `location_ewaste`
  3. `category_ewaste`
  4. `scenario_ewaste`
  5. `flow_ewaste`

---

## 7. Spark SQL Analytical Queries
- **Script**: `spark/sql_queries.py` (Runtime: 22.31 seconds)
- **Query A (Yearly Generation)**:
  - 2010 OBS: 12,805,616.57 Mg
  - 2015 OBS: 14,824,630.81 Mg
  - 2020 OBS: 15,242,909.96 Mg
  - 2022 OBS: 15,758,030.91 Mg
  - 2025 BAU: 16,875,074.67 Mg
- **Query C (EU WEEE Category Distribution)**:
  - `WEEE_Cat4a` (Large Equipment): 686.22M Mg (38.53%)
  - `WEEE_Cat5` (Small Equipment): 491.11M Mg (27.58%)
  - `WEEE_Cat1` (Temperature Exchange): 285.63M Mg (16.04%)
  - `WEEE_Cat6` (Small IT & Telecom): 115.87M Mg (6.51%)
  - `WEEE_Cat4b` (Photovoltaic Panels): 97.67M Mg (5.48%)
  - `WEEE_Cat2` (Screens & Monitors): 90.29M Mg (5.07%)
  - `WEEE_Cat3` (Lamps): 14.16M Mg (0.80%)
- **Query D (Scenario Policy Comparison, 2023–2050)**:
  - `BAU` (Business As Usual): 616,985,636.53 Mg
  - `REC` (Recycling Target): 518,636,572.09 Mg (-98.3M Mg vs BAU)
  - `CIR` (Circular Economy): 455,520,922.84 Mg (-161.46M Mg vs BAU)

---

## 8. Spark MLlib Baseline Forecasting
- **Algorithm**: Distributed Linear Regression (`featuresCol="Year"`, `labelCol="Total_WEEE_Mg"`)
- **Training Period**: Observed Historical (2010 – 2022)
- **Formula**: $\text{Total\_WEEE\_Mg} = 192,770.41 \times \text{Year} - 374,025,097.97$
- **Evaluation Metrics**:
  - $R^2$ Score: **0.8410**
  - RMSE: **313,674.50 Mg**
- **Projections**:
  - 2025: 16,334,976.83 Mg
  - 2030: 17,298,828.86 Mg
  - 2035: 18,262,680.90 Mg

---

## 9. MongoDB Recycling Infrastructure
- **Database**: `ewaste_db` | **Collection**: `facilities`
- **Seeded Records**: 18 large-scale European recycling, smelting, and depollution plants
- **Geospatial Coordinates**: Verified lat/lon across Germany, Belgium, France, UK, Italy, Spain, Netherlands, Poland, Sweden, Austria, Switzerland.
- **Total Registered Capacity**: 1,605,000 Mg/year

---

## 10. Interactive Streamlit Dashboard
- **URL**: `http://localhost:8501`
- **Pages**:
  1. Overview & KPIs (17.52M records, 31 countries, dynamic Plotly charts)
  2. Geographic & Location Analysis (European intensity map and rankings)
  3. Category Distribution (EU 6/7 product category shares)
  4. Scenario Comparison (BAU vs REC vs CIR savings curves)
  5. Stock/Flow Mass Balance (Taxonomy definitions and allocations)
  6. MLlib Baseline Forecast (Trendline and evaluation table)
  7. Recycling Infrastructure (Interactive treatment plant map from MongoDB)
  8. Data & Pipeline Architecture (Live health cards, lineage diagram, log viewers)

---

## 11. Limitations
1. Single-node pseudo-distributed cluster simulates distributed mechanics locally on NVMe SSD.
2. FutuRaM dataset location codes are national ISO codes; sub-regional coordinates for recycling infrastructure were modeled as reference facilities.

---

## 12. Future Work
1. Integrate Apache Kafka for real-time sensor streams from IoT-enabled smart e-waste collection bins.
2. Deploy multi-node Kubernetes / YARN cluster across distributed nodes for multi-terabyte material flow datasets.
