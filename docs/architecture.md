# System Architecture Documentation

**Project Title**: Big Data Analytics for Electronic Waste Generation Analysis and Recycling Infrastructure Optimization

---

## 1. Executive Summary & Design Principles

The Electronic Waste (WEEE) Big Data Analytics platform processes large-scale, hierarchical European material flow datasets (FutuRaM project, ~2.22 GB, ~17.52 million rows) to extract actionable intelligence on national waste generation, category distributions, future trajectories across policy scenarios (BAU, REC, CIR), and recycling facility infrastructure.

### Architectural Core Tenets:
1. **Zero-Root Portability**: Built entirely in user-space on Omarchy Linux without requiring system administrative root permissions.
2. **Scan-Once Pipeline Principle**: Raw 2.22 GB CSV is ingested once into HDFS, cleansed, deduplicated across the hierarchical Layer 1-4 structure, and converted into columnar Snappy-compressed Parquet. All analytical workloads, SQL queries, and dashboard sessions query Parquet/aggregates.
3. **Dual Processing Engines**: Utilizes both classic **Hadoop MapReduce (Hadoop Streaming on YARN)** for distributed batch key-value aggregation and **Apache Spark 4.2.0** for high-throughput DataFrame transformations and Spark SQL.
4. **Hybrid Storage**: Distributed raw/processed storage in **HDFS**, relational analytical views in **Spark SQL**, and semi-structured document storage with geospatial indexing in **MongoDB 7.0**.

---

## 2. Complete End-to-End System Architecture

```
                                 ┌──────────────────────────────────┐
                                 │     Local Raw WEEE Dataset       │
                                 │  ~/Downloads/DATASET/WEEE_SF.csv │
                                 │      2.12 GB, 17.52M Rows        │
                                 └─────────────────┬────────────────┘
                                                   │
                                                   │ HDFS Ingestion (hdfs dfs -put)
                                                   ▼
                                 ┌──────────────────────────────────┐
                                 │        Hadoop HDFS 3.5.0         │
                                 │  /ewaste/raw/futuram/WEEE_SF.csv │
                                 └─────────┬──────────────┬─────────┘
                                           │              │
                   ┌───────────────────────┘              └────────────────────────┐
                   ▼                                                               ▼
    ┌─────────────────────────────┐                                ┌─────────────────────────────┐
    │   Hadoop MapReduce (YARN)   │                                │   PySpark Preprocessing     │
    │   Hadoop Streaming          │                                │   Explicit Schema           │
    │   mapper.py / reducer.py    │                                │   Hierarchical Dedup        │
    │   Location → Total WEEE     │                                │   Snappy Compression        │
    └──────────────┬──────────────┘                                └──────────────┬──────────────┘
                   │                                                              │
                   ▼                                                              ▼
    ┌─────────────────────────────┐                                ┌─────────────────────────────┐
    │   MapReduce Output Tables   │                                │   Snappy Parquet Datasets   │
    │   /ewaste/output/mapreduce/ │                                │   /ewaste/processed/parquet/│
    └─────────────────────────────┘                                └──────────────┬──────────────┘
                                                                                  │
                                  ┌───────────────────────────────────────────────┼──────────────────────────────┐
                                  ▼                                               ▼                              ▼
                   ┌─────────────────────────────┐                 ┌─────────────────────────────┐┌─────────────────────────────┐
                   │     Spark SQL Engine        │                 │    Spark MLlib Engine       ││      MongoDB 7.0 DB         │
                   │   Queries A, B, C, D, E     │                 │    Linear Regression Model  ││  Recycling Facilities       │
                   │   Regional & Category Views │                 │    Forecast to 2035         ││  Geospatial 2dsphere Index  │
                   └──────────────┬──────────────┘                 └──────────────┬──────────────┘└──────────────┬──────────────┘
                                  │                                               │                              │
                                  └───────────────────────┬───────────────────────┘                              │
                                                          │                                                      │
                                                          ▼                                                      ▼
                                           ┌────────────────────────────────────────────────────────────────────────────┐
                                           │                   Interactive Streamlit Web Dashboard                      │
                                           │                         http://localhost:8501                              │
                                           │  8 Pages: KPIs, Geospatial Maps, Categories, Scenarios, Forecast, Pipeline  │
                                           └────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Technology Stack Breakdown & Rationale

| Layer | Technology | Version | Purpose & Rationale |
|---|---|---|---|
| **Distributed Storage** | Apache Hadoop HDFS | 3.5.0 | High-fault-tolerant, scalable block storage for massive multi-gigabyte raw inputs and analytical Parquet outputs. |
| **Cluster Manager** | Apache Hadoop YARN | 3.5.0 | Resource negotiation and container scheduling for MapReduce jobs. |
| **Batch Processing** | Hadoop Streaming MapReduce | 3.5.0 | Low-level distributed key-value mapping and reduction demonstration. |
| **In-Memory Compute** | Apache Spark / PySpark | 4.2.0 | High-speed distributed DataFrame transformations, caching, schema enforcement, and column pruning. |
| **Structured Querying** | Spark SQL | 4.2.0 | ANSI SQL execution over Parquet temp views with Catalyst query optimizer and Adaptive Query Execution (AQE). |
| **Machine Learning** | Spark MLlib | 4.2.0 | Distributed Linear Regression with VectorAssembler for historical trend fitting and forward forecasting. |
| **NoSQL / Metadata** | MongoDB Community Server | 7.0.14 | JSON document repository with 2dsphere geospatial indexing for facility capacity, coordinates, and treatment metadata. |
| **Visualization UI** | Streamlit + Plotly | 1.64.0 / 7.1.0 | Modern, responsive dashboard interface with Plotly choropleth maps, area plots, donut charts, and live pipeline monitor. |
| **Runtime & Language** | Java OpenJDK LTS / Python | 17.0.12 / 3.14.7 | Robust execution environment with Java 17 compatibility flags (`--add-opens`). |

---

## 4. Storage Architecture

### HDFS Directory Hierarchy
- `/ewaste/raw/futuram/`: Immutable raw WEEE CSV files.
- `/ewaste/processed/parquet/`:
  - `yearly_ewaste`: Aggregated yearly flows by scenario.
  - `location_ewaste`: Country-level generation by year and scenario.
  - `category_ewaste`: 7 EU WEEE Layer 1 category aggregates.
  - `scenario_ewaste`: Comparative trajectories across BAU, REC, and CIR.
  - `flow_ewaste`: Stock/Flow mass balance aggregates.
- `/ewaste/output/mapreduce/`: Distributed MapReduce output (`part-00000`).
- `/ewaste/warehouse/`: Spark SQL analytical tables and external tables.

---

## 5. Hierarchical Data Deduplication Strategy
In the FutuRaM dataset, each equipment category coexists with sub-components (e.g. PCB, metals, plastics) across Layers 1 to 4. Blind summation causes massive double counting. The preprocessing layer strictly isolates summary records where `Layer 2` is blank/null (`139,963` rows out of `17,521,915`), guaranteeing mathematical accuracy across all downstream metrics.
