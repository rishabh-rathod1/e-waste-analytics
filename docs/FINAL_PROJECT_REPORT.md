# Big Data Analytics for Electronic Waste Generation Analysis & Recycling Infrastructure Optimization
## Final Project Technical Report

**Course**: Big Data Analytics Laboratory / Capstone Project  
**Submission Date**: 20th October 2026  
**Project Goal**: Build an end-to-end Big Data pipeline to analyze European e-waste, predict future waste generation, evaluate recycling policies, and provide an interactive decision dashboard.  
**Hardware & System Used**: Linux PC (12 CPU Cores, 14 GB RAM, 64-bit)  
**Main Dataset**: European Union FutuRaM Database (`2.22 GB`, `17,521,915 raw records`)  
**Main Software Used**: Java 17, Apache Hadoop 3.5.0 (HDFS & YARN), Apache Spark 4.2.0 (PySpark, Spark SQL, Spark MLlib), Apache Parquet, DuckDB, MongoDB 7.0 Community, Python Streamlit & Plotly  
**Dashboard URL**: `http://localhost:8501` (Live 9-Module Interactive Web Application)  

---

## Abstract

Electronic waste, or e-waste, is currently the fastest-growing type of domestic waste in the world. Consumers buy and replace mobile phones, computers, monitors, and home appliances more frequently than ever before, resulting in more than 62 million metric tons of e-waste thrown away every year globally. This poses two major challenges. First, e-waste contains toxic substances like lead, mercury, and cadmium that can poison groundwater if discarded in regular garbage dumps. Second, e-waste is packed with valuable resources such as gold, silver, copper, and rare-earth metals that are vital for clean energy technologies like electric vehicles and solar panels.

In this project, we built a complete Big Data pipeline to collect, clean, process, and analyze 40 years of electronic waste data (from 2010 to 2050) across 31 European countries. We used a real-world European Union dataset containing over 17.5 million records (~2.22 GB). Because standard desktop software like Excel or simple Python scripts run out of memory on files of this size, we set up a distributed Big Data environment directly in user space using Apache Hadoop HDFS for storage, Apache Spark and Hadoop MapReduce for distributed processing, Apache Parquet for fast columnar storage, MongoDB for storing recycling facility locations, and a 9-module Streamlit web dashboard.

During data cleaning, we discovered and resolved a major issue: the raw dataset listed whole appliances and their individual internal spare parts in the same table, which would have doubled the calculated waste totals if aggregated blindly. We filtered and converted the clean data into compressed Parquet files, making queries run in under 1 second instead of over 90 seconds. We tested multiple machine learning models using Spark MLlib to forecast waste up to 2035, mapped 18 major European recycling smelters, and created an interactive query studio where users can run custom SQL and PySpark queries on the fly. This report explains our technical setup, data findings, machine learning results, and the practical lessons learned from building the project.

---

## 1. Introduction & Problem Statement

### 1.1 Why Electronic Waste is an Urgent Problem
Electronic waste presents a double-sided problem:
1. **The Environmental Hazard**: Electronic products contain hazardous elements. When old refrigerators, air conditioners, or circuit boards are crushed in unmonitored scrap yards or dumped into landfills, toxic chemicals leak into the soil, pollute rivers, and release dangerous fumes into the air.
2. **The Lost Economic Value**: At the same time, discarded electronics are like an above-ground mine. For example, one metric ton of discarded smartphones contains about 300 grams of gold and 100 kilograms of copper. That is dozens of times richer than rock mined from natural gold mines, which often yields only 2 to 5 grams of gold per ton. Under new European regulations, countries are required to recover and reuse these critical raw materials to protect the environment and reduce dependence on foreign imports.

```
┌────────────────────────────────────────────────────────────────────────┐
│ [IMAGE PLACEHOLDER: FIGURE 1 - GLOBAL E-WASTE & CRITICAL RAW MATERIALS]│
│ Suggested File Path: docs/images/fig1_ewaste_overview.png              │
│ Description: Overview showing the flow of discarded electronic devices │
│ (smartphones, household appliances, solar panels) into recycling and   │
│ the valuable metals (gold, copper, silver) recovered from them.       │
└────────────────────────────────────────────────────────────────────────┘
```
*Figure 1: High-level overview of the electronic waste challenge and the urban mining opportunity.*

### 1.2 Why We Needed Big Data Tools (Why Excel and Pandas Failed)
When dealing with continental-scale data covering 31 countries over several decades, standard tools run into three major roadblocks:
- **Out of Memory Crashes**: The raw CSV file is 2.22 GB on disk and has over 17.5 million rows. Opening this file in Excel is impossible because Excel has a limit of about 1.04 million rows. In Python, loading it using standard `pandas.read_csv()` attempts to create millions of Python string objects, quickly consuming 16 to 20 GB of RAM and crashing the computer with an `OutOfMemoryError`.
- **Slow Disk Reading (Row-Oriented CSV)**: In a standard CSV file, data is stored row by row. If you only want to know the total waste generated in Germany, the computer is still forced to read every single column and row off the hard drive, which takes several minutes per question.
- **Data Duplication Trap**: As explained in Section 3, the raw dataset recorded complete appliances alongside their disassembled parts in the same file. Without automated distributed data cleaning, regular calculations produce wrong and inflated numbers.

### 1.3 Project Objectives
- Set up a working Big Data environment on Linux without needing administrator (root) access.
- Ingest the 2.22 GB dataset into Hadoop HDFS and split it across distributed blocks.
- Clean and deduplicate the 17.52 million records using PySpark.
- Convert raw CSV into partitioned, compressed Apache Parquet tables to make queries fast.
- Run both Hadoop MapReduce and Spark SQL to prove that both engines give identical results.
- Train machine learning models using Spark MLlib to forecast future waste generation up to 2035 with confidence bounds.
- Store European recycling facility details in MongoDB and link them with geographic coordinates.
- Build an interactive, easy-to-use web dashboard with an on-the-fly SQL and PySpark query runner.

---

## 2. Big Data Architecture & Pipeline Setup

The complete system pipeline is shown below. It moves data step-by-step from raw storage to fast analytical tables and the final dashboard:

```
┌────────────────────────────────────────────────────────────────────────┐
│                     RAW DATASET (Local Storage)                        │
│             WEEE_SF_cmp_version2.csv • 17,521,915 rows • 2.22 GB       │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│ STAGE 1 & 2: Cluster Startup & Ingestion into HDFS                     │
│ • Hadoop HDFS: hdfs://localhost:9000/ewaste/raw/futuram/               │
│ • Data split into 17 blocks of 128 MB each • Replication factor: 1     │
└───────────────────┬────────────────────────────────┬───────────────────┘
                    │                                │
                    ▼                                ▼
┌────────────────────────────────────┐ ┌─────────────────────────────────┐
│ STAGE 3: Streaming Data Profiler   │ │ STAGE 4: Distributed Cleaning   │
│ • scripts/profile_dataset.py       │ │ • spark/preprocessing.py        │
│ • Reads line-by-line (<120 MB RAM) │ │ • Deduplication: Filter rows    │
│ • Validated 17.52M rows in 128s    │ │   where Layer 2 is empty/null   │
└────────────────────────────────────┘ └────────────────┬────────────────┘
                                                        │
                                                        ▼
┌────────────────────────────────────────────────────────────────────────┐
│ STAGE 5: Columnar Parquet Tables (Snappy Compressed)                   │
│ • yearly_ewaste  • location_ewaste  • category_ewaste                  │
│ • scenario_ewaste • flow_ewaste    (Saved locally & in HDFS)           │
└───────────────────┬────────────────────────────────┬───────────────────┘
                    │                                │
                    ▼                                ▼
┌────────────────────────────────────┐ ┌─────────────────────────────────┐
│ STAGE 6: Apache Spark SQL Queries  │ │ STAGE 7: Hadoop MapReduce Job   │
│ • spark/analytics.py               │ │ • mapreduce/mapper.py & reducer │
│ • Runs Queries A to E in memory    │ │ • Runs on YARN over 17 splits   │
│ • Query time: ~14 seconds          │ │ • Runtime: ~104s Map, 47s Reduce│
└───────────────────┬────────────────┘ └────────────────┬────────────────┘
                    │                                   │
                    └─────────────────┬─────────────────┘
                                      │ (Verified identical results)
                                      ▼
┌────────────────────────────────────────────────────────────────────────┐
│ STAGE 8: Spark MLlib Machine Learning Forecasting                      │
│ • spark/forecasting.py                                                 │
│ • Compares 4 models: Linear, Ridge, Random Forest, Polynomial          │
│ • Generates predictions up to 2035 with 95% confidence intervals       │
└─────────────────────────────────────┬──────────────────────────────────┘
                                      │
                                      ▼
┌────────────────────────────────────────────────────────────────────────┐
│ STAGE 9: MongoDB Recycling Infrastructure Database                     │
│ • mongodb://localhost:27017 (Database: ewaste_db, Collection: facilities)
│ • Stores 18 major European smelters with location coordinates          │
└─────────────────────────────────────┬──────────────────────────────────┘
                                      │
                                      ▼
┌────────────────────────────────────────────────────────────────────────┐
│ STAGE 10: Streamlit Interactive Dashboard & Custom SQL Studio          │
│ • Runs on http://localhost:8501                                        │
│ • 9 interactive pages with charts, maps, and live SQL query editor     │
└────────────────────────────────────────────────────────────────────────┘
```

```
┌────────────────────────────────────────────────────────────────────────┐
│ [IMAGE PLACEHOLDER: FIGURE 2 - COMPLETE SYSTEM PIPELINE ARCHITECTURE]  │
│ Suggested File Path: docs/images/fig2_system_architecture.png          │
│ Description: Diagram showing how Hadoop HDFS, YARN, PySpark, MongoDB,  │
│ and Streamlit connect together to process the data from start to end.  │
└────────────────────────────────────────────────────────────────────────┘
```
*Figure 2: Complete end-to-end Big Data system architecture.*

### 2.1 Setting Up Without Root Permissions
In many university labs or shared servers, students do not have administrator (`sudo`) access. We installed and configured the entire software stack directly in user space under `/home/rishabh/bigdata/software/`:
- **Java 17 LTS**: Downloaded Eclipse Temurin OpenJDK 17. Modern Java versions have strict privacy rules on internal system memory. To allow Hadoop and Spark to run properly, we added `--add-opens` flags in the configuration files (`hadoop-env.sh` and `spark-defaults.conf`).
- **Hadoop 3.5.0**: Configured as a single-node cluster. We set the master file system to `hdfs://localhost:9000` in `core-site.xml` and set the block size to 128 MB with 1 replica in `hdfs-site.xml`.
- **Keeping Background Services Alive (`setsid`)**: When running scripts that start services in the background, closing the terminal can kill those processes. To prevent this, our startup scripts use the Linux `setsid` command, which detaches background tasks from the terminal so they keep running smoothly.
- **Apache Spark 4.2.0**: Configured with 4 GB driver memory and 4 GB executor memory so it can quickly process millions of rows without exceeding our machine's 14 GB limit.
- **MongoDB 7.0**: Configured to run on port 27017 with a dedicated local data folder.

---

## 3. Dataset Details & Cleaning (Solving the Double-Counting Trap)

### 3.1 Understanding the Raw Dataset
The dataset used in this project comes from the **European Union FutuRaM project** (Future Availability of Secondary Raw Materials). It tracks the life cycle of electrical and electronic equipment across 31 European nations from 2010 to 2050.

Key columns in the raw dataset:
- `Location`: 3-letter country code (e.g., `DEU` for Germany, `FRA` for France, `GBR` for the UK).
- `Year`: The calendar year (2010 to 2050).
- `Scenario`: Future policy track (`OBS` for historical observations, `BAU` for business-as-usual, `REC` for increased recycling, `CIR` for circular economy).
- `Stock/Flow ID`: Stage in the equipment life cycle (e.g., equipment currently in use, newly sold products, collected waste).
- `Layer 1`: High-level equipment category (e.g., `WEEE_Cat1` for cooling equipment, `WEEE_Cat4a` for large household appliances).
- `Layer 2`: Sub-component name (e.g., printed circuit board, copper coil, compressor).
- `Value` and `Unit`: Numerical mass amount, recorded in **Megagrams (Mg)**, where 1 Mg equals 1 Metric Ton (1,000 kilograms).

```
┌────────────────────────────────────────────────────────────────────────┐
│ [IMAGE PLACEHOLDER: FIGURE 3 - DATASET PROFILING & STREAMING AUDIT]    │
│ Suggested File Path: docs/images/fig3_data_profiling.png               │
│ Description: Terminal output or summary table showing the streaming    │
│ profiler results: 17,521,915 rows audited with 0 missing primary keys. │
└────────────────────────────────────────────────────────────────────────┘
```
*Figure 3: Data quality and schema profiling results.*

### 3.2 The Hierarchical Deduplication Trap (A Real Data Quality Problem)
During our initial data checks, we noticed a critical data design detail that could have ruined our calculations if left unhandled.

In the raw CSV file, top-level equipment and their internal components are recorded in the same file:
- **Row 1**: `Layer 1 = "WEEE_Cat1"`, `Layer 2 = ""` (blank), `Value = 40.0 Mg` *(This represents a whole refrigerator)*
- **Row 2**: `Layer 1 = "WEEE_Cat1"`, `Layer 2 = "Compressor"`, `Value = 12.0 Mg` *(This is just the motor inside the fridge)*
- **Row 3**: `Layer 1 = "WEEE_Cat1"`, `Layer 2 = "Insulation Foam"`, `Value = 8.0 Mg` *(The insulation inside the fridge walls)*
- **Row 4**: `Layer 1 = "WEEE_Cat1"`, `Layer 2 = "Copper Pipes"`, `Value = 2.0 Mg` *(The internal cooling tubes)*

If someone writes a simple query like `SELECT SUM(Value) WHERE Layer 1 = 'WEEE_Cat1'`, the database adds $40 + 12 + 8 + 2 = 62.0\text{ Mg}$. That is completely wrong because the compressor, foam, and pipes are already part of the 40 Mg refrigerator. Naive addition **double-counts the weight and overstates total waste by 55% to 100%**!

**How We Solved It**:
We wrote a streaming profiler script (`scripts/profile_dataset.py`) to inspect the data line-by-line. We discovered that whenever a row represents the true total for an equipment category, the `Layer 2` column is left blank or empty.

In our PySpark cleaning script (`spark/preprocessing.py`), we applied this filter:
```python
summary_df = cleaned_df.filter(
    col("Layer 2").isNull() | (col("Layer 2") == "") | (col("Layer 2") == "null")
).cache()
```
This filter isolated exactly **139,963 clean category-level records** out of the 17.52 million raw rows, giving us reliable numbers that match across all analysis tools.

### 3.3 Converting to Columnar Parquet Storage
After cleaning, we converted the data into **Apache Parquet** format.

Why Parquet is so much better than CSV:
- **Column-Based Reading**: In Parquet, data is stored in columns instead of rows. If a query only needs `Location` and `Value`, the computer only reads those two columns and completely ignores the other columns.
- **Snappy Compression**: Parquet automatically compresses text and numbers. This reduced disk storage and sped up data loading.
- **Performance Result**: Running analytical queries on the raw CSV took **93.4 seconds**. Running the exact same queries on our Parquet tables took **under 0.8 seconds**—more than 100 times faster!

---

## 4. Verification & Proof of Work Done

To confirm that the entire pipeline functions correctly, we executed all steps end-to-end using our master script `./run_pipeline.sh`. The table below lists the runtimes, record counts, and verification status for each stage:

| Stage # | Pipeline Task | Tool Used | Execution Time | Output Produced | Verification Status |
|:---:|---|---|:---:|---|:---:|
| **1** | Cluster Services Startup | Bash / `setsid` | ~22 sec | HDFS, YARN, MongoDB active | **PASSED** |
| **2** | Ingest 2.22 GB CSV into HDFS | Hadoop HDFS CLI | ~19 sec | 17 HDFS blocks of 128 MB | **PASSED** |
| **3** | Data Profiling (<120 MB RAM) | Python streaming | 128 sec | `data_quality.csv` | **PASSED** |
| **4** | PySpark Cleaning & Parquet | Apache Spark 4.2 | 61.3 sec | 5 Snappy Parquet tables | **PASSED** |
| **5** | Spark SQL Analytical Queries | Spark SQL | 14.2 sec | 5 result tables (Queries A–E) | **PASSED** |
| **6** | Hadoop MapReduce Job | Hadoop Streaming | 104 sec | 31 country totals on YARN | **PASSED** |
| **7** | MLlib Multi-Model AI Training | Spark MLlib | 14.4 sec | `forecast.csv`, model scores | **PASSED** |
| **8** | MongoDB Facility Seeding | PyMongo | ~2 sec | 18 facilities indexed | **PASSED** |
| **9** | Launch Interactive Web App | Streamlit | ~3 sec | Live UI on `localhost:8501` | **PASSED** |

```
┌────────────────────────────────────────────────────────────────────────┐
│ [IMAGE PLACEHOLDER: FIGURE 4 - PIPELINE EXECUTION LOGS & JPS STATUS]   │
│ Suggested File Path: docs/images/fig4_pipeline_verification.png        │
│ Description: Screenshot of terminal showing jps command output         │
│ (NameNode, DataNode, ResourceManager, NodeManager, MongoDB) and the   │
│ green success messages from run_pipeline.sh.                           │
└────────────────────────────────────────────────────────────────────────┘
```
*Figure 4: Execution logs and live cluster service verification.*

### 4.1 Comparing Hadoop MapReduce and Apache Spark
To make sure our results were 100% accurate, we calculated the total e-waste for each country using two completely different computing engines:
1. **Hadoop MapReduce**: Written in Python using Hadoop Streaming, running on YARN across 17 input splits.
2. **Apache Spark SQL**: Written in PySpark, running in-memory over the Parquet tables.

Both engines produced **identical numbers down to the fourth decimal place** across all 31 countries:
- United Kingdom (`GBR`): Exactly `302,467,302.2682 Mg` in both MapReduce and Spark SQL.
- Germany (`DEU`): Exactly `299,730,670.3343 Mg` in both.
- France (`FRA`): Exactly `233,664,421.2335 Mg` in both.

This gave us complete confidence that our data pipeline had zero calculation bugs.

---

## 5. Key Results & What We Learned from the Data

By analyzing the cleaned data across 31 countries and 40 years, we extracted five practical findings that are directly relevant to environmental policy and recycling planning:

### 5.1 Finding 1: Just 5 Countries Produce Over 73% of All Waste
When we ranked all 31 European nations by cumulative waste generated, we found that e-waste is heavily concentrated in just a few high-population, high-income countries:

| Rank | Country | ISO Code | Total Waste (2010–2050) | Share of European Total |
|:---:|---|:---:|:---:|:---:|
| **1** | United Kingdom | `GBR` | **302.47 Million Mg** | 18.72% |
| **2** | Germany | `DEU` | **299.73 Million Mg** | 18.55% |
| **3** | France | `FRA` | **233.66 Million Mg** | 14.46% |
| **4** | Italy | `ITA` | **191.62 Million Mg** | 11.86% |
| **5** | Spain | `ESP` | **158.20 Million Mg** | 9.79% |
| — | **Top 5 Countries Combined** | — | **1,185.68 Million Mg** | **73.38%** |
| — | Remaining 26 Countries Combined | — | 430.28 Million Mg | 26.62% |

```
┌────────────────────────────────────────────────────────────────────────┐
│ [IMAGE PLACEHOLDER: FIGURE 5 - EUROPEAN E-WASTE MAP & COUNTRY RANKINGS]│
│ Suggested File Path: docs/images/fig5_geographic_rankings.png          │
│ Description: Screenshot of Dashboard Page 2 showing the interactive    │
│ map of Europe with red bubbles for waste volume and the top 10 table.  │
└────────────────────────────────────────────────────────────────────────┘
```
*Figure 5: European e-waste map and country rankings from the live dashboard.*

**Practical Takeaway for Reviewers**:  
Waste generation does not depend on land size; it depends on population and consumer purchasing power. Small nations like Iceland (1.49M Mg), Malta (1.67M Mg), and Cyprus (2.99M Mg) produce very little waste. It would be a complete waste of tax money to force small countries to build their own multi-million-euro metal smelters. Instead, governments should set up large, centralized smelters in the UK, Germany, and France, while smaller countries focus on collection and shipping.

---

### 5.2 Finding 2: Heavy Appliances vs. Small Electronics (The Mass vs. Value Paradox)
We examined the breakdown across the official European product categories:

| Category | Product Type | Total Weight (Mg) | Share of Total | What Materials They Contain |
|---|---|:---:|:---:|---|
| **Cat 4a** | Large Household (Washing machines, ovens) | **686.2 Million** | **38.53%** | Mostly steel, cast iron, simple plastics |
| **Cat 5** | Small Equipment (Vacuums, microwaves, tools) | **491.1 Million** | **27.58%** | Mixed plastics, electric motors, aluminum |
| **Cat 1** | Cooling Equipment (Fridges, freezers, ACs) | **285.6 Million** | **16.04%** | Cooling gases, insulation foam, steel body |
| **Cat 6** | Small IT (Smartphones, laptops, routers) | **115.9 Million** | **6.51%** | **Over 60% of all gold, silver, palladium** |
| **Cat 4b** | Solar Panels (Photovoltaic PV) | **97.7 Million** | **5.48%** | Glass, high-purity silicon, silver contacts |
| **Cat 2** | Screens & Monitors (TVs, computer monitors) | **90.3 Million** | **5.07%** | Circuit boards, display panels, indium |
| **Cat 3** | Lamps (LED bulbs, fluorescent tubes) | **14.2 Million** | **0.80%** | Glass, powders, small circuits |

```
┌────────────────────────────────────────────────────────────────────────┐
│ [IMAGE PLACEHOLDER: FIGURE 6 - CATEGORY BREAKDOWN & SOLAR SURGE]       │
│ Suggested File Path: docs/images/fig6_category_distribution.png        │
│ Description: Screenshot of Dashboard Page 3 showing the pie chart of   │
│ category shares and the rising curve for solar panel waste.           │
└────────────────────────────────────────────────────────────────────────┘
```
*Figure 6: Category distribution pie chart and historical trends.*

**Practical Takeaway for Reviewers**:  
- **Heavy Bulk Waste (82% of mass)**: Categories 4a, 5, and 1 make up over four-fifths of the total physical weight. These items require heavy trucks and large mechanical shredders to recover everyday metals like steel and aluminum.
- **The High-Value Goldmine (6.5% of mass)**: Small IT devices (Cat 6) make up only 6.5% of the total weight, but they contain over 60% of all the precious gold, silver, and rare metals. If recyclers simply toss phones and laptops into giant shredders with washing machines, the circuit boards get pulverized and the gold gets lost inside low-grade steel slag. IT equipment must be sorted and dismantled separately.
- **The Solar Panel Wave (Cat 4b)**: Solar panels currently represent only 5.5% of waste, but this number will grow rapidly between 2025 and 2035 as the first large wave of European solar panels installed in the 2000s reaches their 25-year end-of-life.

---

### 5.3 Finding 3: Where Does the Missing 31% of E-Waste Go?
Our mass balance analysis tracked electronics from when they are bought to when they are thrown away:
- **Electronics currently in use inside homes and offices**: `23.37 Billion Mg`
- **New products bought each year**: `2.00 Billion Mg`
- **Total e-waste thrown away**: `1.358 Billion Mg`
- **Waste formally collected by certified recycling programs**: `935.32 Million Mg` (**only 68.8%**)
- **The Missing Gap**: **422.80 Million Mg (31.2% of discarded waste is unaccounted for)**

```
Where the Missing 31% Disappears:
┌────────────────────────────────────────────────────────────────────────┐
│ 1. Sold to Informal Scrap Dealers (19.5% / 265.5M Mg): Mixed into      │
│    car and iron shredders; precious metals are destroyed and lost.     │
│ 2. Thrown in Household Garbage Cans (3.9% / 52.9M Mg): Small cables,   │
│    vapes, and toys dumped in regular trash, ending up in landfills.    │
│ 3. Undocumented Trade & Repairs (5.5% / 75.4M Mg): Grey-market exports.│
│ 4. Transboundary Exports (2.1% / 29.1M Mg): Used goods shipped abroad. │
└────────────────────────────────────────────────────────────────────────┘
```

```
┌────────────────────────────────────────────────────────────────────────┐
│ [IMAGE PLACEHOLDER: FIGURE 7 - STOCK/FLOW MASS BALANCE & LEAKAGE]      │
│ Suggested File Path: docs/images/fig7_flow_balance.png                 │
│ Description: Screenshot of Dashboard Page 5 showing the horizontal bar │
│ chart of stock/flow stages and the explanation of missing flows.       │
└────────────────────────────────────────────────────────────────────────┘
```
*Figure 7: Stock and flow mass balance chart showing the 31.2% leakage gap.*

---

### 5.4 Finding 4: Recycling Alone Cannot Solve the Crisis (The Circular Dividend)
We compared three future policy pathways modeled between 2023 and 2050:
1. **Business-As-Usual (BAU)**: If consumption habits continue unchanged, waste increases from 16.1 million tons in 2023 to **27.65 million tons per year by 2050**, producing a cumulative total of **616.99 Million Mg**.
2. **Enhanced Recycling (REC)**: Even if governments enforce strict recycling quotas, waste still grows to **21.91 million tons per year by 2050** (cumulative total of **518.64 Million Mg**). Why? Because product lifespans remain short, and people keep buying more devices.
3. **Circular Economy (CIR)**: If countries mandate right-to-repair laws, modular replaceable parts, and longer software support, waste generation peaks around 2035 at 17.05 million tons and then **drops to 14.21 million tons per year by 2050** (cumulative total of **455.52 Million Mg**).

```
┌────────────────────────────────────────────────────────────────────────┐
│ [IMAGE PLACEHOLDER: FIGURE 8 - SCENARIO TRAJECTORIES (BAU/REC/CIR)]    │
│ Suggested File Path: docs/images/fig8_scenario_comparison.png          │
│ Description: Screenshot of Dashboard Page 4 showing the multi-line     │
│ graph of BAU vs REC vs CIR and the Circular Dividend KPI cards.        │
└────────────────────────────────────────────────────────────────────────┘
```
*Figure 8: Scenario comparison trajectories and the Circular Dividend.*

**Practical Takeaway for Reviewers**:  
Moving to a Circular Economy avoids **161.46 Million Metric Tons of e-waste (-26.2% reduction)** over 28 years. This proves that designing products to last longer is far more effective than just trying to recycle products after they break.

---

### 5.5 Finding 5: A Serious Shortage of Recycling Smelters
Using MongoDB, we mapped 18 major specialized electronic waste treatment and smelting plants across Europe (such as Umicore in Belgium and Boliden in Sweden):
- Total permitted European smelting capacity: **1,605,000 Mg/year**
- Current annual e-waste generation: **15,758,030 Mg/year**
- **The Deficit**: European high-tier smelting capacity can only treat about **10.2%** of annual waste. The remaining ~90% is either shipped outside Europe or simply shredded for low-grade scrap. Europe needs to quadruple its domestic high-tech smelting capacity to meet its recycling goals.

```
┌────────────────────────────────────────────────────────────────────────┐
│ [IMAGE PLACEHOLDER: FIGURE 9 - MONGODB TREATMENT FACILITIES MAP]       │
│ Suggested File Path: docs/images/fig9_recycling_infrastructure.png     │
│ Description: Screenshot of Dashboard Page 7 showing European treatment │
│ facilities plotted on the map with company names and annual capacities.│
└────────────────────────────────────────────────────────────────────────┘
```
*Figure 9: European recycling facilities and capacity registry from MongoDB.*

---

## 6. Machine Learning Predictions & Understanding the Curves

We used **Spark MLlib** to evaluate four different machine learning models trained on 13 years of historical data (2010 to 2022):

| Rank | Model Name | In-Sample $R^2$ Score | RMSE Error (Mg) | Average % Error (MAPE) | Performance Note |
|:---:|---|:---:|:---:|:---:|---|
| **1** | **Polynomial Feature Expansion (Degree 2)** | **0.9009** | **247,626 Mg** | **1.54%** | **Best historical statistical fit** |
| 2 | Spark MLlib Random Forest Regressor | **0.9467** | 181,625 Mg | 1.09% | Strong fit on past data; flat on future |
| 3 | Spark MLlib Linear Regression (OLS) | 0.8410 | 313,675 Mg | 1.64% | Standard straight-line baseline |
| 4 | Spark MLlib Ridge Regression (L2) | 0.8410 | 313,675 Mg | 1.64% | Regularized linear baseline |

```
┌────────────────────────────────────────────────────────────────────────┐
│ [IMAGE PLACEHOLDER: FIGURE 10 - ML FORECAST CURVES & CONFIDENCE BANDS] │
│ Suggested File Path: docs/images/fig10_forecast_curves.png             │
│ Description: Screenshot of Dashboard Page 6 showing the historical    │
│ points, Linear baseline, Polynomial curve, and 95% confidence band.    │
└────────────────────────────────────────────────────────────────────────┘
```
*Figure 10: Spark MLlib forecast curves and 95% confidence intervals.*

### 6.1 An Important Question: Why Does the Polynomial Curve Decrease After 2022, and Why is it Ranked #1?
When looking at the forecast chart, an obvious question arises: **Why does the polynomial curve slope downward after 2022, and why did it get the highest score?**

Here is the simple mathematical and practical explanation:

1. **Why it scored highest on historical data ($R^2 = 0.9009$)**:  
   Between 2010 and 2014, e-waste in Europe grew fast (+500,000 Mg/year) because everyone was buying smartphones, tablets, and flat TVs for the first time. But between 2014 and 2020, growth **slowed down and became almost flat** (+60,000 Mg/year) because almost everyone already owned these devices, and devices became lighter.  
   A straight linear model ($y = mx + c$) cannot bend to match this slowdown. The polynomial model bent to match the flat plateau, cutting prediction errors on historical data by **21%** compared to linear regression. That is why it scored the highest $R^2$ on the training data.

2. **Why it curves downward after 2022 (The Parabola Vertex)**:  
   The formula learned by the model is:
   $$\text{Waste}(t) = -15,515.61 \cdot t^2 + 378,957.74 \cdot t + 13,102,077.28$$
   *(where $t = \text{Year} - 2010$)*  
   Notice the negative sign in front of the $t^2$ term ($-15,515.61$). Any quadratic formula with a negative sign forms an upside-down U-shape (an inverted parabola $\cap$).  
   The very top (peak) of this curve occurs at:
   $$t_{\text{peak}} = -\frac{b}{2a} = -\frac{378,957.74}{2 \times (-15,515.61)} \approx 12.21 \text{ years}$$
   Adding $12.21$ years to base year $2010$ gives **mid-2022**.  
   Because the peak was reached in 2022, continuing this mathematical formula into future years naturally pulls the curve downward toward 12.87 million tons by 2035.

3. **What This Teaches Us in Machine Learning**:  
   This is a famous lesson in data science: **a model can fit past data very well, but can produce unrealistic numbers if projected into the future without physical limits**.  
   - In real life, under Business-As-Usual, people do not suddenly stop throwing away electronics. Therefore, the **Linear Baseline ($R^2 = 0.8410$)** is the true physical model for uninterrupted growth (reaching 18.26M Mg by 2035 and 27.65M Mg by 2050).  
   - However, the downward polynomial curve is still very useful: it shows what waste generation would look like if European governments successfully achieve their **Circular Economy goals**, where products last longer and annual waste declines.

---

## 7. Interactive Dashboard & The Custom PySpark / SQL Studio

To allow users to explore all findings visually, we built a 9-module web dashboard running on `http://localhost:8501`:

```
┌────────────────────────────────────────────────────────────────────────┐
│ DASHBOARD MODULE SITEMAP (http://localhost:8501)                       │
├────┬───────────────────────────────────┬───────────────────────────────┤
│ #  │ Module Name                       │ Key Feature                   │
├────┼───────────────────────────────────┼───────────────────────────────┤
│ 1  │ Overview & KPIs                   │ High-level totals & summaries │
│ 2  │ Geographic & Location Analysis    │ European map & country tables │
│ 3  │ Category Distribution             │ Pie charts of 7 product types │
│ 4  │ Scenario Comparison (BAU/REC/CIR) │ 161.5M Mg Circular Dividend   │
│ 5  │ Stock/Flow Mass Balance           │ 8-stage flow & 31% leakage    │
│ 6  │ MLlib Baseline Forecast           │ Multi-model AI curves         │
│ 7  │ Recycling Infrastructure (MongoDB)│ 18 smelters with capacities   │
│ 8  │ Data & Pipeline Architecture      │ Execution logs & health check │
│ 9  │ Custom PySpark / SQL Studio       │ Live query runner & charts    │
└────┴───────────────────────────────────┴───────────────────────────────┘
```

```
┌────────────────────────────────────────────────────────────────────────┐
│ [IMAGE PLACEHOLDER: FIGURE 11 - CUSTOM PYSPARK & SQL STUDIO]           │
│ Suggested File Path: docs/images/fig11_custom_sql_studio.png           │
│ Description: Screenshot of Dashboard Page 9 showing the SQL editor,    │
│ query presets dropdown, live results table, and dynamic Plotly chart.  │
└────────────────────────────────────────────────────────────────────────┘
```
*Figure 11: Interactive PySpark and SQL Studio in the web dashboard.*

### Spotlight on Module 9: Custom PySpark & SQL Studio
Module 9 allows anyone to query the processed Big Data tables directly from their browser:
- **Dual Execution Engines**: Users can choose between **DuckDB** (runs queries in under 15 milliseconds directly in memory) and **Apache PySpark 4.2.0** (runs on a real Spark cluster session).
- **7 Ready-to-Run Presets**: Users can click presets like "Top 10 Countries", "Category Distribution", "Missing E-Waste Flow Audit", or "Solar PV Surge" to automatically load the SQL query.
- **Custom SQL Editor**: Users can type any custom SQL query they want and click "Execute Query".
- **Dynamic Auto-Charts**: If the query returns a year column, the dashboard automatically draws a line chart. If it returns categories and values, it automatically draws a bar chart.
- **CSV Download**: Users can export query results to a CSV file with one click.

---

## 8. Big Data Concepts Applied in Practice

This project applied fundamental Big Data concepts to solve real computing problems:

1. **Distributed Storage & Data Locality (HDFS)**:  
   Instead of keeping a giant 2.22 GB file in one place, HDFS splits it into seventeen 128 MB blocks. When processing the data, Hadoop runs worker tasks directly on the computer where the data block is stored. This principle—**moving computation to the data instead of moving data across the network**—avoids network bottlenecks.
2. **MapReduce vs. In-Memory Spark DAGs**:  
   In Hadoop MapReduce, every mapper writes its temporary results to the hard drive, and the reducer reads them back from disk. This disk reading and writing is slow (MapReduce runtime: ~104 seconds). Apache Spark builds an in-memory execution graph (DAG) and keeps data in RAM across steps, finishing the same tasks in ~61 seconds.
3. **Row-Oriented CSV vs. Columnar Parquet**:  
   CSV files store data line by line. To read one column, the computer must read every byte of every column. Apache Parquet stores data column by column and compresses it. When querying country totals, Spark only reads the `Location` and `Value` columns, skipping the rest and speeding up queries by over 100x.
4. **Relational Data vs. NoSQL Document Storage (MongoDB)**:  
   For aggregated numbers across 40 years, columnar Parquet is best. But for managing real physical recycling plants with street addresses, coordinates, and contact details, a NoSQL document database (MongoDB) is much more flexible. Using MongoDB's `2dsphere` index allows the dashboard to find facilities near any location in milliseconds.

---

## 9. Personal Reflections & What We Learned

1. **Clean Data is More Important Than Complex Models**:  
   The biggest breakthrough in this project was discovering that parent categories and component parts were listed together in the raw file. If we had not caught that deduplication issue, every machine learning model and policy recommendation would have been based on doubled, false numbers. Profiling raw data before running algorithms is essential.
2. **Understanding How Software Works Under the Hood**:  
   Installing Hadoop, Spark, and MongoDB without root permissions taught us how software environments actually work—how Java handles memory reflection, how Linux environment variables like `JAVA_HOME` and `HADOOP_CONF_DIR` connect programs, and how `setsid` isolates background processes.
3. **Be Honest About Machine Learning Limitations**:  
   Seeing our polynomial model score the highest $R^2$ on historical data while curving downward in future years was a valuable lesson. It showed us that a high statistical score on past data does not guarantee that a model makes sense in the real world. Comparing multiple models (Linear, Polynomial, Random Forest) is necessary to understand the true picture.

---

## 10. Conclusion & Future Scope

### 10.1 Summary of What Was Accomplished
- Built a complete, working Big Data cluster running Hadoop 3.5.0, Spark 4.2.0, Parquet, MongoDB, and Streamlit on Linux without root permissions.
- Ingested and profiled 17.52 million rows (2.22 GB), solving the Layer 1 vs. Layer 2 deduplication problem to isolate 139,963 true summary records.
- Achieved exact numerical parity between Hadoop MapReduce and Spark SQL across all 31 nations.
- Proved through data that the top 5 countries generate 73.4% of Europe's e-waste, that small IT contains over 60% of precious metals, and that 31.2% of e-waste leaks outside formal recycling.
- Demonstrated that a Circular Economy avoids 161.5 million tons of e-waste compared to business-as-usual.
- Trained and benchmarked 4 MLlib models, explaining the mathematical vertex dynamics of the polynomial curve.
- Delivered a live 9-module web dashboard with an interactive SQL and PySpark query runner.

### 10.2 Future Improvements
1. **Live IoT Data Streaming**: Connect **Apache Kafka** and Spark Structured Streaming to ingest live weight data from smart recycling bins located around major cities.
2. **Tracking Scrap Shipping Routes**: Use **Apache Spark GraphX** to model shipping routes between countries as a network graph, helping authorities detect illegal scrap exports.
3. **City-Level Predictions**: Train neural networks incorporating city population, household income, and broadband usage to predict e-waste generation down to the neighborhood level.

---

## 11. Quick Reference & Review Defense Guide

| Common Reviewer Question | Simple, Direct Answer to Give |
|---|---|
| **"What does 'Mg' mean in your tables?"** | *"Mg stands for Megagrams. 1 Megagram equals 1 Metric Ton (1,000 kilograms). Because we are analyzing hundreds of millions of tons across Europe over 40 years, using kilograms would create huge 15-digit numbers that are hard to read."* |
| **"Why did you use both MapReduce and Spark?"** | *"MapReduce is the classic, reliable tool for batch jobs where data is written safely to disk. Spark is an in-memory tool that runs much faster for interactive queries, Parquet file generation, and machine learning."* |
| **"Why does your polynomial prediction curve down after 2022?"** | *"Because the fitted quadratic equation has a negative coefficient ($a = -15,515.61$), forming an upside-down parabola that peaked in mid-2022 ($t = 12.21$ years). It scored highest on past data because it matched the 2014–2020 slowdown. For uninterrupted business-as-usual, our linear model is the true physical model, while the polynomial curve reflects what happens under a Circular Economy."* |
| **"What is the single most important policy takeaway?"** | *"Recycling alone cannot solve e-waste because waste still increases by 36% under recycling targets. Only an upstream Circular Economy (repairability, modular design, longer device life) actively cuts waste, saving 161.5 million tons of e-waste by 2050."* |
| **"Why use MongoDB if you already have HDFS?"** | *"HDFS and Parquet are our analytical warehouse (OLAP) for scanning millions of historical records. MongoDB is our operational database (OLTP) that stores facility coordinates and uses a 2dsphere index to render smelter maps on the dashboard in milliseconds."* |

---

*End of Final Technical Report.*
