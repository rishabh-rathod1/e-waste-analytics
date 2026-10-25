# Big Data Analytics for Electronic Waste Management — Presentation Deck

**Project Title**: Big Data Analytics for Electronic Waste Generation Analysis and Recycling Infrastructure Optimization  
**Format**: 10 Modern Technical Presentation Slides (1 Title, 8 Content, 1 Thank You)  
**Interactive Slide Deck**: [presentation/index.html](file:///home/rishabh/ewaste-bigdata/presentation/index.html)  

---

## Slide 1: Title Slide (Cover)

### **Big Data Analytics for Electronic Waste Generation Analysis & Recycling Infrastructure Optimization**
*Continent-Scale Distributed Ingestion, Deduplication, Multi-Engine Analytics, and Geospatial Optimization over 17.52 Million European Material Flow Records*

- **Domain**: Distributed Systems, Big Data Engineering, Environmental Informatics & Circular Economy
- **Target OS**: Omarchy Linux (Kernel 7.2.3, 12 Cores, 14 GB RAM, x86_64)
- **Primary Dataset**: EU Horizon Europe FutuRaM WEEE Database (`2.22 GB`, `17,521,915 records`)
- **Key Technologies**: Java 17 LTS, Apache Hadoop 3.5.0 (HDFS/YARN), Apache Spark 4.2.0 (SQL/MLlib), Snappy Parquet, MongoDB 7.0, Streamlit & Plotly
- **Live System URL**: `http://localhost:8501`

> 🗣️ **Speaker Notes (Slide 1)**:  
> *"Good morning respected evaluators and committee members. Today I present our Big Data Analytics project on Electronic Waste Generation Analysis and Recycling Infrastructure Optimization. Managing discarded electronics across 31 European nations over a 40-year horizon involves analyzing tens of millions of records. We have engineered an enterprise distributed big data platform entirely in user space, resolving critical domain double-counting challenges and delivering sub-second interactive analytics through modern distributed engines."*

---

## Slide 2: Problem Statement & The FutuRaM Challenge

### **The Global E-Waste Crisis & The Horizon Europe FutuRaM Dataset**

#### 1. The Dual Environmental & Strategic Challenge
- **Global Generation**: Exceeds **62 million metric tons** annually, expanding at 3–5% p.a.
- **Toxicity**: Contains bioaccumulative heavy metals (lead, mercury, cadmium) and halogenated flame retardants.
- **Critical Raw Materials (CRMs)**: Discarded electronics hold high-grade concentrations of Gold, Silver, Copper, and Palladium essential for the clean energy transition.

#### 2. Dataset Scale & Structural Complexity
- **Source**: Horizon Europe FutuRaM (Future Raw Materials) Consortium.
- **Volume**: **17,521,915 rows**, **2.22 GB raw uncompressed CSV**, 12 columns.
- **Geographic & Temporal Span**: 31 European Nations (EU-27 + UK, CHE, NOR, ISL) from **2010 to 2050**.
- **The Big Data Bottleneck**: Single-node tools (Pandas, Excel) crash with `OutOfMemoryError` (>16 GB RAM required for dataframe expansion). Uncompressed text incurs heavy disk I/O penalties.

> 🗣️ **Speaker Notes (Slide 2)**:  
> *"To understand the magnitude of the problem: electronic waste is the world's fastest-growing municipal solid waste stream. However, it is also an urban goldmine containing critical raw materials. The EU FutuRaM dataset provides the world's most comprehensive mapping of stocks and flows, but at 17.5 million records, traditional data science tools fail to scale. This necessitated an enterprise distributed architecture."*

---

## Slide 3: End-to-End System Architecture & Data Lineage

### **Complete Lakehouse Architecture & Lineage Flowchart**

```
[ Raw CSV: 17.52M Rows (2.22 GB) ] ──> [ HDFS Ingestion (128MB Blocks) ]
                                             │
                       ┌─────────────────────┴─────────────────────┐
                       ▼                                           ▼
          [ Streaming Profiler ]                        [ PySpark Preprocessing ]
          (csv.reader, <120MB RAM)                      (Layer 2 Null Filter: 139K Rows)
                       │                                           │
                       ▼                                           ▼
          [ data_quality.csv ]                         [ Snappy Columnar Parquet ]
                                                                   │
                               ┌───────────────────────────────────┼───────────────────────────────────┐
                               ▼                                   ▼                                   ▼
                   [ Hadoop MapReduce (YARN) ]           [ Spark SQL Views (A-E) ]           [ Spark MLlib Regression ]
                   (mapper.py + reducer.py)              (Catalyst Optimizer)                (Fit on OBS -> 2035 Projection)
                               │                                   │                                   │
                               └─────────────────┬─────────────────┴───────────────────────────────────┘
                                                 ▼
                               [ MongoDB 2dsphere Geospatial Indexing ]
                               (18 Facilities, 1.6M Mg Capacity)
                                                 │
                                                 ▼
                               [ Interactive Streamlit Dashboard (Port 8501) ]
                               (8 Glassmorphism Pages, Plotly European Choropleths)
```

#### Key Architecture Principles:
1. **User-Space Deployment**: Full Hadoop, Spark, and MongoDB setup without root or `sudo` privileges.
2. **Java 17 Compatibility**: Solved modular encapsulation via explicit `--add-opens` JVM reflection flags.
3. **Session Detachment (`setsid`)**: Prevented automated terminal subshells from killing background daemons.

> 🗣️ **Speaker Notes (Slide 3)**:  
> *"Here is our end-to-end architecture. Data flows from local disk into HDFS 128 MB blocks. A streaming profiler audits data quality in constant memory. PySpark performs ETL and deduplication, saving Snappy-compressed Parquet tables. We deploy dual compute engines: Hadoop MapReduce on YARN and Spark SQL, coupled with MLlib forecasting and a MongoDB geospatial layer, all surfaced via an interactive Streamlit dashboard."*

---

## Slide 4: Data Profiling & The Hierarchical Deduplication Trap

### **Resolving Multi-Million-Ton False Double-Counting**

#### 1. The Core Domain Trap
In raw material flow tables, parent summary categories (Layer 1) and component decompositions (Layer 2) coexist:
- `Layer 1 = "WEEE_Cat1"`, `Layer 2 = ""` -> `Value = 40.0 Mg` *(Whole Appliance)*
- `Layer 1 = "WEEE_Cat1"`, `Layer 2 = "Compressor"` -> `Value = 12.0 Mg` *(Part)*
- `Layer 1 = "WEEE_Cat1"`, `Layer 2 = "Foam"` -> `Value = 8.0 Mg` *(Part)*
- `Layer 1 = "WEEE_Cat1"`, `Layer 2 = "Copper Coil"` -> `Value = 2.0 Mg` *(Part)*
- **Naive Sum**: $40 + 12 + 8 + 2 = 62.0\text{ Mg}$ (**55% to 100% False Inflation!**)

#### 2. The Algorithmic Fix in PySpark
```python
summary_df = cleaned_df.filter(
    (col("Layer2").isNull()) | (col("Layer2") == "") | (col("Layer2") == "null")
).cache()
```
- Isolates **139,963 true Level-1 summary rows** out of 17.52M rows.
- Streaming Profiler verified: **17,521,915 rows audited in 128s with zero numeric errors using <120 MB RAM**.

#### 3. Storage Optimization: Columnar Snappy Parquet
- **Column Pruning**: Queries read only requested byte streams from disk.
- **Predicate Pushdown**: Skips row groups using block min/max statistics.
- **Performance**: Downstream analytical queries dropped from **90+ seconds to <1 second**.

> 🗣️ **Speaker Notes (Slide 4)**:  
> *"The most critical engineering achievement of our project was identifying and solving the hierarchical deduplication trap. Blindly calculating totals on this dataset inflates real waste by nearly double because sub-assemblies are co-listed. By filtering for blank Layer 2 records, we isolated 139,963 true summary records, ensuring absolute mathematical integrity across all downstream analytics."*

---

## Slide 5: Dual Distributed Compute Engines (MapReduce vs Spark)

### **Low-Level Batch MapReduce on YARN vs In-Memory Spark 4.2.0**

| Architectural Dimension | Apache Hadoop MapReduce on YARN | Apache Spark 4.2.0 (Core & SQL) |
|---|---|---|
| **Computation Paradigm** | Disk-bound batch processing (Map -> Shuffle -> Reduce). | Memory-centric Directed Acyclic Graph (DAG) execution. |
| **Intermediate Storage** | Spills intermediate map outputs to local disk before shuffle. | Keeps intermediate data in executor memory (RAM) across stages. |
| **Python Integration** | Standard UNIX pipes via Hadoop Streaming (`stdin` / `stdout`). | Py4J gateway communicating with native JVM execution. |
| **Optimization Engine** | Static runtime container allocations without dynamic query rewrites. | Catalyst Optimizer (predicate pushdown, constant folding) + Tungsten. |
| **Execution Time** | **140 seconds** (`map 100% reduce 100%`). | **93 seconds** (Complete 17.5M row multi-table ETL). |

#### Verified Output Consistency:
Both engines executed independent country aggregations over 17.52 million rows and produced **identical results down to the decimal** (e.g. United Kingdom = `302,467,302.27 Mg`).

> 🗣️ **Speaker Notes (Slide 5)**:  
> *"To rigorously evaluate distributed computing paradigms, we implemented both Hadoop MapReduce and Apache Spark. Our MapReduce streaming job ran over 17 input splits on YARN, while Spark processed the pipeline using its in-memory DAG and Catalyst optimizer. Both produced identical country totals down to the decimal, proving mathematical consistency while demonstrating Spark's superior speed."*

---

## Slide 6: Spark SQL Analytical Engine & Multi-Dimensional Insights

### **Key Empirical Findings over 17.52 Million Records**

#### 1. Top E-Waste Generating Hotspots (Cumulative 2010–2050)
1. **United Kingdom (`GBR`)**: **302.47 Million Mg**
2. **Germany (`DEU`)**: **299.73 Million Mg**
3. **France (`FRA`)**: **233.66 Million Mg**
4. **Italy (`ITA`)**: **191.62 Million Mg**
5. **Spain (`ESP`)**: **158.20 Million Mg**
6. **Poland (`POL`)**: **81.57 Million Mg**

#### 2. EU Product Category Distribution (Query C)
- **Large Household Appliances (`WEEE_Cat4a`)**: **38.53%** (686.22M Mg) — Washing machines, dishwashers, ovens.
- **Small Equipment (`WEEE_Cat5`)**: **27.58%** (491.11M Mg) — Microwaves, vacuum cleaners, power tools.
- **Temperature Exchange Equipment (`WEEE_Cat1`)**: **16.04%** (285.63M Mg) — Refrigerators, AC units, heat pumps.
- **Small IT & Telecommunications (`WEEE_Cat6`)**: **6.51%** (115.87M Mg) — Laptops, smartphones, routers.
- **Photovoltaic Panels (`WEEE_Cat4b`)**: **5.48%** (97.67M Mg) — Fastest expanding future stream.

#### 3. Mass Balance Dynamics (Query E)
- Active in-use appliance stock: **23.37 Billion Mg**.
- Put On Market (POM) sales: **2.00 Billion Mg**.
- Formally collected e-waste: **935.32 Million Mg**.

> 🗣️ **Speaker Notes (Slide 6)**:  
> *"Our Spark SQL analytics revealed striking geographical and category concentrations. The top 5 economies—UK, Germany, France, Italy, and Spain—account for over 70% of Western European electronic waste. Furthermore, large household appliances and small consumer equipment dominate the waste stream, representing two-thirds of total mass."*

---

## Slide 7: Policy Scenario Simulation & Spark MLlib Forecasting

### **The Circular Economy Dividend & Distributed Machine Learning**

#### 1. Policy Scenario Projections (2023–2050)
- **Business-As-Usual (BAU)**: **616.99 Million Mg**
- **Recycling Targets (REC)**: **518.64 Million Mg** (reduces waste by 98.35M Mg)
- **Circular Economy (CIR)**: **455.52 Million Mg**
- **Strategic Impact**: Aggressive circular policies (ecodesign, modular repair, product life extension) eliminate **161.46 Million Metric Tons** of electronic waste compared to BAU.

#### 2. Enhanced Multi-Model AI Predictive Engine
- **Model Benchmark**: Evaluated 4 distinct machine learning models on OBS historical actuals (2010–2022):
  1. **Champion Model**: Polynomial Feature Expansion ($R^2 = 0.9009$, $\text{RMSE} = 247,626\text{ Mg}$, $\text{MAPE} = 1.54\%$)
  2. **Ensemble Learning**: Spark MLlib Random Forest Regressor ($R^2 = 0.9467$ in-sample, $\text{RMSE} = 181,625\text{ Mg}$)
  3. **Linear Baseline**: Spark MLlib Linear Regression OLS ($R^2 = 0.8410$, $\text{RMSE} = 313,674\text{ Mg}$)
  4. **Regularized L2**: Spark MLlib Ridge Regression ($R^2 = 0.8410$, $\text{RMSE} = 313,674\text{ Mg}$)
- **Algorithmic Accuracy Gain**: The Champion Polynomial model reduced RMSE by **21.06%** over standard linear regression by modeling non-linear accelerated consumer electronics adoption cycles.
- **Projections with 95% Confidence Bounds**:
  - 2025: **15.30M Mg** [95% CI: 14.81M – 15.78M Mg]
  - 2030: **14.47M Mg** [95% CI: 13.99M – 14.96M Mg]
  - 2035: **12.88M Mg** [95% CI: 12.39M – 13.36M Mg]

> 🗣️ **Speaker Notes (Slide 7)**:  
> *"When evaluating future trajectories, our policy simulations show that adopting a full Circular Economy framework avoids 161.5 million metric tons of e-waste by 2050 compared to Business-As-Usual. Furthermore, we refined our AI predictive modeling by benchmarking 4 distinct distributed machine learning algorithms in PySpark. Our Champion model, Polynomial Feature Expansion, achieved an R-squared of 0.9009, reducing RMSE by 21% over baseline linear regression. It captures non-linear device replacement cycles with 95% confidence bounds."*

---

## Slide 8: MongoDB Recycling Infrastructure & Geospatial Modeling

### **NoSQL Semi-Structured Modeling & 2dsphere Geospatial Indexing**

#### 1. Why NoSQL for E-Waste Infrastructure?
- **Semi-Structured Metallurgy**: Smelters, mechanical shredders, depollution stations, and hydrometallurgical refining plants have heterogeneous, non-uniform attributes.
- **Nested Collections**: Dynamic arrays of accepted WEEE categories (`["WEEE_Cat1", "WEEE_Cat4a", "WEEE_Cat5"]`).
- **Spatial Geometry**: Storing exact coordinates for spatial radius and nearest-facility queries.

#### 2. European Treatment Network
- **Repository**: Database `ewaste_db`, Collection `facilities`.
- **Modeled Capacity**: 18 major European facilities across 11 nations representing **1,605,000 Metric Tons/year** capacity.
- **Sample Facilities**:
  - *Aurubis Smelter* (Hamburg, DEU): 280,000 Mg/year (Specialized Smelting)
  - *Umicore Hoboken* (Antwerp, BEL): 150,000 Mg/year (Precious Metals Recovery)
  - *Boliden Rönnskär* (Skellefteå, SWE): 120,000 Mg/year (Base/Precious Metallurgy)
  - *Remondis Lünen* (Lünen, DEU): 95,000 Mg/year (Integrated Dismantling)
- **Geospatial Compound Index**: `(latitude, longitude)` index enabling sub-second spatial querying and map pin rendering.

> 🗣️ **Speaker Notes (Slide 8)**:  
> *"To optimize recycling logistics, we integrated MongoDB to model European treatment infrastructure. Treatment facilities feature highly heterogeneous processes—from pyrometallurgy to hydrometallurgy. Using compound 2dsphere geospatial indexing, our system calculates spatial proximity between waste generation hotspots and authorized smelters in real time."*

---

## Slide 9: Interactive Streamlit Analytics Dashboard

### **Production Serving Layer & Decision-Support Portal**

#### 1. Serving Layer Design Philosophy
- **Zero Raw File Scans**: The dashboard **never scans the 2.22 GB CSV on page load**; it queries pre-aggregated Snappy Parquet tables and compact CSVs.
- **In-Memory Caching**: Implements Streamlit's `@st.cache_data` for instant rendering and sub-second filtering.
- **Modern Glassmorphism UI**: Dark slate theme (`#0f172a`), responsive cards, Plotly charts.

#### 2. Eight Interactive Modules:
1. **Executive Overview**: KPI scorecards, historical trends, category donut.
2. **Geographic Analysis**: Plotly European choropleth with bubble overlays.
3. **Category Distribution**: EU Directive Cat 1–6 rankings & stacked area charts.
4. **Scenario Comparison**: BAU vs REC vs CIR policy trajectory simulation.
5. **Stock & Flow Balance**: Mass conservation across stocks, sales, and informal scrap.
6. **MLlib Forecast**: Actuals vs fitted regression line through 2035.
7. **Infrastructure Map**: Interactive facility map loaded from MongoDB.
8. **Cluster Architecture**: Live daemon badges & embedded pipeline logs.
- **Access URL**: `http://localhost:8501`

> 🗣️ **Speaker Notes (Slide 9)**:  
> *"Our user interface is an 8-page interactive Streamlit dashboard. By following a serving layer design pattern where the UI queries pre-aggregated Parquet tables and cached CSVs, the dashboard achieves sub-second response times despite the 17.5-million-row underlying scale. It gives environmental policy makers instant access to maps, scenario comparisons, and infrastructure gaps."*

---

## Slide 10: Conclusion, Technical Contributions & Thank You

### **Summary of Key Technical Contributions & Defense Wrap-Up**

```
┌────────────────────────────────────────────────────────────────────────┐
│                      KEY TECHNICAL ACHIEVEMENTS                        │
├────────────────────────────────────────────────────────────────────────┤
│ 1. 100% User-Space Cluster    │ Zero sudo/root deployment of Hadoop,   │
│                               │ Spark, YARN, and MongoDB on Omarchy.   │
│ 2. Deduplication Solution     │ Solved double-counting, isolating      │
│                               │ 139,963 clean Level-1 records.         │
│ 3. Dual Compute Verification  │ Proved MapReduce & Spark parity down   │
│                               │ to the exact decimal (GBR = 302.47M).  │
│ 4. Parquet Acceleration       │ Reduced analytical query latency from  │
│                               │ 90+ seconds to <1 second.              │
│ 5. MLlib Forecasting          │ Fitted linear regression on actuals    │
│                               │ (R² = 0.8410, RMSE = 313,674 Mg).     │
│ 6. Geospatial Optimization    │ Compound 2dsphere indexing across 18   │
│                               │ European recycling & smelting plants.  │
│ 7. Decision-Support UI        │ 8-page glassmorphism Streamlit portal. │
└────────────────────────────────────────────────────────────────────────┘
```

- **Live Dashboard**: `http://localhost:8501`
- **Master Engineering Handbook**: [PROJECT_EXPLANATION_AND_CONCEPTS.md](file:///home/rishabh/ewaste-bigdata/PROJECT_EXPLANATION_AND_CONCEPTS.md)
- **Interactive Slides**: [presentation/index.html](file:///home/rishabh/ewaste-bigdata/presentation/index.html)

### **Thank You! Questions & Discussion**

> 🗣️ **Speaker Notes (Slide 10)**:  
> *"To conclude, we have successfully designed, built, and validated an end-to-end Big Data engineering platform for continental electronic waste analytics. We solved core domain data traps, verified dual distributed compute engines, and delivered a production-ready analytical dashboard. Thank you for your time and attention. I am now pleased to take your questions."*

---
