#!/usr/bin/env python3
"""
Enterprise PowerPoint (PPTX) Generator
Big Data Analytics for Electronic Waste Management
Creates an ultra-modern 16:9 widescreen presentation with dark theme,
metrics cards, tables, flowcharts, speaker notes, and embedded 3D assets.
"""

import os
from pptx import Presentation
from pptx.util import Inches, Pt
from pptx.enum.text import PP_ALIGN
from pptx.dml.color import RGBColor
from pptx.enum.shapes import MSO_SHAPE

def build_presentation():
    prs = Presentation()
    # Set 16:9 widescreen dimensions (13.333 x 7.5 inches)
    prs.slide_width = Inches(13.333)
    prs.slide_height = Inches(7.5)
    blank_layout = prs.slide_layouts[6]

    # Color Palette Definitions
    COLOR_BG = RGBColor(11, 17, 30)         # #0B111E Dark Navy Slate
    COLOR_CARD = RGBColor(19, 29, 49)       # #131D31 Card Fill
    COLOR_CARD_BORDER = RGBColor(36, 56, 89)# #243859 Subtle Border
    COLOR_TEXT_WHITE = RGBColor(248, 250, 252) # #F8FAFC Primary Text
    COLOR_TEXT_MUTED = RGBColor(148, 163, 184) # #94A3B8 Secondary Text
    COLOR_CYAN = RGBColor(56, 189, 248)     # #38BDF8 Accent Cyan
    COLOR_EMERALD = RGBColor(16, 185, 129)  # #10B981 Accent Green
    COLOR_AMBER = RGBColor(245, 158, 11)    # #F59E0B Accent Amber
    COLOR_ROSE = RGBColor(244, 63, 94)      # #F43F5E Accent Rose
    COLOR_TABLE_HEADER = RGBColor(30, 41, 59) # #1E293B

    # Helper: Set Slide Background
    def set_slide_background(slide):
        background = slide.background
        fill = background.fill
        fill.solid()
        fill.fore_color.rgb = COLOR_BG

    # Helper: Add Slide Header
    def add_header(slide, badge_text, title_text, subtitle_text, counter_text):
        # Badge
        badge_box = slide.shapes.add_textbox(Inches(0.8), Inches(0.45), Inches(8.0), Inches(0.35))
        tf_b = badge_box.text_frame
        tf_b.word_wrap = True
        p_b = tf_b.paragraphs[0]
        p_b.text = badge_text.upper()
        p_b.font.size = Pt(10)
        p_b.font.bold = True
        p_b.font.color.rgb = COLOR_CYAN
        p_b.font.name = 'Arial'

        # Title
        title_box = slide.shapes.add_textbox(Inches(0.8), Inches(0.8), Inches(10.5), Inches(0.6))
        tf_t = title_box.text_frame
        tf_t.word_wrap = True
        p_t = tf_t.paragraphs[0]
        p_t.text = title_text
        p_t.font.size = Pt(22)
        p_t.font.bold = True
        p_t.font.color.rgb = COLOR_TEXT_WHITE
        p_t.font.name = 'Arial'

        # Subtitle
        sub_box = slide.shapes.add_textbox(Inches(0.8), Inches(1.4), Inches(10.5), Inches(0.35))
        tf_s = sub_box.text_frame
        tf_s.word_wrap = True
        p_s = tf_s.paragraphs[0]
        p_s.text = subtitle_text
        p_s.font.size = Pt(11)
        p_s.font.color.rgb = COLOR_TEXT_MUTED
        p_s.font.name = 'Arial'

        # Counter Box
        counter_box = slide.shapes.add_textbox(Inches(11.5), Inches(0.55), Inches(1.1), Inches(0.4))
        tf_c = counter_box.text_frame
        p_c = tf_c.paragraphs[0]
        p_c.alignment = PP_ALIGN.RIGHT
        p_c.text = counter_text
        p_c.font.size = Pt(12)
        p_c.font.bold = True
        p_c.font.color.rgb = COLOR_CYAN
        p_c.font.name = 'Arial'

    # Helper: Add Card Rectangle
    def add_card(slide, left, top, width, height, border_color=COLOR_CARD_BORDER):
        shape = slide.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, left, top, width, height)
        shape.fill.solid()
        shape.fill.fore_color.rgb = COLOR_CARD
        shape.line.color.rgb = border_color
        shape.line.width = Pt(1.2)
        return shape

    # Helper: Add Speaker Notes
    def add_speaker_notes(slide, notes_text):
        notes_slide = slide.notes_slide
        tf = notes_slide.notes_text_frame
        tf.text = notes_text

    # ==========================================
    # SLIDE 1: TITLE SLIDE
    # ==========================================
    slide1 = prs.slides.add_slide(blank_layout)
    set_slide_background(slide1)

    # Title Badge
    s1_badge = slide1.shapes.add_textbox(Inches(1.0), Inches(1.2), Inches(11.333), Inches(0.4))
    tf1_b = s1_badge.text_frame
    p1_b = tf1_b.paragraphs[0]
    p1_b.alignment = PP_ALIGN.CENTER
    p1_b.text = "UNIVERSITY BIG DATA ANALYTICS — FINAL ENGINEERING DEFENSE"
    p1_b.font.size = Pt(11)
    p1_b.font.bold = True
    p1_b.font.color.rgb = COLOR_CYAN

    # Main Title
    s1_title = slide1.shapes.add_textbox(Inches(1.0), Inches(1.7), Inches(11.333), Inches(1.6))
    tf1_t = s1_title.text_frame
    tf1_t.word_wrap = True
    p1_t = tf1_t.paragraphs[0]
    p1_t.alignment = PP_ALIGN.CENTER
    p1_t.text = "Big Data Analytics for Electronic Waste Generation\n& Recycling Infrastructure Optimization"
    p1_t.font.size = Pt(30)
    p1_t.font.bold = True
    p1_t.font.color.rgb = COLOR_TEXT_WHITE

    # Subtitle
    s1_sub = slide1.shapes.add_textbox(Inches(1.5), Inches(3.5), Inches(10.333), Inches(0.8))
    tf1_s = s1_sub.text_frame
    tf1_s.word_wrap = True
    p1_s = tf1_s.paragraphs[0]
    p1_s.alignment = PP_ALIGN.CENTER
    p1_s.text = "Continent-Scale Distributed Ingestion, Deduplication, Multi-Engine Analytics, and Geospatial Optimization over 17.52 Million European Material Flow Records"
    p1_s.font.size = Pt(13)
    p1_s.font.color.rgb = COLOR_TEXT_MUTED

    # Tech Pills Card
    pill_card = add_card(slide1, Inches(1.5), Inches(4.5), Inches(10.333), Inches(1.1))
    s1_pills = slide1.shapes.add_textbox(Inches(1.6), Inches(4.6), Inches(10.133), Inches(0.9))
    tf1_p = s1_pills.text_frame
    tf1_p.word_wrap = True
    p1_p = tf1_p.paragraphs[0]
    p1_p.alignment = PP_ALIGN.CENTER
    p1_p.text = "• Apache Hadoop 3.5.0 (HDFS/YARN)   • Apache Spark 4.2.0 (SQL/MLlib)   • Snappy Parquet\n• MongoDB 7.0 (2dsphere Geospatial)   • Streamlit & Plotly Decision Support Portal"
    p1_p.font.size = Pt(12)
    p1_p.font.bold = True
    p1_p.font.color.rgb = COLOR_CYAN

    # Metadata Footer
    s1_meta = slide1.shapes.add_textbox(Inches(1.5), Inches(5.9), Inches(10.333), Inches(0.8))
    tf1_m = s1_meta.text_frame
    p1_m = tf1_m.paragraphs[0]
    p1_m.alignment = PP_ALIGN.CENTER
    p1_m.text = "Operating Environment: Omarchy Linux (12 CPU Cores, 14 GB RAM) | Dataset: EU FutuRaM (2.22 GB, 17.52M Rows)\nLive Web Dashboard: http://localhost:8501"
    p1_m.font.size = Pt(11)
    p1_m.font.color.rgb = COLOR_TEXT_MUTED

    add_speaker_notes(slide1,
        "Good morning respected evaluators and committee members. Today I present our Big Data Analytics project "
        "on Electronic Waste Generation Analysis and Recycling Infrastructure Optimization. Managing discarded electronics "
        "across 31 European nations over a 40-year horizon involves analyzing tens of millions of records. "
        "We have engineered an enterprise distributed big data platform entirely in user space, resolving critical domain "
        "double-counting challenges and delivering sub-second interactive analytics through modern distributed engines.")

    # ==========================================
    # SLIDE 2: PROBLEM STATEMENT & DOMAIN CONTEXT
    # ==========================================
    slide2 = prs.slides.add_slide(blank_layout)
    set_slide_background(slide2)
    add_header(slide2, "01. Motivation & Domain Context", "The Global E-Waste Challenge & FutuRaM",
               "Why continent-scale material flows demand enterprise distributed computing architectures", "02 / 10")

    # Left Column: Card 1 (Dual Crisis)
    add_card(slide2, Inches(0.8), Inches(1.9), Inches(5.6), Inches(2.5))
    tb2_1 = slide2.shapes.add_textbox(Inches(0.95), Inches(2.0), Inches(5.3), Inches(2.3))
    tf2_1 = tb2_1.text_frame
    tf2_1.word_wrap = True
    p = tf2_1.paragraphs[0]
    p.text = "The Dual Crisis of Electronic Waste"
    p.font.size = Pt(13)
    p.font.bold = True
    p.font.color.rgb = COLOR_TEXT_WHITE

    bullets2_1 = [
        "• Fastest Growing Solid Waste Stream: 62+ million metric tons generated annually worldwide (+3% to 5% p.a.).",
        "• High Environmental Toxicity: Heavy metals (lead, mercury, cadmium) and flame retardants degrade soil and water.",
        "• Strategic Urban Goldmine: Discarded electronics contain high concentrations of Gold, Silver, Copper, and Palladium."
    ]
    for b in bullets2_1:
        p = tf2_1.add_paragraph()
        p.text = b
        p.font.size = Pt(10)
        p.font.color.rgb = COLOR_TEXT_MUTED

    # Left Column: Card 2 (Dataset Metrics)
    add_card(slide2, Inches(0.8), Inches(4.55), Inches(5.6), Inches(2.3))
    tb2_2 = slide2.shapes.add_textbox(Inches(0.95), Inches(4.65), Inches(5.3), Inches(2.1))
    tf2_2 = tb2_2.text_frame
    tf2_2.word_wrap = True
    p = tf2_2.paragraphs[0]
    p.text = "Horizon Europe FutuRaM Consortium Dataset"
    p.font.size = Pt(13)
    p.font.bold = True
    p.font.color.rgb = COLOR_TEXT_WHITE

    bullets2_2 = [
        "• Total Records: 17,521,915 material flow rows (~2.22 GB raw CSV).",
        "• Geographic Scope: 31 European Nations (EU-27 + UK, CHE, NOR, ISL).",
        "• Temporal Horizon: 2010 to 2050 (Historical OBS + BAU, REC, CIR).",
        "• The Big Data Bottleneck: Single-node Pandas/Excel crashes with OutOfMemoryError (>16 GB RAM required for row conversion)."
    ]
    for b in bullets2_2:
        p = tf2_2.add_paragraph()
        p.text = b
        p.font.size = Pt(10)
        p.font.color.rgb = COLOR_TEXT_MUTED

    # Right Column: Hero Image
    hero_img_path = "/home/rishabh/ewaste-bigdata/presentation/assets/hero.jpg"
    if os.path.exists(hero_img_path):
        slide2.shapes.add_picture(hero_img_path, Inches(6.6), Inches(1.9), Inches(5.9), Inches(4.95))

    add_speaker_notes(slide2,
        "To understand the magnitude of the problem: electronic waste is the world's fastest-growing municipal solid waste stream. "
        "However, it is also an urban goldmine containing critical raw materials. The EU FutuRaM dataset provides the world's most "
        "comprehensive mapping of stocks and flows, but at 17.5 million records, traditional data science tools fail to scale. "
        "This necessitated an enterprise distributed architecture.")

    # ==========================================
    # SLIDE 3: SYSTEM ARCHITECTURE & FLOWCHART
    # ==========================================
    slide3 = prs.slides.add_slide(blank_layout)
    set_slide_background(slide3)
    add_header(slide3, "02. Distributed System Engineering", "End-to-End Lakehouse Architecture & Lineage",
               "Zero-sudo user-space cluster orchestrating HDFS, Spark, YARN, and MongoDB", "03 / 10")

    # Left Column: Flowchart Card
    add_card(slide3, Inches(0.8), Inches(1.9), Inches(6.8), Inches(4.95))
    tb3_1 = slide3.shapes.add_textbox(Inches(0.95), Inches(2.0), Inches(6.5), Inches(4.7))
    tf3_1 = tb3_1.text_frame
    tf3_1.word_wrap = True
    p = tf3_1.paragraphs[0]
    p.text = "Complete Data Pipeline Execution Flow"
    p.font.size = Pt(13)
    p.font.bold = True
    p.font.color.rgb = COLOR_TEXT_WHITE

    flow_steps = [
        "1. Raw HDFS Ingestion: Discovers local CSV (2.22 GB) and ingests into HDFS across 128MB blocks.",
        "2. Streaming Data Profiler: Audits 17.52M rows in fixed memory (<120MB RAM) in 128s. Zero errors.",
        "3. PySpark ETL & Deduplication: Filters Layer 2 == '', isolating 139,963 Level-1 summary rows.",
        "4. Snappy Parquet Generation: Creates partitioned columnar tables (yearly, location, category, scenario, flow).",
        "5. Dual Compute Processing: Hadoop Streaming on YARN (MapReduce) + Spark SQL analytical queries.",
        "6. MLlib Linear Regression: Fits temporal model on OBS actuals (2010–2022) predicting waste to 2035 (R² = 0.841).",
        "7. MongoDB Geospatial Layer: Seeds 18 European recycling facilities with compound 2dsphere index.",
        "8. Interactive Serving: 8-Page Streamlit dashboard (port 8501) with sub-second response times."
    ]
    for step in flow_steps:
        p = tf3_1.add_paragraph()
        p.text = step
        p.font.size = Pt(9.5)
        p.font.color.rgb = COLOR_TEXT_MUTED

    # Right Column: Architecture Image
    arch_img_path = "/home/rishabh/ewaste-bigdata/presentation/assets/architecture.jpg"
    if os.path.exists(arch_img_path):
        slide3.shapes.add_picture(arch_img_path, Inches(7.8), Inches(1.9), Inches(4.7), Inches(4.95))

    add_speaker_notes(slide3,
        "Here is our end-to-end architecture. Data flows from local disk into HDFS 128 MB blocks. A streaming profiler audits data "
        "quality in constant memory. PySpark performs ETL and deduplication, saving Snappy-compressed Parquet tables. We deploy dual "
        "compute engines: Hadoop MapReduce on YARN and Spark SQL, coupled with MLlib forecasting and a MongoDB geospatial layer, "
        "all surfaced via an interactive Streamlit dashboard.")

    # ==========================================
    # SLIDE 4: HIERARCHICAL DEDUPLICATION TRAP
    # ==========================================
    slide4 = prs.slides.add_slide(blank_layout)
    set_slide_background(slide4)
    add_header(slide4, "03. Big Data Engineering Challenge", "The Hierarchical Deduplication Trap",
               "Solving multi-million-ton double counting in secondary material flow databases", "04 / 10")

    # Left Column: Problem & Code Fix
    add_card(slide4, Inches(0.8), Inches(1.9), Inches(5.6), Inches(2.6), border_color=COLOR_ROSE)
    tb4_1 = slide4.shapes.add_textbox(Inches(0.95), Inches(2.0), Inches(5.3), Inches(2.4))
    tf4_1 = tb4_1.text_frame
    tf4_1.word_wrap = True
    p = tf4_1.paragraphs[0]
    p.text = "The Double-Counting Trap in Raw Material Flows"
    p.font.size = Pt(13)
    p.font.bold = True
    p.font.color.rgb = COLOR_ROSE

    p = tf4_1.add_paragraph()
    p.text = "In raw FutuRaM tables, parent categories and sub-assemblies coexist:\n" \
             "• Row 1: Cat1 (Refrigerator) | Layer 2 = '' | Value = 40.0 Mg [WHOLE]\n" \
             "• Row 2: Cat1 (Refrigerator) | Layer 2 = 'Compressor' | Value = 12.0 Mg\n" \
             "• Row 3: Cat1 (Refrigerator) | Layer 2 = 'Foam' | Value = 8.0 Mg\n" \
             "• Row 4: Cat1 (Refrigerator) | Layer 2 = 'Copper' | Value = 2.0 Mg\n" \
             "Naive SUM = 40 + 12 + 8 + 2 = 62 Mg (55% TO 100% FALSE INFLATION!)"
    p.font.size = Pt(9.5)
    p.font.color.rgb = COLOR_TEXT_MUTED

    # Left Column: PySpark Solution Box
    add_card(slide4, Inches(0.8), Inches(4.65), Inches(5.6), Inches(2.2), border_color=COLOR_EMERALD)
    tb4_2 = slide4.shapes.add_textbox(Inches(0.95), Inches(4.75), Inches(5.3), Inches(2.0))
    tf4_2 = tb4_2.text_frame
    tf4_2.word_wrap = True
    p = tf4_2.paragraphs[0]
    p.text = "The PySpark Deduplication Solution"
    p.font.size = Pt(13)
    p.font.bold = True
    p.font.color.rgb = COLOR_EMERALD

    p = tf4_2.add_paragraph()
    p.text = "# Strictly filter for Level-1 summary rows where Layer 2 is blank\n" \
             "summary_df = cleaned_df.filter(\n" \
             "    col('Layer2').isNull() | (col('Layer2') == '') | (col('Layer2') == 'null')\n" \
             ").cache()\n" \
             "# Isolates exactly 139,963 clean records from 17.52 million rows."
    p.font.size = Pt(9.5)
    p.font.color.rgb = COLOR_CYAN

    # Right Column: Data Profiling Audit & Parquet Optimization
    add_card(slide4, Inches(6.6), Inches(1.9), Inches(5.9), Inches(4.95))
    tb4_3 = slide4.shapes.add_textbox(Inches(6.8), Inches(2.0), Inches(5.5), Inches(4.7))
    tf4_3 = tb4_3.text_frame
    tf4_3.word_wrap = True
    p = tf4_3.paragraphs[0]
    p.text = "Streaming Data Quality Profiler & Parquet Benefits"
    p.font.size = Pt(13)
    p.font.bold = True
    p.font.color.rgb = COLOR_TEXT_WHITE

    p = tf4_3.add_paragraph()
    p.text = "Streaming Quality Audit Results (scripts/profile_dataset.py):\n" \
             "• Audited Records: 17,521,915 rows in 128.12s\n" \
             "• Invalid Numeric Records: 0 (100% Data Integrity)\n" \
             "• True Category Summaries: 139,963 records\n" \
             "• Memory Consumption: <120 MB RAM (streaming generator)\n\n" \
             "Why Snappy Columnar Parquet?\n" \
             "• Column Pruning: Reads only requested columns from disk.\n" \
             "• Predicate Pushdown: Skips row groups based on min/max stats.\n" \
             "• Analytical Speedup: Query latency dropped from 90+s to <1s.\n" \
             "• Storage Compression: Substantial disk footprint reduction."
    p.font.size = Pt(10)
    p.font.color.rgb = COLOR_TEXT_MUTED

    add_speaker_notes(slide4,
        "The most critical engineering achievement of our project was identifying and solving the hierarchical deduplication trap. "
        "Blindly calculating totals on this dataset inflates real waste by nearly double because sub-assemblies are co-listed. "
        "By filtering for blank Layer 2 records, we isolated 139,963 true summary records, ensuring absolute mathematical integrity "
        "across all downstream analytics.")

    # ==========================================
    # SLIDE 5: DUAL DISTRIBUTED COMPUTE ENGINES
    # ==========================================
    slide5 = prs.slides.add_slide(blank_layout)
    set_slide_background(slide5)
    add_header(slide5, "04. Distributed Processing", "Dual Compute: Hadoop MapReduce vs Spark",
               "Benchmarking low-level batch MapReduce on YARN against in-memory Spark SQL", "05 / 10")

    # Left Column: Hadoop MapReduce
    add_card(slide5, Inches(0.8), Inches(1.9), Inches(5.6), Inches(3.6))
    tb5_1 = slide5.shapes.add_textbox(Inches(0.95), Inches(2.0), Inches(5.3), Inches(3.4))
    tf5_1 = tb5_1.text_frame
    tf5_1.word_wrap = True
    p = tf5_1.paragraphs[0]
    p.text = "Hadoop MapReduce Streaming on YARN"
    p.font.size = Pt(13)
    p.font.bold = True
    p.font.color.rgb = COLOR_TEXT_WHITE

    bullets5_1 = [
        "• Native Hadoop Streaming: Uses standard UNIX STDIN/STDOUT via hadoop-streaming-3.5.0.jar.",
        "• Input Splitting: YARN splits 2.22 GB input into 17 blocks (~128MB each) executed in parallel.",
        "• Stateful Reducer: Accumulates running sum per nation key during network shuffle & sort.",
        "• Benchmark Runtime: 140 seconds (map 100% reduce 100%)."
    ]
    for b in bullets5_1:
        p = tf5_1.add_paragraph()
        p.text = b
        p.font.size = Pt(10)
        p.font.color.rgb = COLOR_TEXT_MUTED

    # Right Column: Apache Spark In-Memory Engine
    add_card(slide5, Inches(6.6), Inches(1.9), Inches(5.9), Inches(3.6))
    tb5_2 = slide5.shapes.add_textbox(Inches(6.8), Inches(2.0), Inches(5.5), Inches(3.4))
    tf5_2 = tb5_2.text_frame
    tf5_2.word_wrap = True
    p = tf5_2.paragraphs[0]
    p.text = "Apache Spark 4.2.0 In-Memory Engine"
    p.font.size = Pt(13)
    p.font.bold = True
    p.font.color.rgb = COLOR_TEXT_WHITE

    bullets5_2 = [
        "• In-Memory DAG: Eliminates intermediate disk writes by caching DataFrames directly in RAM.",
        "• Catalyst Optimizer: Generates optimized physical plans, pushing filters into Parquet scan.",
        "• Tungsten Engine: Generates compact JVM bytecode for memory and CPU efficiency.",
        "• Benchmark Runtime: 93 seconds (complete multi-table ETL across 17.5M records)."
    ]
    for b in bullets5_2:
        p = tf5_2.add_paragraph()
        p.text = b
        p.font.size = Pt(10)
        p.font.color.rgb = COLOR_TEXT_MUTED

    # Bottom Full-Width Consistency Card
    add_card(slide5, Inches(0.8), Inches(5.65), Inches(11.7), Inches(1.2), border_color=COLOR_EMERALD)
    tb5_3 = slide5.shapes.add_textbox(Inches(0.95), Inches(5.75), Inches(11.4), Inches(1.0))
    tf5_3 = tb5_3.text_frame
    tf5_3.word_wrap = True
    p = tf5_3.paragraphs[0]
    p.text = "Empirical Validation & Numerical Parity Across Engines"
    p.font.size = Pt(12)
    p.font.bold = True
    p.font.color.rgb = COLOR_EMERALD

    p = tf5_3.add_paragraph()
    p.text = "Both MapReduce and Spark SQL produced identical country totals down to the exact decimal:\n" \
             "United Kingdom: 302,467,302.27 Mg | Germany: 299,730,670.33 Mg | France: 233,664,421.23 Mg"
    p.font.size = Pt(10)
    p.font.color.rgb = COLOR_TEXT_WHITE

    add_speaker_notes(slide5,
        "To rigorously evaluate distributed computing paradigms, we implemented both Hadoop MapReduce and Apache Spark. "
        "Our MapReduce streaming job ran over 17 input splits on YARN, while Spark processed the pipeline using its in-memory DAG "
        "and Catalyst optimizer. Both produced identical country totals down to the decimal, proving mathematical consistency "
        "while demonstrating Spark's superior speed.")

    # ==========================================
    # SLIDE 6: SPARK SQL ANALYTICAL INSIGHTS
    # ==========================================
    slide6 = prs.slides.add_slide(blank_layout)
    set_slide_background(slide6)
    add_header(slide6, "05. Analytical Results & Insights", "Spark SQL Multi-Dimensional Analytics",
               "Empirical findings across 31 countries, 7 product categories, and 8 flow taxonomies", "06 / 10")

    # Left Column: Top 6 Hotspots Table
    add_card(slide6, Inches(0.8), Inches(1.9), Inches(5.6), Inches(4.95))
    tb6_1 = slide6.shapes.add_textbox(Inches(0.95), Inches(2.0), Inches(5.3), Inches(0.4))
    tf6_1 = tb6_1.text_frame
    p = tf6_1.paragraphs[0]
    p.text = "Top E-Waste Hotspots (Cumulative 2010–2050)"
    p.font.size = Pt(12)
    p.font.bold = True
    p.font.color.rgb = COLOR_TEXT_WHITE

    # Add Table for Hotspots
    table_shape = slide6.shapes.add_table(7, 4, Inches(0.95), Inches(2.5), Inches(5.3), Inches(2.8))
    table = table_shape.table
    table.columns[0].width = Inches(0.7)
    table.columns[1].width = Inches(2.0)
    table.columns[2].width = Inches(0.8)
    table.columns[3].width = Inches(1.8)

    headers = ["Rank", "Country", "ISO", "Generated (Mg)"]
    for i, h in enumerate(headers):
        cell = table.cell(0, i)
        cell.text = h
        cell.fill.solid()
        cell.fill.fore_color.rgb = COLOR_TABLE_HEADER
        for p in cell.text_frame.paragraphs:
            p.font.size = Pt(9)
            p.font.bold = True
            p.font.color.rgb = COLOR_CYAN

    data = [
        ("1", "United Kingdom", "GBR", "302,467,302.27"),
        ("2", "Germany", "DEU", "299,730,670.33"),
        ("3", "France", "FRA", "233,664,421.23"),
        ("4", "Italy", "ITA", "191,617,288.72"),
        ("5", "Spain", "ESP", "158,197,115.71"),
        ("6", "Poland", "POL", "81,572,628.70")
    ]
    for row_idx, row_data in enumerate(data):
        for col_idx, val in enumerate(row_data):
            cell = table.cell(row_idx + 1, col_idx)
            cell.text = val
            for p in cell.text_frame.paragraphs:
                p.font.size = Pt(8.5)
                p.font.color.rgb = COLOR_TEXT_WHITE

    tb6_sub = slide6.shapes.add_textbox(Inches(0.95), Inches(5.5), Inches(5.3), Inches(1.2))
    tf6_sub = tb6_sub.text_frame
    tf6_sub.word_wrap = True
    p = tf6_sub.paragraphs[0]
    p.text = "Historical Growth (Query A):\nAnnual e-waste grew from 12.8M Mg in 2010 to 15.7M Mg in 2022 (+22.6%), projected to reach 16.8M Mg in 2025."
    p.font.size = Pt(9.5)
    p.font.color.rgb = COLOR_TEXT_MUTED

    # Right Column: Category Distribution & Mass Balance
    add_card(slide6, Inches(6.6), Inches(1.9), Inches(5.9), Inches(4.95))
    tb6_2 = slide6.shapes.add_textbox(Inches(6.8), Inches(2.0), Inches(5.5), Inches(4.7))
    tf6_2 = tb6_2.text_frame
    tf6_2.word_wrap = True
    p = tf6_2.paragraphs[0]
    p.text = "EU Product Category Breakdown (Query C)"
    p.font.size = Pt(13)
    p.font.bold = True
    p.font.color.rgb = COLOR_TEXT_WHITE

    cats = [
        "• Large Household Appliances (Cat4a): 38.53% (686.2M Mg) — Washing machines, dishwashers, ovens.",
        "• Small Equipment (Cat5): 27.58% (491.1M Mg) — Microwaves, vacuum cleaners, power tools.",
        "• Temperature Exchange (Cat1): 16.04% (285.6M Mg) — Refrigerators, AC units, heat pumps.",
        "• Small IT & Telecoms (Cat6): 6.51% (115.9M Mg) — Laptops, smartphones, routers.",
        "• Photovoltaic Panels (Cat4b): 5.48% (97.7M Mg) — Fastest expanding future waste stream.",
        "• Screens & Monitors (Cat2): 5.07% (90.3M Mg) — Televisions, LCD screens.",
        "• Lamps & Lighting (Cat3): 0.80% (14.2M Mg) — Fluorescent tubes, LEDs."
    ]
    for c in cats:
        p = tf6_2.add_paragraph()
        p.text = c
        p.font.size = Pt(9.5)
        p.font.color.rgb = COLOR_TEXT_MUTED

    p = tf6_2.add_paragraph()
    p.text = "\nMass Balance Dynamics (Query E):\nActive in-use appliance stock = 23.37 Billion Mg | New sales = 2.00 Billion Mg | Formally collected = 935.32 Million Mg."
    p.font.size = Pt(9.5)
    p.font.color.rgb = COLOR_CYAN

    add_speaker_notes(slide6,
        "Our Spark SQL analytics revealed striking geographical and category concentrations. The top 5 economies—UK, Germany, "
        "France, Italy, and Spain—account for over 70% of Western European electronic waste. Furthermore, large household appliances "
        "and small consumer equipment dominate the waste stream, representing two-thirds of total mass.")

    # ==========================================
    # SLIDE 7: POLICY SIMULATION & ML FORECASTING
    # ==========================================
    slide7 = prs.slides.add_slide(blank_layout)
    set_slide_background(slide7)
    add_header(slide7, "06. Machine Learning & Policy Simulation", "Circular Economy Simulation & MLlib Forecasting",
               "Simulating BAU vs REC vs CIR scenarios and distributed Ordinary Least Squares regression", "07 / 10")

    # Left Column: Policy Card
    add_card(slide7, Inches(0.8), Inches(1.9), Inches(5.6), Inches(2.4))
    tb7_1 = slide7.shapes.add_textbox(Inches(0.95), Inches(2.0), Inches(5.3), Inches(2.2))
    tf7_1 = tb7_1.text_frame
    tf7_1.word_wrap = True
    p = tf7_1.paragraphs[0]
    p.text = "Policy Scenario Comparison (2023–2050)"
    p.font.size = Pt(13)
    p.font.bold = True
    p.font.color.rgb = COLOR_TEXT_WHITE

    p = tf7_1.add_paragraph()
    p.text = "• Business-As-Usual (BAU): 616.99 Million Mg\n" \
             "• Recycling Targets (REC): 518.64 Million Mg (Saves 98.35M Mg)\n" \
             "• Circular Economy (CIR): 455.52 Million Mg (Saves 161.46M Mg!)\n\n" \
             "Circular Economy Dividend: Product life extension, modular repair, and reuse eliminate 161.46 Million Metric Tons of waste."
    p.font.size = Pt(9.5)
    p.font.color.rgb = COLOR_TEXT_MUTED

    # Left Column: MLlib Multi-Model AI Engine Card
    add_card(slide7, Inches(0.8), Inches(4.45), Inches(5.6), Inches(2.4))
    tb7_2 = slide7.shapes.add_textbox(Inches(0.95), Inches(4.55), Inches(5.3), Inches(2.2))
    tf7_2 = tb7_2.text_frame
    tf7_2.word_wrap = True
    p = tf7_2.paragraphs[0]
    p.text = "Enhanced Multi-Model AI Predictive Engine"
    p.font.size = Pt(13)
    p.font.bold = True
    p.font.color.rgb = COLOR_TEXT_WHITE

    p = tf7_2.add_paragraph()
    p.text = "Benchmarked across 4 Spark MLlib models on OBS actuals:\n" \
             "• Champion Model: Polynomial Feature Expansion (R² = 0.9009, RMSE = 247,626 Mg)\n" \
             "• Ensemble Learning: Random Forest Regressor (R² = 0.9467 in-sample)\n" \
             "• Linear Baseline: OLS Linear Regression (R² = 0.8410, RMSE = 313,674 Mg)\n" \
             "• Regularized L2: Spark MLlib Ridge Regression (R² = 0.8410)\n" \
             "Error Reduction: Champion AI model reduced RMSE by 21% (MAPE = 1.54%)."
    p.font.size = Pt(9.2)
    p.font.color.rgb = COLOR_CYAN

    # Right Column: Circular Economy Image
    circular_img_path = "/home/rishabh/ewaste-bigdata/presentation/assets/circular.jpg"
    if os.path.exists(circular_img_path):
        slide7.shapes.add_picture(circular_img_path, Inches(6.6), Inches(1.9), Inches(5.9), Inches(4.95))

    add_speaker_notes(slide7,
        "When comparing policy trajectories, our simulations demonstrate that adopting a full Circular Economy framework avoids "
        "161.5 million metric tons of e-waste by 2050. Using Spark MLlib, we upgraded our predictive engine to benchmark 4 distinct "
        "machine learning models. Our Champion model, Polynomial Feature Expansion, achieved an R-squared of 0.9009, reducing RMSE "
        "by 21% compared to standard linear regression. It projects European e-waste generation to reach 15.3M Mg by 2025 and 14.5M Mg "
        "by 2030, capturing accelerated device replacement cycles.")

    # ==========================================
    # SLIDE 8: MONGODB GEOSPATIAL INFRASTRUCTURE
    # ==========================================
    slide8 = prs.slides.add_slide(blank_layout)
    set_slide_background(slide8)
    add_header(slide8, "07. NoSQL Geospatial Data Layer", "MongoDB Infrastructure Modeling",
               "Semi-structured schema validation & 2dsphere indexing across European recycling plants", "08 / 10")

    # Left Column: Why NoSQL & Indexing
    add_card(slide8, Inches(0.8), Inches(1.9), Inches(5.6), Inches(4.95))
    tb8_1 = slide8.shapes.add_textbox(Inches(0.95), Inches(2.0), Inches(5.3), Inches(4.7))
    tf8_1 = tb8_1.text_frame
    tf8_1.word_wrap = True
    p = tf8_1.paragraphs[0]
    p.text = "Why NoSQL for E-Waste Infrastructure?"
    p.font.size = Pt(13)
    p.font.bold = True
    p.font.color.rgb = COLOR_TEXT_WHITE

    bullets8_1 = [
        "• Heterogeneous Metallurgy: Smelters, mechanical shredders, depollution stations, and hydrometallurgical refining plants have non-uniform schema requirements.",
        "• Dynamic Category Arrays: Nested lists of accepted categories (e.g. ['WEEE_Cat1', 'WEEE_Cat4a', 'WEEE_Cat5']).",
        "• Geospatial Indexing: Compound (latitude, longitude) index with 2dsphere spatial geometry.",
        "• Total Permitted Capacity: 1,605,000 Metric Tons/year across 18 major European facilities in 11 nations.",
        "• Proximity Queries: Calculates distance between generation hotspots and closest authorized smelters."
    ]
    for b in bullets8_1:
        p = tf8_1.add_paragraph()
        p.text = b
        p.font.size = Pt(10)
        p.font.color.rgb = COLOR_TEXT_MUTED

    # Right Column: Facility Table
    add_card(slide8, Inches(6.6), Inches(1.9), Inches(5.9), Inches(4.95))
    tb8_2 = slide8.shapes.add_textbox(Inches(6.8), Inches(2.0), Inches(5.5), Inches(0.4))
    tf8_2 = tb8_2.text_frame
    p = tf8_2.paragraphs[0]
    p.text = "Representative Modeled European Facilities"
    p.font.size = Pt(12)
    p.font.bold = True
    p.font.color.rgb = COLOR_TEXT_WHITE

    table8_shape = slide8.shapes.add_table(7, 4, Inches(6.8), Inches(2.5), Inches(5.5), Inches(2.8))
    table8 = table8_shape.table
    table8.columns[0].width = Inches(1.8)
    table8.columns[1].width = Inches(1.3)
    table8.columns[2].width = Inches(1.3)
    table8.columns[3].width = Inches(1.1)

    headers8 = ["Facility Name", "Location", "Type", "Capacity"]
    for i, h in enumerate(headers8):
        cell = table8.cell(0, i)
        cell.text = h
        cell.fill.solid()
        cell.fill.fore_color.rgb = COLOR_TABLE_HEADER
        for p in cell.text_frame.paragraphs:
            p.font.size = Pt(8.5)
            p.font.bold = True
            p.font.color.rgb = COLOR_CYAN

    data8 = [
        ("Aurubis Smelter", "Hamburg, DEU", "Smelter/Refining", "280,000 Mg"),
        ("Umicore Hoboken", "Antwerp, BEL", "Precious Metals", "150,000 Mg"),
        ("Boliden Rönnskär", "Skellefteå, SWE", "Secondary Smelter", "120,000 Mg"),
        ("Remondis Lünen", "Lünen, DEU", "Dismantling/Sort", "95,000 Mg"),
        ("Sims Lifecycle", "Daventry, GBR", "Auto Shredding", "80,000 Mg"),
        ("Montello SpA", "Bergamo, ITA", "WEEE Sorting", "70,000 Mg")
    ]
    for row_idx, row_data in enumerate(data8):
        for col_idx, val in enumerate(row_data):
            cell = table8.cell(row_idx + 1, col_idx)
            cell.text = val
            for p in cell.text_frame.paragraphs:
                p.font.size = Pt(8)
                p.font.color.rgb = COLOR_TEXT_WHITE

    tb8_sub = slide8.shapes.add_textbox(Inches(6.8), Inches(5.5), Inches(5.5), Inches(1.2))
    tf8_sub = tb8_sub.text_frame
    tf8_sub.word_wrap = True
    p = tf8_sub.paragraphs[0]
    p.text = "Schema Validation (facility_schema.json):\nEnforces required coordinates, ISO nation codes, permitted categories, and annual capacity constraints."
    p.font.size = Pt(9.5)
    p.font.color.rgb = COLOR_TEXT_MUTED

    add_speaker_notes(slide8,
        "To optimize recycling logistics, we integrated MongoDB to model European treatment infrastructure. Treatment facilities "
        "feature highly heterogeneous processes—from pyrometallurgy to hydrometallurgy. Using compound 2dsphere geospatial indexing, "
        "our system calculates spatial proximity between waste generation hotspots and authorized smelters in real time.")

    # ==========================================
    # SLIDE 9: STREAMLIT ANALYTICS DASHBOARD
    # ==========================================
    slide9 = prs.slides.add_slide(blank_layout)
    set_slide_background(slide9)
    add_header(slide9, "08. Decision Support Portal", "Interactive Streamlit Web Dashboard",
               "Glassmorphism UI serving pre-aggregated Parquet datasets with sub-second response times", "09 / 10")

    # Left Column: Serving Architecture
    add_card(slide9, Inches(0.8), Inches(1.9), Inches(5.6), Inches(4.95))
    tb9_1 = slide9.shapes.add_textbox(Inches(0.95), Inches(2.0), Inches(5.3), Inches(4.7))
    tf9_1 = tb9_1.text_frame
    tf9_1.word_wrap = True
    p = tf9_1.paragraphs[0]
    p.text = "Zero-Latency Serving Layer Design"
    p.font.size = Pt(13)
    p.font.bold = True
    p.font.color.rgb = COLOR_TEXT_WHITE

    bullets9_1 = [
        "• Serving Layer Architecture: The web dashboard never scans the 2.22 GB raw file on user interaction; it queries pre-aggregated Snappy Parquet tables and compact CSVs.",
        "• In-Memory Caching: Utilizes Streamlit's @st.cache_data decorator to cache queries and geographical GeoJSON payloads.",
        "• Glassmorphism Design: Custom dark slate theme (#0f172a), translucent cards, modern typography, and Plotly European choropleth maps.",
        "• Live Telemetry Integration: Monitors HDFS (port 9000), YARN (port 8088), Spark, and MongoDB services in real time.",
        "• Production Deployment: Hosted at http://localhost:8501."
    ]
    for b in bullets9_1:
        p = tf9_1.add_paragraph()
        p.text = b
        p.font.size = Pt(10)
        p.font.color.rgb = COLOR_TEXT_MUTED

    # Right Column: 8 Application Modules
    add_card(slide9, Inches(6.6), Inches(1.9), Inches(5.9), Inches(4.95))
    tb9_2 = slide9.shapes.add_textbox(Inches(6.8), Inches(2.0), Inches(5.5), Inches(4.7))
    tf9_2 = tb9_2.text_frame
    tf9_2.word_wrap = True
    p = tf9_2.paragraphs[0]
    p.text = "Eight Interactive Dashboard Modules"
    p.font.size = Pt(13)
    p.font.bold = True
    p.font.color.rgb = COLOR_TEXT_WHITE

    modules = [
        "1. Executive Overview: 5 KPI scorecards, historical trends, category donut chart.",
        "2. Geographic Analysis: Plotly European choropleth with bubble overlays by tonnage.",
        "3. Category Distribution: EU Directive Cat 1–6 rankings & stacked area charts.",
        "4. Scenario Comparison: BAU vs REC vs CIR policy trajectory simulation.",
        "5. Stock & Flow Balance: Mass conservation across stocks, POM, and informal scrap.",
        "6. MLlib Forecast: Historical actuals vs fitted regression line through 2035.",
        "7. Infrastructure Map: Interactive facility pins dynamically loaded from MongoDB.",
        "8. Cluster Architecture: Live daemon status badges and embedded pipeline logs."
    ]
    for m in modules:
        p = tf9_2.add_paragraph()
        p.text = m
        p.font.size = Pt(9.5)
        p.font.color.rgb = COLOR_TEXT_MUTED

    add_speaker_notes(slide9,
        "Our user interface is an 8-page interactive Streamlit dashboard. By following a serving layer design pattern where the UI "
        "queries pre-aggregated Parquet tables and cached CSVs, the dashboard achieves sub-second response times despite the "
        "17.5-million-row underlying scale. It gives environmental policy makers instant access to maps, scenario comparisons, "
        "and infrastructure gaps.")

    # ==========================================
    # SLIDE 10: CONCLUSION & THANK YOU
    # ==========================================
    slide10 = prs.slides.add_slide(blank_layout)
    set_slide_background(slide10)

    # Title Badge
    s10_badge = slide10.shapes.add_textbox(Inches(1.0), Inches(0.8), Inches(11.333), Inches(0.4))
    tf10_b = s10_badge.text_frame
    p10_b = tf10_b.paragraphs[0]
    p10_b.alignment = PP_ALIGN.CENTER
    p10_b.text = "09. SUMMARY & TECHNICAL CONTRIBUTIONS"
    p10_b.font.size = Pt(11)
    p10_b.font.bold = True
    p10_b.font.color.rgb = COLOR_CYAN

    # Main Title
    s10_title = slide10.shapes.add_textbox(Inches(1.0), Inches(1.2), Inches(11.333), Inches(1.0))
    tf10_t = s10_title.text_frame
    p10_t = tf10_t.paragraphs[0]
    p10_t.alignment = PP_ALIGN.CENTER
    p10_t.text = "Thank You! Questions & Discussion"
    p10_t.font.size = Pt(28)
    p10_t.font.bold = True
    p10_t.font.color.rgb = COLOR_TEXT_WHITE

    # Summary Grid Card
    add_card(slide10, Inches(1.5), Inches(2.2), Inches(10.333), Inches(3.6))
    tb10_grid = slide10.shapes.add_textbox(Inches(1.7), Inches(2.3), Inches(9.933), Inches(3.4))
    tf10_g = tb10_grid.text_frame
    tf10_g.word_wrap = True
    p = tf10_g.paragraphs[0]
    p.text = "Summary of Key Technical Achievements"
    p.font.size = Pt(13)
    p.font.bold = True
    p.font.color.rgb = COLOR_EMERALD

    achievements = [
        "1. 100% User-Space Big Data Cluster: Full deployment of Java 17, Hadoop 3.5, Spark 4.2, and MongoDB without root access.",
        "2. Hierarchical Deduplication: Solved multi-million-ton double counting, isolating 139,963 clean Level-1 records.",
        "3. Dual Distributed Compute: Proved MapReduce on YARN and Spark SQL parity down to the exact decimal (UK = 302.47M Mg).",
        "4. Snappy Parquet Optimization: Dropped downstream analytical query execution latency from 90+ seconds to <1 second.",
        "5. Spark MLlib Machine Learning: Fitted distributed linear regression on actuals (R² = 0.8410, RMSE = 313,674 Mg).",
        "6. MongoDB Geospatial Layer: Modeled European recycling infrastructure with compound 2dsphere indexing.",
        "7. Decision-Support Portal: Created an 8-page glassmorphism Streamlit web dashboard with interactive Plotly maps."
    ]
    for a in achievements:
        p = tf10_g.add_paragraph()
        p.text = a
        p.font.size = Pt(10)
        p.font.color.rgb = COLOR_TEXT_MUTED

    # Bottom Pill Links
    s10_links = slide10.shapes.add_textbox(Inches(1.5), Inches(6.0), Inches(10.333), Inches(0.8))
    tf10_l = s10_links.text_frame
    p10_l = tf10_l.paragraphs[0]
    p10_l.alignment = PP_ALIGN.CENTER
    p10_l.text = "Live Dashboard: http://localhost:8501   |   Codebase: ~/ewaste-bigdata\nHandbook: PROJECT_EXPLANATION_AND_CONCEPTS.md"
    p10_l.font.size = Pt(11)
    p10_l.font.bold = True
    p10_l.font.color.rgb = COLOR_CYAN

    add_speaker_notes(slide10,
        "To conclude, we have successfully designed, built, and validated an end-to-end Big Data engineering platform "
        "for continental electronic waste analytics. We solved core domain data traps, verified dual distributed compute engines, "
        "and delivered a production-ready analytical dashboard. Thank you for your time and attention. I am now pleased to take your questions.")

    # Save presentation
    output_path = "/home/rishabh/ewaste-bigdata/presentation/E_Waste_Big_Data_Analytics_Presentation.pptx"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    prs.save(output_path)
    print(f"Presentation saved successfully to: {output_path}")

if __name__ == "__main__":
    build_presentation()
