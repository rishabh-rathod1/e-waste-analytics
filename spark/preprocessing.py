#!/usr/bin/env python3
"""
PySpark Preprocessing Pipeline for FutuRaM WEEE Dataset
Reads raw CSV from HDFS, applies schema validation, cleans hierarchical data,
and produces Snappy-compressed Parquet datasets for downstream analytics.
"""

import os
import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)
from pyspark.sql.functions import (
    col, trim, upper, when, sum as _sum, round as _round, count as _count
)

def create_spark_session():
    return (
        SparkSession.builder
        .appName("EWaste-Preprocessing-Pipeline")
        .master("local[8]")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )

def main():
    start_time = time.time()
    print("=" * 60)
    print("Starting PySpark Preprocessing Pipeline")
    print("=" * 60)
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # 1. Define explicit schema matching FutuRaM WEEE structure
    schema = StructType([
        StructField("Waste Stream", StringType(), True),
        StructField("Location", StringType(), True),
        StructField("Year", IntegerType(), True),
        StructField("Scenario", StringType(), True),
        StructField("additionalSpecification", StringType(), True),
        StructField("Stock/Flow ID", StringType(), True),
        StructField("Layer 1", StringType(), True),
        StructField("Layer 2", StringType(), True),
        StructField("Layer 3", StringType(), True),
        StructField("Layer 4", StringType(), True),
        StructField("Value", DoubleType(), True),
        StructField("Unit", StringType(), True)
    ])

    raw_hdfs_path = "hdfs://localhost:9000/ewaste/raw/futuram/WEEE_SF_cmp_version2.csv"
    print(f"Reading raw CSV from HDFS: {raw_hdfs_path}")

    # 2. Read raw CSV
    df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("mode", "DROPMALFORMED")
        .schema(schema)
        .load(raw_hdfs_path)
    )

    print("\n=== Dataset Schema ===")
    df.printSchema()

    raw_count = df.count()
    print(f"\nTotal Raw Records Read: {raw_count:,}")

    print("\nSample Raw Records:")
    df.select("Location", "Year", "Scenario", "Stock/Flow ID", "Layer 1", "Layer 2", "Value", "Unit").show(5, truncate=False)

    # 3. Clean and Normalize Columns
    cleaned_df = (
        df
        .filter(col("Location").isNotNull() & col("Year").isNotNull() & col("Value").isNotNull())
        .filter(col("Value") >= 0)
        .withColumn("Location", upper(trim(col("Location"))))
        .withColumn("Scenario", upper(trim(col("Scenario"))))
        .withColumn("Stock_Flow", trim(col("Stock/Flow ID")))
        .withColumn("Category", trim(col("Layer 1")))
        .withColumn("Layer2", trim(col("Layer 2")))
        .withColumn("Flow_Type", 
            when(col("Stock_Flow").like("%generated%"), "Generation")
            .when(col("Stock_Flow") == "WEEE_collected", "Collection")
            .when(col("Stock_Flow") == "WEEE_EEEPOM", "Put_On_Market")
            .otherwise("Other_Flow")
        )
    )

    cleaned_count = cleaned_df.count()
    print(f"Records After Cleaning / Null Filtering: {cleaned_count:,}")

    # 4. Filter for Level 1 Summary Rows to Avoid Hierarchical Double Counting
    # In FutuRaM, Layer 2 is empty when the row represents the category/location total
    summary_df = cleaned_df.filter(
        (col("Layer2").isNull()) | (col("Layer2") == "") | (col("Layer2") == "null")
    ).cache()

    summary_count = summary_df.count()
    print(f"Category-Level Summary Records (Layer 2 is blank): {summary_count:,}")

    # HDFS and Local output paths
    hdfs_parquet_base = "hdfs://localhost:9000/ewaste/processed/parquet"
    local_processed_base = "/home/rishabh/ewaste-bigdata/data/processed"
    results_base = "/home/rishabh/ewaste-bigdata/output/results"
    
    os.makedirs(local_processed_base, exist_ok=True)
    os.makedirs(results_base, exist_ok=True)

    # 5. Aggregation A: Yearly E-Waste by Flow Type and Scenario
    print("\nGenerating Yearly E-Waste Aggregation...")
    yearly_df = (
        summary_df
        .groupBy("Year", "Scenario", "Flow_Type")
        .agg(_round(_sum("Value"), 2).alias("Total_WEEE_Mg"))
        .orderBy("Year", "Scenario", "Flow_Type")
    )
    yearly_df.show(15, truncate=False)
    yearly_df.write.mode("overwrite").parquet(f"{hdfs_parquet_base}/yearly_ewaste")
    yearly_df.write.mode("overwrite").parquet(f"{local_processed_base}/yearly_ewaste")
    yearly_df.toPandas().to_csv(f"{results_base}/yearly_ewaste.csv", index=False)

    # 6. Aggregation B: Location-wise E-Waste Generation
    print("\nGenerating Location-wise E-Waste Aggregation...")
    location_gen_df = (
        summary_df
        .filter(col("Flow_Type") == "Generation")
        .groupBy("Location", "Year", "Scenario")
        .agg(_round(_sum("Value"), 2).alias("Generated_WEEE_Mg"))
        .orderBy("Location", "Year", "Scenario")
    )
    location_gen_df.write.mode("overwrite").parquet(f"{hdfs_parquet_base}/location_ewaste")
    location_gen_df.write.mode("overwrite").parquet(f"{local_processed_base}/location_ewaste")

    # Top locations overall (OBS + modeled totals)
    top_locations_df = (
        summary_df
        .filter(col("Flow_Type") == "Generation")
        .groupBy("Location")
        .agg(_round(_sum("Value"), 2).alias("Total_Generated_Mg"))
        .orderBy(col("Total_Generated_Mg").desc())
    )
    top_locations_df.show(10, truncate=False)
    top_locations_df.toPandas().to_csv(f"{results_base}/top_locations.csv", index=False)

    # 7. Aggregation C: Category Distribution
    print("\nGenerating Category-wise E-Waste Aggregation...")
    category_df = (
        summary_df
        .filter((col("Flow_Type") == "Generation") & (col("Category") != ""))
        .groupBy("Category", "Year", "Scenario")
        .agg(_round(_sum("Value"), 2).alias("Generated_WEEE_Mg"))
        .orderBy("Category", "Year", "Scenario")
    )
    category_df.write.mode("overwrite").parquet(f"{hdfs_parquet_base}/category_ewaste")
    category_df.write.mode("overwrite").parquet(f"{local_processed_base}/category_ewaste")

    category_summary = (
        summary_df
        .filter((col("Flow_Type") == "Generation") & (col("Category") != ""))
        .groupBy("Category")
        .agg(_round(_sum("Value"), 2).alias("Total_Generated_Mg"))
        .orderBy(col("Total_Generated_Mg").desc())
    )
    category_summary.show(truncate=False)
    category_summary.toPandas().to_csv(f"{results_base}/category_summary.csv", index=False)

    # 8. Aggregation D: Scenario Comparison
    print("\nGenerating Scenario Comparison Aggregation...")
    scenario_df = (
        summary_df
        .filter(col("Flow_Type") == "Generation")
        .groupBy("Scenario", "Year")
        .agg(_round(_sum("Value"), 2).alias("Generated_WEEE_Mg"))
        .orderBy("Scenario", "Year")
    )
    scenario_df.write.mode("overwrite").parquet(f"{hdfs_parquet_base}/scenario_ewaste")
    scenario_df.write.mode("overwrite").parquet(f"{local_processed_base}/scenario_ewaste")
    scenario_df.toPandas().to_csv(f"{results_base}/scenario_summary.csv", index=False)

    # 9. Aggregation E: Detailed Stock/Flow Summary
    print("\nGenerating Stock/Flow Distribution Aggregation...")
    flow_df = (
        summary_df
        .groupBy("Stock_Flow", "Year", "Scenario")
        .agg(_round(_sum("Value"), 2).alias("Total_Value_Mg"))
        .orderBy("Stock_Flow", "Year", "Scenario")
    )
    flow_df.write.mode("overwrite").parquet(f"{hdfs_parquet_base}/flow_ewaste")
    flow_df.write.mode("overwrite").parquet(f"{local_processed_base}/flow_ewaste")

    flow_summary = (
        summary_df
        .groupBy("Stock_Flow")
        .agg(_round(_sum("Value"), 2).alias("Total_Value_Mg"))
        .orderBy(col("Total_Value_Mg").desc())
    )
    flow_summary.show(truncate=False)
    flow_summary.toPandas().to_csv(f"{results_base}/flow_summary.csv", index=False)

    elapsed = time.time() - start_time
    print(f"\nPre-processing Pipeline Completed in {elapsed:.2f} seconds.")
    print("=" * 60)
    spark.stop()

if __name__ == "__main__":
    main()
