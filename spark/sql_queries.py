#!/usr/bin/env python3
"""
Spark SQL Analytical Engine for E-Waste Big Data
Executes SQL queries over Parquet datasets and generates analytical reports.
Queries:
  A. Year-wise WEEE Generation
  B. Top 10 Locations
  C. Category Distribution
  D. Scenario Comparison
  E. Flow Analysis
"""

import os
import sys
import time
from pyspark.sql import SparkSession

def main():
    start_time = time.time()
    print("=" * 60)
    print("Executing Spark SQL Analytical Queries")
    print("=" * 60)

    spark = (
        SparkSession.builder
        .appName("EWaste-SparkSQL-Analytics")
        .master("local[8]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    parquet_base = "hdfs://localhost:9000/ewaste/processed/parquet"
    results_dir = "/home/rishabh/ewaste-bigdata/output/results"
    os.makedirs(results_dir, exist_ok=True)

    # Register temporary views over Parquet tables
    spark.read.parquet(f"{parquet_base}/yearly_ewaste").createOrReplaceTempView("yearly_ewaste")
    spark.read.parquet(f"{parquet_base}/location_ewaste").createOrReplaceTempView("location_ewaste")
    spark.read.parquet(f"{parquet_base}/category_ewaste").createOrReplaceTempView("category_ewaste")
    spark.read.parquet(f"{parquet_base}/scenario_ewaste").createOrReplaceTempView("scenario_ewaste")
    spark.read.parquet(f"{parquet_base}/flow_ewaste").createOrReplaceTempView("flow_ewaste")

    print("\n--- Query A: Year-wise Total WEEE Generation (OBS historical + BAU forecast) ---")
    query_a = """
        SELECT Year, 
               Scenario, 
               SUM(Total_WEEE_Mg) as Total_WEEE_Mg
        FROM yearly_ewaste
        WHERE Flow_Type = 'Generation'
        GROUP BY Year, Scenario
        ORDER BY Year, Scenario
    """
    df_a = spark.sql(query_a)
    df_a.show(20, truncate=False)
    df_a.toPandas().to_csv(f"{results_dir}/sql_yearly_ewaste.csv", index=False)

    print("\n--- Query B: Top 10 High-Waste Locations ---")
    query_b = """
        SELECT Location,
               ROUND(SUM(Generated_WEEE_Mg), 2) as Total_Generated_Mg,
               ROUND(AVG(Generated_WEEE_Mg), 2) as Avg_Annual_Generated_Mg
        FROM location_ewaste
        GROUP BY Location
        ORDER BY Total_Generated_Mg DESC
        LIMIT 10
    """
    df_b = spark.sql(query_b)
    df_b.show(truncate=False)
    df_b.toPandas().to_csv(f"{results_dir}/sql_top_locations.csv", index=False)

    print("\n--- Query C: Category Distribution (EU WEEE Categories 1 to 6) ---")
    query_c = """
        SELECT Category,
               ROUND(SUM(Generated_WEEE_Mg), 2) as Total_Generated_Mg,
               ROUND(SUM(Generated_WEEE_Mg) * 100.0 / (SELECT SUM(Generated_WEEE_Mg) FROM category_ewaste), 2) as Share_Percentage
        FROM category_ewaste
        WHERE Category IS NOT NULL AND Category != ''
        GROUP BY Category
        ORDER BY Total_Generated_Mg DESC
    """
    df_c = spark.sql(query_c)
    df_c.show(truncate=False)
    df_c.toPandas().to_csv(f"{results_dir}/sql_category_distribution.csv", index=False)

    print("\n--- Query D: Scenario Comparison (Total Modeled Waste Generation) ---")
    query_d = """
        SELECT Scenario,
               ROUND(SUM(Generated_WEEE_Mg), 2) as Total_Generated_Mg,
               MIN(Year) as Start_Year,
               MAX(Year) as End_Year
        FROM scenario_ewaste
        GROUP BY Scenario
        ORDER BY Total_Generated_Mg DESC
    """
    df_d = spark.sql(query_d)
    df_d.show(truncate=False)
    df_d.toPandas().to_csv(f"{results_dir}/sql_scenario_comparison.csv", index=False)

    print("\n--- Query E: Stock/Flow Mass Balance Analysis ---")
    query_e = """
        SELECT Stock_Flow,
               ROUND(SUM(Total_Value_Mg), 2) as Total_Mass_Mg
        FROM flow_ewaste
        GROUP BY Stock_Flow
        ORDER BY Total_Mass_Mg DESC
    """
    df_e = spark.sql(query_e)
    df_e.show(truncate=False)
    df_e.toPandas().to_csv(f"{results_dir}/sql_flow_analysis.csv", index=False)

    elapsed = time.time() - start_time
    print(f"\nSpark SQL Queries Completed in {elapsed:.2f} seconds.")
    print("=" * 60)
    spark.stop()

if __name__ == "__main__":
    main()
