#!/usr/bin/env python3
"""
Dataset Profiling & Data Quality Script
Analyzes the FutuRaM WEEE dataset:
- Evaluates total record count, null values, invalid numerics
- Identifies distinct locations, year range, scenarios, stock/flow IDs, Layer 1 categories, units
- Produces output/results/data_quality.csv and output/reports/profiling_report.txt
"""

import os
import sys
import csv
import time
from pathlib import Path
from collections import Counter

def profile_dataset(file_path=None):
    if file_path is None:
        file_path = os.environ.get("DATASET_PATH", os.path.expanduser("~/Downloads/DATASET/WEEE_SF_cmp_version2.csv"))
    
    file_path = Path(file_path).resolve()
    print(f"=== Starting Profiling on Dataset: {file_path.name} ===")
    start_time = time.time()
    
    if not file_path.exists():
        print(f"ERROR: Dataset file not found: {file_path}")
        sys.exit(1)

    file_size_bytes = file_path.stat().st_size
    file_size_gb = file_size_bytes / (1024 ** 3)
    print(f"File Size: {file_size_gb:.2f} GB ({file_size_bytes:,} bytes)")
    
    total_records = 0
    null_counts = Counter()
    invalid_numeric_count = 0
    locations = set()
    years = set()
    scenarios = set()
    stock_flows = set()
    layer1_categories = set()
    units = set()
    
    header = None
    
    with open(file_path, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f)
        try:
            header = next(reader)
        except StopIteration:
            print("ERROR: Empty file!")
            sys.exit(1)
            
        print(f"Detected Columns ({len(header)}): {header}")
        
        col_indices = {name.strip('"'): idx for idx, name in enumerate(header)}
        loc_idx = col_indices.get("Location")
        yr_idx = col_indices.get("Year")
        scen_idx = col_indices.get("Scenario")
        sf_idx = col_indices.get("Stock/Flow ID")
        l1_idx = col_indices.get("Layer 1")
        val_idx = col_indices.get("Value")
        unit_idx = col_indices.get("Unit")
        
        batch_size = 1_000_000
        for row in reader:
            total_records += 1
            
            # Check column lengths
            if len(row) < len(header):
                # row with missing trailing columns
                for c_idx in range(len(row), len(header)):
                    null_counts[header[c_idx]] += 1
            
            for idx, val in enumerate(row):
                if idx < len(header):
                    if val == "" or val.lower() == "null" or val.lower() == "na":
                        null_counts[header[idx]] += 1
            
            # Dimension extractions
            if loc_idx is not None and loc_idx < len(row):
                loc = row[loc_idx].strip()
                if loc:
                    locations.add(loc)
                    
            if yr_idx is not None and yr_idx < len(row):
                yr_str = row[yr_idx].strip()
                if yr_str.isdigit():
                    years.add(int(yr_str))
                    
            if scen_idx is not None and scen_idx < len(row):
                scen = row[scen_idx].strip()
                if scen:
                    scenarios.add(scen)
                    
            if sf_idx is not None and sf_idx < len(row):
                sf = row[sf_idx].strip()
                if sf:
                    stock_flows.add(sf)
                    
            if l1_idx is not None and l1_idx < len(row):
                l1 = row[l1_idx].strip()
                if l1:
                    layer1_categories.add(l1)
                    
            if unit_idx is not None and unit_idx < len(row):
                u = row[unit_idx].strip()
                if u:
                    units.add(u)
                    
            if val_idx is not None and val_idx < len(row):
                val_str = row[val_idx].strip()
                try:
                    float(val_str)
                except ValueError:
                    invalid_numeric_count += 1
                    
            if total_records % batch_size == 0:
                elapsed = time.time() - start_time
                print(f"  Processed {total_records:,} records... ({elapsed:.1f}s)")
                
    elapsed_total = time.time() - start_time
    print(f"\nProfiling Complete in {elapsed_total:.2f} seconds.")
    print(f"Total Rows Processed: {total_records:,}")
    print(f"Invalid Numeric Rows: {invalid_numeric_count:,}")
    print(f"Year Range: {min(years) if years else 'N/A'} - {max(years) if years else 'N/A'}")
    print(f"Distinct Locations ({len(locations)}): {sorted(list(locations))}")
    print(f"Distinct Scenarios ({len(scenarios)}): {sorted(list(scenarios))}")
    print(f"Distinct Stock/Flow IDs ({len(stock_flows)}): {sorted(list(stock_flows))}")
    print(f"Distinct Layer 1 Categories ({len(layer1_categories)}): {sorted(list(layer1_categories))}")
    print(f"Distinct Units ({len(units)}): {sorted(list(units))}")
    
    # Save Data Quality CSV
    os.makedirs("/home/rishabh/ewaste-bigdata/output/results", exist_ok=True)
    os.makedirs("/home/rishabh/ewaste-bigdata/output/reports", exist_ok=True)
    
    dq_csv = "/home/rishabh/ewaste-bigdata/output/results/data_quality.csv"
    with open(dq_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Metric", "Value", "Description"])
        writer.writerow(["total_records", total_records, "Total records in dataset"])
        writer.writerow(["file_size_gb", f"{file_size_gb:.2f}", "File size on disk in GB"])
        writer.writerow(["year_min", min(years) if years else 0, "Earliest recorded year"])
        writer.writerow(["year_max", max(years) if years else 0, "Latest projected year"])
        writer.writerow(["location_count", len(locations), "Number of distinct geographic locations"])
        writer.writerow(["scenario_count", len(scenarios), "Number of scenarios (e.g. OBS, BAU, REC, CIR)"])
        writer.writerow(["stock_flow_count", len(stock_flows), "Number of stock/flow classifications"])
        writer.writerow(["category_count", len(layer1_categories), "Number of Layer 1 WEEE categories"])
        writer.writerow(["unit_count", len(units), "Distinct measurement units"])
        writer.writerow(["invalid_numeric_records", invalid_numeric_count, "Rows with non-parseable numeric values"])
        for col in header:
            writer.writerow([f"null_count_{col.strip('\"')}", null_counts[col], f"Null/blank count in column {col}"])
            
    print(f"\nSaved Data Quality Report to: {dq_csv}")
    return dq_csv

if __name__ == "__main__":
    profile_dataset()
