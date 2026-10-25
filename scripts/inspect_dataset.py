#!/usr/bin/env python3
"""
Inspect Dataset Script
Discovers and audits the FutuRaM WEEE dataset files in ~/Downloads/DATASET.
Identifies file sizes, headers, lines, and logs structural metadata.
"""

import os
import sys
import csv
from pathlib import Path

def discover_datasets(base_dir=None):
    if base_dir is None:
        base_dir = os.environ.get("DATASET_DIR", os.path.expanduser("~/Downloads/DATASET"))
    
    path = Path(base_dir).resolve()
    print(f"=== Scanning Dataset Directory: {path} ===")
    
    if not path.exists():
        print(f"ERROR: Dataset directory does not exist: {path}")
        sys.exit(1)
        
    found_files = []
    for item in sorted(path.rglob("*")):
        if item.is_file():
            size_bytes = item.stat().st_size
            size_mb = size_bytes / (1024 * 1024)
            found_files.append((item, size_bytes, size_mb))
            print(f"  [{size_mb:8.2f} MB] {item.name} -> {item}")

    csv_files = [f for f in found_files if f[0].suffix.lower() == ".csv"]
    print(f"\nFound {len(csv_files)} CSV files:")
    
    primary_csv = None
    max_size = 0
    for file_path, size_bytes, size_mb in csv_files:
        print(f"\nAnalyzing: {file_path.name} ({size_mb:.2f} MB)")
        try:
            with open(file_path, "r", encoding="utf-8", errors="replace") as f:
                reader = csv.reader(f)
                header = next(reader)
                print(f"  Header columns ({len(header)}): {header}")
                first_row = next(reader)
                print(f"  Sample row 1: {first_row}")
        except Exception as e:
            print(f"  Error reading header: {e}")
            
        if size_bytes > max_size:
            max_size = size_bytes
            primary_csv = file_path

    if primary_csv:
        print(f"\n===> Selected Primary WEEE Stock/Flow Dataset: {primary_csv.name} ({max_size / (1024*1024):.2f} MB)")
    else:
        print("\nERROR: No suitable CSV file found!")
        sys.exit(1)
        
    return primary_csv

if __name__ == "__main__":
    discover_datasets()
