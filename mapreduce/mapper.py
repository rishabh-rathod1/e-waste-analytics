#!/usr/bin/env python3
"""
MapReduce Mapper for WEEE Generation Analysis
Filters for primary WEEE generation flows at Layer 1 category level
(avoiding double-counting Layer 2/3/4 sub-allocations)
Emits: Location <tab> Value (in Mg)
"""

import sys
import csv

def main():
    # Use csv reader to properly handle quoted values and commas
    reader = csv.reader(sys.stdin)
    
    for row in reader:
        if not row or len(row) < 11:
            continue
            
        # Skip header
        if row[0].strip('"').lower() == "waste stream" or row[1].strip('"').lower() == "location":
            continue
            
        location = row[1].strip()
        stock_flow = row[5].strip()
        layer1 = row[6].strip()
        layer2 = row[7].strip() if len(row) > 7 else ""
        raw_val = row[10].strip()
        
        # We focus on WEEE generation flows (dedicated and complementary streams)
        # and evaluate at Layer 1 summary level (layer2 is blank) to avoid double-counting subcomponents
        if "generated" in stock_flow.lower() and layer1 != "" and layer2 == "":
            try:
                val = float(raw_val)
                if val > 0:
                    print(f"{location}\t{val:.6f}")
            except ValueError:
                continue

if __name__ == "__main__":
    main()
