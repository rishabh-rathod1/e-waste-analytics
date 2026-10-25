#!/usr/bin/env python3
"""
MapReduce Reducer for WEEE Generation Analysis
Aggregates total generated WEEE (in Megagrams / Metric Tons) per Location.
Emits: Location <tab> Total_WEEE_Mg
"""

import sys

def main():
    current_location = None
    current_total = 0.0

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
            
        parts = line.split("\t")
        if len(parts) != 2:
            continue
            
        location, val_str = parts[0].strip(), parts[1].strip()
        try:
            val = float(val_str)
        except ValueError:
            continue

        if current_location == location:
            current_total += val
        else:
            if current_location is not None:
                print(f"{current_location}\t{current_total:.4f}")
            current_location = location
            current_total = val

    if current_location is not None:
        print(f"{current_location}\t{current_total:.4f}")

if __name__ == "__main__":
    main()
