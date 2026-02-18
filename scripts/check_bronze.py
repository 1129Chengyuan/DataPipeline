#!/usr/bin/env python3
"""Quick count of files in each Bronze subdirectory."""
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from nba_etl.config import settings

bronze = settings.bronze_path
total = 0
for folder in sorted(os.listdir(bronze)):
    path = os.path.join(bronze, folder)
    if os.path.isdir(path):
        count = len([f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))])
        total += count
        print(f"  {folder:15s} {count:,}")
print(f"  {'TOTAL':15s} {total:,}")
