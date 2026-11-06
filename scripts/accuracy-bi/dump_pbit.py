#!/usr/bin/env python3
"""Print the partition sources of .pbit templates (DataModelSchema), to
derive the Power BI ground truth by hand. Usage: dump_pbit.py <dir>"""
import json
import sys
import zipfile
from pathlib import Path

for pbit in sorted(Path(sys.argv[1]).rglob("*.pbit")):
    with zipfile.ZipFile(pbit) as z:
        schema = json.loads(z.read("DataModelSchema").decode("utf-16-le").lstrip("﻿"))
    print(f"=== {pbit.stem}")
    for table in schema["model"].get("tables", []):
        for part in table.get("partitions", []):
            src = part.get("source", {})
            expr = src.get("expression", "")
            if isinstance(expr, list):
                expr = "\n".join(expr)
            lines = [l.strip() for l in expr.splitlines()
                     if "Kind=" in l or "Item=" in l or "NativeQuery" in l or '{[Name="' in l]
            print(f"  {table['name']} [{src.get('type')}] " + " | ".join(lines)[:220])
