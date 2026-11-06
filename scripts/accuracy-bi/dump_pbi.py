#!/usr/bin/env python3
"""Print every report's semantic-model partitions (M source), to derive the
Power BI ground truth by hand. Usage: dump_pbi.py <dir>"""
import json
import re
import sys
from pathlib import Path

root = Path(sys.argv[1])
for pbir in sorted(root.rglob("definition.pbir")):
    report = pbir.parent.name.removesuffix(".Report")
    ref = json.loads(pbir.read_text(encoding="utf-8-sig")).get("datasetReference", {})
    path = ref.get("byPath", {}).get("path")
    print(f"=== REPORT {report} -> {path or ref}")
    if not path:
        continue
    model = (pbir.parent / path).resolve()
    for tmdl in sorted((model / "definition").rglob("*.tmdl")):
        text = tmdl.read_text(encoding="utf-8-sig")
        for m in re.finditer(r"^\s*(partition|expression) (.+?)=\s*(\w+)?\s*$(.*?)(?=^\s*(?:partition|expression|column|measure|annotation|table|hierarchy|ref)\b|\Z)",
                             text, re.M | re.S):
            body = "\n".join(l.strip() for l in m.group(4).splitlines() if l.strip())
            print(f"--- {tmdl.relative_to(model)} :: {m.group(1)} {m.group(2).strip()} [{m.group(3)}]\n{body[:600]}")
    bim = model / "model.bim"
    if bim.exists():
        print("  (model.bim present)")
