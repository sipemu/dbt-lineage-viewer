"""Compare dbt-lineage's column lineage with the sqlglot reference.

Usage: python compare.py <reference.json> <dbt-lineage column-lineage.json>

Only models the reference could trace are compared, and only edges from
upstream relations whose columns the reference knew. dbt-lineage's `*` edges
are ignored: column impact follows named columns only.
"""

import json
import sys
from collections import Counter

ref = json.load(open(sys.argv[1]))
ours_raw = json.load(open(sys.argv[2]))["edges"]
measured = set(ref["measured"])
known = set(ref.get("known", [])) | measured

R = {tuple(e) for e in ref["edges"] if e[2] in measured and e[0] in known}
O = {
    (e["source_node"], e["source_column"].lower(), e["target_node"], e["target_column"].lower())
    for e in ours_raw
    if e["target_node"] in measured
    and e["source_node"] in known
    and "*" not in (e["source_column"], e["target_column"])
}
hit = R & O
ref_cols = {(e[2], e[3]) for e in R}
our_cols = {(e[2], e[3]) for e in O}
print(
    json.dumps(
        {
            "models": len(measured),
            "ref_edges": len(R),
            "our_edges": len(O),
            "matched": len(hit),
            "recall": round(100 * len(hit) / max(len(R), 1), 1),
            "precision": round(100 * len(hit) / max(len(O), 1), 1),
            "columns_with_any_lineage": round(100 * len(ref_cols & our_cols) / max(len(ref_cols), 1), 1),
        }
    )
)
if "-v" in sys.argv:
    by_model = Counter(e[2] for e in R - O)
    print("most missed models:", by_model.most_common(8))
    for e in sorted(R - O)[:15]:
        print("  missed", e)
    for e in sorted(O - R)[:10]:
        print("  extra ", e)
