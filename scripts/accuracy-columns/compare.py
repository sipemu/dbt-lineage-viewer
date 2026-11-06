"""Compare dbt-lineage's column lineage with the sqlglot reference.

Usage: python compare.py <reference.json> <dbt-lineage column-lineage.json> [-v]

Only models the reference could trace are compared, and only edges from
upstream relations whose columns the reference knew. dbt-lineage's `*` edges
are ignored: column impact follows named columns only.
"""

import json
import sys
from collections import Counter


def edge_sets(ref, ours_raw):
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
    return measured, R, O


def compare(ref, ours_raw):
    """Recall/precision of dbt-lineage's column edges against the reference."""
    measured, R, O = edge_sets(ref, ours_raw)
    hit = R & O
    return {
        "models": len(measured),
        "ref_edges": len(R),
        "our_edges": len(O),
        "matched": len(hit),
        "recall": round(100 * len(hit) / max(len(R), 1), 1),
        "precision": round(100 * len(hit) / max(len(O), 1), 1),
    }


def main():
    ref = json.load(open(sys.argv[1]))
    ours = json.load(open(sys.argv[2]))["edges"]
    print(json.dumps(compare(ref, ours)))
    if "-v" in sys.argv:
        _, R, O = edge_sets(ref, ours)
        print("most missed models:", Counter(e[2] for e in R - O).most_common(8))
        for e in sorted(R - O)[:15]:
            print("  missed", e)
        for e in sorted(O - R)[:10]:
            print("  extra ", e)


if __name__ == "__main__":
    main()
