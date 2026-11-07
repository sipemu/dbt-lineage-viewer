#!/usr/bin/env python3
"""Check the GitLab CI/CD component in integrations/gitlab offline-ish.

Validates the component and the catalog project's .gitlab-ci.yml against
GitLab's CI JSON schema (the one the pipeline editor uses), and checks what
the CI/CD Catalog expects: templates/<name>.yml with a `spec:inputs` header,
every input typed, defaulted and described, every `$[[ inputs.x ]]` declared
and every input used, plus a README.md at the project root.

    pip install jsonschema pyyaml
    python3 scripts/check-gitlab-component.py [--schema ci.json]

Without --schema the schema is downloaded from gitlab.com.
"""

import argparse
import json
import pathlib
import re
import sys
import urllib.request

import jsonschema
import yaml

SCHEMA_URL = (
    "https://gitlab.com/gitlab-org/gitlab/-/raw/master/"
    "app/assets/javascripts/editor/schema/ci.json"
)
ROOT = pathlib.Path(__file__).resolve().parent.parent / "integrations" / "gitlab"
INPUT_RE = re.compile(r"\$\[\[\s*inputs\.([A-Za-z0-9_-]+)\s*\]\]")
TYPES = {"string": str, "boolean": bool, "number": (int, float), "array": list}


def load_schema(path):
    if path:
        return json.loads(pathlib.Path(path).read_text())
    with urllib.request.urlopen(SCHEMA_URL, timeout=30) as r:
        return json.load(r)


def validate(validator, doc, what, errors):
    for e in sorted(validator.iter_errors(doc), key=lambda e: list(e.path)):
        loc = "/".join(str(p) for p in e.path) or "<root>"
        errors.append(f"{what}: {loc}: {e.message[:300]}")


def check_component(path, validator, errors):
    text = path.read_text()
    docs = text.split("\n---\n", 1)
    if len(docs) != 2:
        errors.append(f"{path}: expected a spec header, `---`, then the jobs")
        return
    header = yaml.safe_load(docs[0])
    validate(validator, header, f"{path} (spec header)", errors)
    inputs = (header.get("spec") or {}).get("inputs") or {}
    if not inputs:
        errors.append(f"{path}: spec:inputs is empty")
    for name, spec in inputs.items():
        for key in ("type", "default", "description"):
            if key not in spec:
                errors.append(f"{path}: input {name} has no {key}")
        want = TYPES.get(spec.get("type", "string"))
        if want is None:
            errors.append(f"{path}: input {name} has unknown type {spec.get('type')}")
        elif "default" in spec and not isinstance(spec["default"], want):
            errors.append(f"{path}: input {name} default does not match its type")

    used = set(INPUT_RE.findall(docs[1]))
    for name in sorted(used - set(inputs)):
        errors.append(f"{path}: $[[ inputs.{name} ]] is not declared")
    for name in sorted(set(inputs) - used):
        errors.append(f"{path}: input {name} is never used")

    # Interpolate the defaults the way GitLab does, then validate the jobs.
    def subst(m):
        v = inputs.get(m.group(1), {}).get("default", "")
        return str(v).lower() if isinstance(v, bool) else str(v)

    jobs = yaml.safe_load(INPUT_RE.sub(subst, docs[1]))
    validate(validator, jobs, f"{path} (jobs)", errors)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--schema", help="local copy of GitLab's ci.json")
    args = ap.parse_args()
    validator = jsonschema.Draft7Validator(load_schema(args.schema))
    errors = []

    if not (ROOT / "README.md").is_file():
        errors.append(f"{ROOT}: README.md is missing")
    templates = sorted((ROOT / "templates").glob("*.yml")) + sorted(
        (ROOT / "templates").glob("*/template.yml")
    )
    if not templates:
        errors.append(f"{ROOT}/templates: no components")
    for t in templates:
        check_component(t, validator, errors)

    pipeline = ROOT / ".gitlab-ci.yml"
    validate(validator, yaml.safe_load(pipeline.read_text()), str(pipeline), errors)

    for e in errors:
        print(e, file=sys.stderr)
    if errors:
        sys.exit(1)
    print(f"OK: {len(templates)} component(s) and .gitlab-ci.yml")


if __name__ == "__main__":
    main()
