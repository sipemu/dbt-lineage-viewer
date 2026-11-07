#!/usr/bin/env python3
"""End-to-end test of the pipe image, standard library only.

Builds the pipe image, creates a tiny git repository with a dbt project in a
subdirectory, and runs the image's entrypoint the way Bitbucket Pipelines
does (clone mounted at /opt/atlassian/pipelines/agent/build, BITBUCKET_*
variables) against mock_bitbucket.py, which plays both the Bitbucket API and
the Pipelines auth proxy. Nothing leaves the machine.

    python3 integrations/bitbucket/test/test_pipe.py

Environment:
    PIPE_IMAGE   use this pipe image instead of building one
    BASE_IMAGE   base for the pipe build (default: build the repository's
                 root Dockerfile as dbt-lineage:pipe-test)
    TEST_TMPDIR  where the fixture repository is created (default: system tmp)

Skips (exit 0) when Docker is not available.
"""

import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
PIPE_DIR = HERE.parent
REPO_ROOT = PIPE_DIR.parent.parent
FIXTURE = HERE / "fixture"
CLONE_DIR = "/opt/atlassian/pipelines/agent/build"
N_BROKEN = 110  # models with an undefined source: > 100 annotations


def docker_available():
    if shutil.which("docker") is None:
        return False
    return subprocess.run(["docker", "info"], capture_output=True).returncode == 0


def sh(*args, cwd=None, check=True, **kw):
    return subprocess.run(list(args), cwd=cwd, check=check, text=True, capture_output=True, **kw)


def build_image():
    if os.environ.get("PIPE_IMAGE"):
        return os.environ["PIPE_IMAGE"]
    base = os.environ.get("BASE_IMAGE")
    if not base:
        base = "dbt-lineage:pipe-test"
        print(f"building {base} from {REPO_ROOT}/Dockerfile (cached after the first run)", flush=True)
        subprocess.run(["docker", "build", "-q", "-t", base, str(REPO_ROOT)], check=True)
    image = "dbt-lineage-pipe:test"
    subprocess.run(
        ["docker", "build", "-q", "--build-arg", f"BASE_IMAGE={base}", "-t", image, str(PIPE_DIR)],
        check=True,
    )
    return image


def make_repo(root: Path):
    """main: the fixture project under dbt/. feature: a changed model plus
    N_BROKEN models reading an undefined source (lint errors)."""
    sh("git", "init", "-q", "-b", "main", str(root))
    for k, v in [
        ("user.email", "t@example.com"),
        ("user.name", "t"),
        ("commit.gpgsign", "false"),
        # As in a Pipelines clone: git traffic through the step's proxy.
        ("http.http://bitbucket.org/ws/repo.proxy", "http://localhost:29418"),
    ]:
        sh("git", "config", k, v, cwd=root)
    shutil.copytree(FIXTURE, root / "dbt")
    sh("git", "add", "-A", cwd=root)
    sh("git", "commit", "-q", "-m", "base", cwd=root)
    sh("git", "checkout", "-q", "-b", "feature", cwd=root)
    orders = root / "dbt" / "models" / "orders.sql"
    orders.write_text(orders.read_text() + "\n-- touched\n")
    broken = root / "dbt" / "models" / "broken"
    broken.mkdir()
    for i in range(N_BROKEN):
        (broken / f"broken_{i:03}.sql").write_text(
            f"select id from {{{{ source('missing', 'tbl_{i:03}') }}}}\n"
        )
    sh("git", "add", "-A", cwd=root)
    sh("git", "commit", "-q", "-m", "feature", cwd=root)
    return sh("git", "rev-parse", "HEAD", cwd=root).stdout.strip()


def free_port(host):
    with socket.socket() as s:
        s.bind((host, 0))
        return s.getsockname()[1]


class PipeTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not docker_available():
            raise unittest.SkipTest("docker is not available")
        cls.image = build_image()
        cls.tmp = Path(tempfile.mkdtemp(prefix="dbt-lineage-pipe-", dir=os.environ.get("TEST_TMPDIR")))
        cls.repo = cls.tmp / "repo"
        cls.commit = make_repo(cls.repo)
        # Containers reach the host as host.docker.internal (host-gateway),
        # which is the default bridge's gateway: bind the mock only there.
        gw = sh(
            "docker", "network", "inspect", "bridge",
            "-f", "{{(index .IPAM.Config 0).Gateway}}",
        ).stdout.strip() or "127.0.0.1"
        cls.port = free_port(gw)
        cls.log = cls.tmp / "requests.jsonl"
        cls.mock = subprocess.Popen(
            [sys.executable, str(HERE / "mock_bitbucket.py"), f"{gw}:{cls.port}", str(cls.log)],
            stdout=subprocess.PIPE,
            text=True,
        )
        cls.mock.stdout.readline()  # wait until it listens

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, "mock"):
            cls.mock.terminate()
            cls.mock.wait()
        if hasattr(cls, "tmp"):
            shutil.rmtree(cls.tmp, ignore_errors=True)

    def run_pipe(self, **variables):
        """Run the pipe once; returns the process and the requests it made."""
        self.log.write_text("")
        env = {
            "BITBUCKET_BUILD_NUMBER": "42",
            "BITBUCKET_CLONE_DIR": CLONE_DIR,
            "BITBUCKET_WORKSPACE": "ws",
            "BITBUCKET_REPO_OWNER": "ws",
            "BITBUCKET_REPO_SLUG": "repo",
            "BITBUCKET_COMMIT": self.commit,
            "BITBUCKET_BRANCH": "feature",
            "BITBUCKET_PR_ID": "7",
            "BITBUCKET_PR_DESTINATION_BRANCH": "main",
            "PROJECT_DIR": "dbt",
            # The Pipelines proxy, as the pipe container sees it.
            "PIPELINES_PROXY_ADDR": f"host.docker.internal:{self.port}",
        }
        env.update(variables)
        cmd = [
            "docker", "run", "--rm",
            "--add-host", "host.docker.internal:host-gateway",
            "--user", f"{os.getuid()}:{os.getgid()}",
            "-v", f"{self.repo}:{CLONE_DIR}",
            "-w", CLONE_DIR,
        ]
        for k, v in env.items():
            cmd += ["-e", f"{k}={v}"]
        cmd.append(self.image)
        p = subprocess.run(cmd, text=True, capture_output=True, timeout=300)
        time.sleep(0.1)
        requests = [json.loads(l) for l in self.log.read_text().splitlines() if l]
        return p, requests

    def check_insights(self, requests, base, auth):
        insights = [r for r in requests if "/reports/" in r["url"]]
        for r in insights:
            # Through the proxy the request line carries the absolute URI.
            if "://" not in r["url"]:
                r["url"] = base + r["url"]
        report_url = f"{base}/2.0/repositories/ws/repo/commit/{self.commit}/reports/dbt-lineage"
        self.assertEqual([r["method"] for r in insights[:2]], ["DELETE", "PUT"])
        self.assertTrue(all(r["url"] == report_url for r in insights[:2]), insights[:2])
        self.assertTrue(all(r["authorization"] == auth for r in insights))
        report = insights[1]["body"]
        self.assertEqual(report["result"], "FAILED")
        self.assertEqual(report["report_type"], "BUG")
        self.assertEqual(report["external_id"], "dbt-lineage")
        self.assertEqual(report["data"][0], {"title": "Errors", "type": "NUMBER", "value": N_BROKEN})
        posts = insights[2:]
        self.assertTrue(posts)
        annotations = []
        for r in posts:
            self.assertEqual((r["method"], r["url"]), ("POST", report_url + "/annotations"))
            self.assertLessEqual(len(r["body"]), 100)
            annotations += r["body"]
        self.assertGreater(len(annotations), 100)
        self.assertEqual(len({a["external_id"] for a in annotations}), len(annotations))
        for a in annotations:
            # Relative to the repository root, and a file that exists there.
            self.assertTrue(a["path"].startswith("dbt/"), a["path"])
            self.assertTrue((self.repo / a["path"]).is_file(), a["path"])
            self.assertGreaterEqual(a["line"], 1)
        bugs = [a for a in annotations if a["annotation_type"] == "BUG"]
        self.assertEqual(len(bugs), N_BROKEN)
        self.assertTrue(all(a["severity"] == "HIGH" for a in bugs))
        self.assertTrue(all(a["path"].startswith("dbt/models/broken/") for a in bugs))
        return insights

    def test_proxy_mode_without_token(self):
        p, requests = self.run_pipe(FAIL_ON="never", DEBUG="true")
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        self.assertIn("rewrote 1 git proxy setting", p.stdout)
        self.assertIn(f"GIT_CONFIG_VALUE_0=http://host.docker.internal:{self.port}", p.stderr)
        self.assertIn("PR comment skipped", p.stdout)
        self.assertIn("dbt-lineage finished", p.stdout)
        self.assertFalse([r for r in requests if "/pullrequests/" in r["url"]])
        # Absolute-URI requests to the plain-HTTP API, auth left to the proxy.
        self.check_insights(requests, "http://api.bitbucket.org", None)
        self.assertTrue((self.repo / "dbt-lineage-report.md").is_file())
        self.assertTrue((self.repo / "dbt-lineage-insights.json").is_file())

    def test_token_mode_comment_insights_and_rerun(self):
        api = f"http://host.docker.internal:{self.port}"
        variables = {"BITBUCKET_ACCESS_TOKEN": "tok", "BITBUCKET_API_URL": api}
        p, requests = self.run_pipe(**variables)
        # Default FAIL_ON=lint-errors: the undefined sources fail the step.
        self.assertEqual(p.returncode, 1, p.stdout + p.stderr)
        self.assertIn("dbt-lineage failed", p.stdout)
        comments = [r for r in requests if "/pullrequests/7/comments" in r["path"]]
        self.assertEqual([r["method"] for r in comments], ["GET", "POST"])
        self.assertTrue(all(r["authorization"] == "Bearer tok" for r in comments))
        self.assertIn("broken_000", comments[1]["body"]["content"]["raw"])
        first = self.check_insights(requests, api, "Bearer tok")

        # Same commit again: same report id, previous report deleted first,
        # identical bodies; the sticky comment is updated, not duplicated.
        p, requests = self.run_pipe(**variables)
        again = [r for r in requests if "/reports/" in r["url"]]
        self.assertEqual(again[0]["method"], "DELETE")
        self.assertEqual([(r["method"], r["path"], r["body"]) for r in again[1:]],
                         [(r["method"], r["path"], r["body"]) for r in first[1:]])
        comments = [r for r in requests if "/pullrequests/7/comments" in r["path"]]
        self.assertEqual([r["method"] for r in comments], ["GET", "PUT"])

    def test_invalid_variable_fails_fast(self):
        p, requests = self.run_pipe(FAIL_ON="sometimes")
        self.assertEqual(p.returncode, 1)
        self.assertIn("FAIL_ON must be one of", p.stdout)
        self.assertEqual(requests, [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
