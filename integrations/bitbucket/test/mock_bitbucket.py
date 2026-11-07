#!/usr/bin/env python3
"""Mock of the Bitbucket Cloud API parts the pipe calls, standard library only.

Serves both as the API itself (`BITBUCKET_API_URL=http://host:port`) and as
the Pipelines auth proxy (`BITBUCKET_PROXY=http://host:port`, requests arrive
with an absolute URI such as `PUT http://api.bitbucket.org/2.0/...`).

Every request is appended to the JSON-lines log given as the second argument:
{"method", "url", "path", "authorization", "body"}.

Usage: mock_bitbucket.py HOST:PORT LOG
"""

import json
import re
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit

REPORT = re.compile(r"^/2\.0/repositories/[^/]+/[^/]+/commit/[0-9a-f]+/reports/[A-Za-z0-9._-]+$")
ANNOTATIONS = re.compile(REPORT.pattern[:-1] + r"/annotations$")
COMMENTS = re.compile(r"^/2\.0/repositories/[^/]+/[^/]+/pullrequests/\d+/comments(/\d+)?$")

lock = threading.Lock()
reports = set()  # report URLs that currently exist
comments = []  # {"id", "content": {"raw"}}


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):  # quiet
        pass

    def _handle(self):
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length) if length else b""
        try:
            body = json.loads(raw) if raw else None
        except ValueError:
            body = raw.decode("utf-8", "replace")
        url = self.path
        path = urlsplit(url).path if "://" in url else url.split("?")[0]
        entry = {
            "method": self.command,
            "url": url,
            "path": path,
            "authorization": self.headers.get("Authorization"),
            "body": body,
        }
        with lock:
            with open(LOG, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry) + "\n")
            status, resp = self._route(self.command, path, body)
        data = json.dumps(resp).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _route(self, method, path, body):
        if REPORT.match(path):
            if method == "DELETE":
                if path in reports:
                    reports.discard(path)
                    return 204, {}
                return 404, {"error": {"message": "Report not found"}}
            if method == "PUT":
                reports.add(path)
                return 200, dict(body or {}, uuid="{00000000-0000-0000-0000-000000000000}")
        if ANNOTATIONS.match(path) and method == "POST":
            if path.rsplit("/annotations", 1)[0] not in reports:
                return 404, {"error": {"message": "Report not found"}}
            if not isinstance(body, list) or len(body) > 100:
                return 400, {"error": {"message": "Up to 100 annotations per request"}}
            return 200, body
        if COMMENTS.match(path):
            if method == "GET":
                return 200, {"values": comments, "pagelen": 100}
            if method == "POST":
                comments.append({"id": len(comments) + 1, "content": body["content"]})
                return 201, comments[-1]
            if method == "PUT":
                cid = int(path.rsplit("/", 1)[1])
                comments[cid - 1]["content"] = body["content"]
                return 200, comments[cid - 1]
        return 404, {"error": {"message": f"mock: no route for {method} {path}"}}

    do_GET = do_PUT = do_POST = do_DELETE = _handle


class Server(ThreadingHTTPServer):
    daemon_threads = True

    def handle_error(self, request, client_address):
        # Clients closing kept-alive connections are not errors.
        if not isinstance(sys.exc_info()[1], ConnectionError):
            super().handle_error(request, client_address)


if __name__ == "__main__":
    host, port = sys.argv[1].rsplit(":", 1)
    LOG = sys.argv[2]
    server = Server((host, int(port)), Handler)
    print(f"mock bitbucket on {host}:{server.server_address[1]}", flush=True)
    server.serve_forever()
