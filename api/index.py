from __future__ import annotations

import json
import sys
from http.server import BaseHTTPRequestHandler
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.market_caps import (  # noqa: E402
    ETF_SOURCE_CHECKS,
    PROVIDER_SOURCE_CHECKS,
    build_payload,
    check_source,
    compile_groups,
    fetch_market_cap_dataset,
)


class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if not self.path.startswith("/api"):
            return self.respond_with_index()
        return self.respond_with_payload()

    def do_HEAD(self):
        if not self.path.startswith("/api"):
            body = b""
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Cache-Control", "public, max-age=0, must-revalidate")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            return
        return self.respond_with_payload(head_only=True)

    def respond_with_index(self):
        body = (ROOT / "index.html").read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Cache-Control", "public, max-age=0, must-revalidate")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def respond_with_payload(self, head_only: bool = False):
        try:
            checks = [check_source(source) for source in (*ETF_SOURCE_CHECKS, *PROVIDER_SOURCE_CHECKS)]
            failed_checks = [check for check in checks if not check["ok"]]
            if failed_checks:
                raise RuntimeError("One or more ETF or provider source checks failed.")

            dataset = fetch_market_cap_dataset()
            results = compile_groups(dataset.country_caps, dataset.denominator_usd_millions, dataset.share_basis)
            payload = build_payload(checks, results, dataset)
            body = json.dumps(payload, indent=2, sort_keys=True).encode("utf-8")
            self.send_response(200)
        except Exception as exc:
            body = json.dumps({"error": str(exc)}, indent=2, sort_keys=True).encode("utf-8")
            self.send_response(502)

        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if not head_only:
            self.wfile.write(body)
