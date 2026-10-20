#!/usr/bin/env python3
import csv
import json
import os
import socketserver
import subprocess
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from io import StringIO
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
INDEX_PATH = PROJECT_ROOT / "dashboard" / "index.html"
HOST = "0.0.0.0"
PORT = 8000


def clean_output(stdout: str) -> str:
    cleaned_lines = []
    for line in stdout.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("Oct ") or "WARNING:" in stripped or "INFO:" in stripped:
            continue
        cleaned_lines.append(stripped)
    return "\n".join(cleaned_lines)


def run_trino(sql: str):
    result = subprocess.run(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "trino",
            "trino",
            "--catalog",
            "iceberg",
            "--execute",
            sql,
        ],
        cwd=str(PROJECT_ROOT),
        capture_output=True,
        text=True,
        check=False,
    )

    if result.returncode != 0:
        error = (result.stderr or result.stdout or "Trino query failed").strip()
        raise RuntimeError(error)

    return clean_output(result.stdout)


def parse_rows(raw_text: str):
    if not raw_text.strip():
        return []
    rows = [row for row in csv.reader(StringIO(raw_text), quotechar='"') if row and any(cell.strip() for cell in row)]
    if not rows:
        return []
    return rows


def fetch_metrics():
    try:
        schemas = parse_rows(run_trino("SHOW SCHEMAS;"))
        silver_tables = parse_rows(run_trino("SHOW TABLES FROM silver;"))
        inventory_rows = parse_rows(
            run_trino(
                "SELECT inventory_id, product_id, warehouse, quantity, last_event_id, updated_at FROM silver.inventory ORDER BY inventory_id;"
            )
        )
        revenue_rows = parse_rows(
            run_trino(
                "SELECT region, ROUND(SUM(revenue), 2) AS revenue, SUM(order_count) AS orders FROM gold.daily_revenue GROUP BY region ORDER BY revenue DESC;"
            )
        )
        quality_rows = parse_rows(
            run_trino(
                "SELECT dataset, metric, issue_count FROM gold.data_quality_summary ORDER BY dataset, metric;"
            )
        )

        return {
            "schemas": [value for row in schemas for value in row if value],
            "silver_tables": [value for row in silver_tables for value in row if value],
            "inventory": inventory_rows,
            "revenue": revenue_rows,
            "quality": quality_rows,
            "ok": True,
        }
    except Exception as exc:  # pragma: no cover - runtime guard for dashboard
        return {"ok": False, "error": str(exc)}


class DashboardHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/api/metrics":
            payload = fetch_metrics()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(payload).encode("utf-8"))
            return

        if self.path == "/":
            html = INDEX_PATH.read_text(encoding="utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(html.encode("utf-8"))))
            self.end_headers()
            self.wfile.write(html.encode("utf-8"))
            return

        self.send_response(404)
        self.end_headers()

    def log_message(self, *args):
        return


if __name__ == "__main__":
    server = ThreadingHTTPServer((HOST, PORT), DashboardHandler)
    print(f"Dashboard running at http://localhost:{PORT}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
