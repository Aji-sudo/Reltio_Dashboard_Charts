#!/usr/bin/env python3
"""
fetch_run_summaries.py
-----------------------
Runs inside a GitHub Actions workflow (server-to-server, no browser
involved, so Reltio's CORS restriction on auth.reltio.com never applies
here). Mints a Reltio OAuth token using client_id/client_secret from
GitHub encrypted secrets, queries the RunSummary entities, and writes the
result to a static JSON file that GitHub Pages will serve alongside
index.html.

Reads configuration from environment variables (set as GitHub Actions
"env" in the workflow, sourced from repo secrets):
    RELTIO_ENVIRONMENT
    RELTIO_TENANT_ID
    RELTIO_CLIENT_ID
    RELTIO_CLIENT_SECRET
    RELTIO_MAX_RUNS   (optional, defaults to 20)

Writes: data/run-summaries.json (relative to repo root)
"""

import os
import sys
import json
import base64
import urllib.request
import urllib.error

RELTIO_AUTH_URL = "https://auth.reltio.com/oauth/token"
ENTITY_TYPE = "configuration/entityTypes/RunSummary"
X_AXIS_ATTRIBUTE = "RUN_ID"
OUTPUT_PATH = "data/run-summaries.json"


def _post(url, headers, data_bytes):
    req = urllib.request.Request(url, data=data_bytes, headers=headers, method="POST")
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _get(url, headers):
    req = urllib.request.Request(url, headers=headers, method="GET")
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def get_access_token(client_id: str, client_secret: str) -> str:
    credentials = f"{client_id}:{client_secret}".encode("utf-8")
    basic_auth = base64.b64encode(credentials).decode("ascii")

    headers = {
        "Authorization": f"Basic {basic_auth}",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    body = b"grant_type=client_credentials"

    token_response = _post(RELTIO_AUTH_URL, headers, body)
    return token_response["access_token"]


def fetch_run_summaries(environment: str, tenant_id: str, token: str, max_runs: int):
    url = (
        f"https://{environment}.reltio.com/reltio/api/{tenant_id}"
        f"/entities/_search?"
        f"filter=equals(type,'{ENTITY_TYPE}')"
        f"&sort=attributes.{X_AXIS_ATTRIBUTE}"
        f"&order=desc"
        f"&max={max_runs}"
    )
    headers = {"Authorization": f"Bearer {token}"}
    return _get(url, headers)


def main():
    environment = os.environ["RELTIO_ENVIRONMENT"]
    tenant_id = os.environ["RELTIO_TENANT_ID"]
    client_id = os.environ["RELTIO_CLIENT_ID"]
    client_secret = os.environ["RELTIO_CLIENT_SECRET"]
    max_runs = int(os.environ.get("RELTIO_MAX_RUNS", "20"))

    try:
        token = get_access_token(client_id, client_secret)
        entities = fetch_run_summaries(environment, tenant_id, token, max_runs)
    except urllib.error.HTTPError as exc:
        print(f"HTTP error: {exc.code} {exc.reason}", file=sys.stderr)
        print(exc.read().decode("utf-8", errors="replace"), file=sys.stderr)
        sys.exit(1)
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(entities, f, indent=2)

    print(f"Wrote {len(entities) if isinstance(entities, list) else '?'} entities to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
