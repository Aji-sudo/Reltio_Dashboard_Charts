#!/usr/bin/env python3
"""
fetch_run_summaries.py
----------------------
Fetches run/audit information directly from the BigQuery audit table and
writes a dashboard-friendly JSON file for GitHub Pages.

The production ingestion script defines the audit table as:
    <PROJECT_ID>.<BQ_AUDIT_DATASET>.<BQ_AUDIT_TABLE>

Defaults:
    BQ_AUDIT_DATASET = prime_test
    BQ_AUDIT_TABLE   = audit_log

Authentication:
    GCP_SERVICE_ACCOUNT_JSON  - complete service-account JSON stored as a
                                GitHub encrypted secret.
    GCP_PROJECT_ID            - optional; otherwise project_id from the JSON.

No service-account JSON is committed to the repository.
"""

import json
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from google.cloud import bigquery
from google.oauth2 import service_account

OUTPUT_PATH = "data/run-summaries.json"

BQ_AUDIT_DATASET = os.getenv("BQ_AUDIT_DATASET", "prime_test")
BQ_AUDIT_TABLE = os.getenv("BQ_AUDIT_TABLE", "audit_log")
MAX_RUNS = max(int(os.getenv("MAX_RUNS", "20")), 1)


def get_bq_client() -> bigquery.Client:
    """Create a BigQuery client from JSON supplied through an environment variable."""
    raw_json = os.getenv("GCP_SERVICE_ACCOUNT_JSON", "").strip()

    if raw_json:
        try:
            info = json.loads(raw_json)
        except json.JSONDecodeError as exc:
            raise ValueError(
                "GCP_SERVICE_ACCOUNT_JSON is not valid JSON"
            ) from exc

        credentials = service_account.Credentials.from_service_account_info(info)
        project_id = os.getenv("GCP_PROJECT_ID") or info.get("project_id")

        if not project_id:
            raise ValueError(
                "GCP_PROJECT_ID is not set and project_id was not present "
                "in GCP_SERVICE_ACCOUNT_JSON"
            )

        return bigquery.Client(project=project_id, credentials=credentials)

    # Optional local-development fallback.
    credentials_file = (
        os.getenv("GCP_SERVICE_ACCOUNT_FILE")
        or os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    )

    if credentials_file:
        credentials = service_account.Credentials.from_service_account_file(
            credentials_file
        )
        project_id = os.getenv("GCP_PROJECT_ID") or credentials.project_id
        return bigquery.Client(project=project_id, credentials=credentials)

    raise ValueError(
        "No BigQuery credentials supplied. Set GCP_SERVICE_ACCOUNT_JSON "
        "(recommended) or GCP_SERVICE_ACCOUNT_FILE."
    )


def fetch_audit_rows(client: bigquery.Client) -> List[Dict[str, Any]]:
    """
    Read audit rows.

    These columns match the write_audit_event() schema in the production
    ingestion script:
      audit_id, run_id, batch_index, event, timestamp, api_url,
      entity_count, response_status, response_body, accepted_count,
      rejected_count, error, detail
    """
    table_ref = f"`{client.project}.{BQ_AUDIT_DATASET}.{BQ_AUDIT_TABLE}`"

    query = f"""
    SELECT
      audit_id,
      run_id,
      batch_index,
      event,
      timestamp,
      api_url,
      entity_count,
      response_status,
      response_body,
      accepted_count,
      rejected_count,
      error,
      detail
    FROM {table_ref}
    WHERE run_id IS NOT NULL
      AND TRIM(CAST(run_id AS STRING)) <> ''
    ORDER BY timestamp DESC
    """

    rows = client.query(query).result()
    return [dict(row) for row in rows]


def _as_int(value: Any) -> int:
    try:
        return int(value) if value is not None else 0
    except (TypeError, ValueError):
        return 0


def _timestamp_string(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat()
    return str(value)


def _attribute(value: Any) -> List[Dict[str, Any]]:
    return [{"value": None if value is None else str(value)}]


def _extract_merged_count(rows: List[Dict[str, Any]]) -> int:
    """
    The audit table has no dedicated merged_count column.
    The ingestion script includes 'merged=<n>' in GBQ sync detail text,
    so this is a best-effort extraction for dashboard display.
    """
    merged = 0

    for row in rows:
        if row.get("event") not in {
            "gbq_sync_verify_complete",
            "gbq_sync_verify_incomplete",
        }:
            continue

        detail = str(row.get("detail") or "")
        marker = "merged="

        if marker not in detail:
            continue

        try:
            value = detail.split(marker, 1)[1].split(";", 1)[0].split()[0]
            merged = max(merged, int(value))
        except (ValueError, IndexError):
            pass

    return merged


def build_run_summary(
    run_id: str,
    rows: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Aggregate audit events for one run into RunSummary-style JSON."""

    rows = sorted(
        rows,
        key=lambda r: (
            str(r.get("timestamp") or ""),
            str(r.get("audit_id") or ""),
        ),
    )

    request_rows = [
        r for r in rows
        if r.get("event") == "request_sent"
        and _as_int(r.get("batch_index")) > 0
    ]

    response_rows = [
        r for r in rows
        if r.get("event") == "response_received"
        and _as_int(r.get("batch_index")) > 0
    ]

    failed_request_rows = [
        r for r in rows
        if r.get("event") == "request_failed"
        and _as_int(r.get("batch_index")) > 0
    ]

    total_submitted = sum(_as_int(r.get("entity_count")) for r in request_rows)
    total_accepted = sum(_as_int(r.get("accepted_count")) for r in response_rows)
    total_rejected = sum(_as_int(r.get("rejected_count")) for r in response_rows)

    # A failed batch has no normal response_received row, so count its
    # submitted entities as rejected for dashboard purposes.
    total_rejected += sum(
        _as_int(r.get("entity_count")) for r in failed_request_rows
    )

    batches_sent = len({
        _as_int(r.get("batch_index")) for r in request_rows
    })

    if batches_sent == 0:
        status = "no_data"
    elif total_rejected == 0 and not failed_request_rows:
        status = "success"
    else:
        status = "partial_success"

    gbq_rows = [
        r for r in rows
        if r.get("event") in {
            "gbq_sync_verify_complete",
            "gbq_sync_verify_incomplete",
        }
    ]

    gbq_expected = sum(_as_int(r.get("entity_count")) for r in gbq_rows)
    gbq_found = sum(_as_int(r.get("accepted_count")) for r in gbq_rows)
    gbq_missing = sum(_as_int(r.get("rejected_count")) for r in gbq_rows)
    gbq_merged = _extract_merged_count(gbq_rows)

    if any(
        r.get("event") == "gbq_sync_verify_incomplete"
        for r in gbq_rows
    ):
        gbq_status = "incomplete"
    elif gbq_rows:
        gbq_status = "success"
    else:
        gbq_status = "skipped"

    timestamps = [
        r.get("timestamp")
        for r in rows
        if r.get("timestamp") is not None
    ]

    started_at = _timestamp_string(min(timestamps)) if timestamps else None
    completed_at = _timestamp_string(max(timestamps)) if timestamps else None

    # Keep the detailed audit events available for future dashboard views.
    audit_events = [
        {
            "audit_id": r.get("audit_id"),
            "batch_index": r.get("batch_index"),
            "event": r.get("event"),
            "timestamp": _timestamp_string(r.get("timestamp")),
            "response_status": r.get("response_status"),
            "entity_count": r.get("entity_count"),
            "accepted_count": r.get("accepted_count"),
            "rejected_count": r.get("rejected_count"),
            "error": r.get("error"),
            "detail": r.get("detail"),
        }
        for r in rows
    ]

    attributes = {
        "RUN_ID": _attribute(run_id),
        "STATUS": _attribute(status),
        "TOTAL_SUBMITTED": _attribute(total_submitted),
        "TOTAL_ACCEPTED": _attribute(total_accepted),
        "TOTAL_REJECTED": _attribute(total_rejected),
        "BATCHES_SENT": _attribute(batches_sent),
        "ACCEPTED_ENTITY_COUNT": _attribute(total_accepted),
        "STARTED_AT": _attribute(started_at),
        "COMPLETED_AT": _attribute(completed_at),
        "GBQ_SYNC": [{
            "value": {
                "STATUS": _attribute(gbq_status),
                "EXPECTED_COUNT": _attribute(gbq_expected),
                "FOUND_COUNT": _attribute(gbq_found),
                "MISSING_COUNT": _attribute(gbq_missing),
                "MERGED_COUNT": _attribute(gbq_merged),
            }
        }],
        "AUDIT_EVENT_COUNT": _attribute(len(rows)),
    }

    return {
        "type": "configuration/entityTypes/RunSummary",
        "attributes": attributes,
        "run_id": run_id,
        "audit_events": audit_events,
    }


def main() -> None:
    client = get_bq_client()

    print(
        f"Reading BigQuery audit table: "
        f"{client.project}.{BQ_AUDIT_DATASET}.{BQ_AUDIT_TABLE}"
    )

    rows = fetch_audit_rows(client)

    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row["run_id"])].append(row)

    run_order = sorted(
        grouped.keys(),
        key=lambda rid: max(
            str(r.get("timestamp") or "")
            for r in grouped[rid]
        ),
        reverse=True,
    )[:MAX_RUNS]

    summaries = [
        build_run_summary(run_id, grouped[run_id])
        for run_id in run_order
    ]

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

    with open(OUTPUT_PATH, "w", encoding="utf-8") as output:
        json.dump(summaries, output, indent=2, default=str)

    print(
        f"Wrote {len(summaries)} run summaries from {len(rows)} "
        f"audit rows to {OUTPUT_PATH}"
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Error: {type(exc).__name__}: {exc}", file=sys.stderr)
        sys.exit(1)
