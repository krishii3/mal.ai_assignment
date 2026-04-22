"""Unified Payment Data Pipeline — entrypoint.

Run:
    python -m src.main

Outputs (under data/output/):
    unified_payments_v1.parquet   — canonical v1 events
    unified_payments_v2.parquet   — migrated v2 events
    validation_errors.json        — rejected records + reasons
    pipeline_summary.json         — run metadata
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from src.migration.v1_to_v2 import migrate_batch
from src.pipeline.ingest import read_all
from src.pipeline.transform import transform_bills, transform_cards, transform_transfers
from src.pipeline.validate import pandera_precheck, validate_records

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"
OUT = ROOT / "data" / "output"


def _events_to_frame(events) -> pd.DataFrame:
    rows = [e.model_dump(mode="json") for e in events]
    return pd.DataFrame(rows)


def run() -> dict:
    OUT.mkdir(parents=True, exist_ok=True)
    processed_at = datetime.now(timezone.utc)

    cards_df, transfers_df, bills_df = read_all(RAW)
    combined_raw = pd.concat([cards_df, transfers_df, bills_df], ignore_index=True)
    pandera_precheck(combined_raw)

    records = (
        transform_cards(cards_df, processed_at)
        + transform_transfers(transfers_df, processed_at)
        + transform_bills(bills_df, processed_at)
    )

    valid_v1, errors = validate_records(records)
    v1_df = _events_to_frame(valid_v1)
    v1_df.to_parquet(OUT / "unified_payments_v1.parquet", index=False)

    valid_v2 = migrate_batch(valid_v1)
    v2_df = _events_to_frame(valid_v2)
    v2_df.to_parquet(OUT / "unified_payments_v2.parquet", index=False)

    (OUT / "validation_errors.json").write_text(json.dumps(errors, indent=2, default=str))

    summary = {
        "processed_at": processed_at.isoformat(),
        "input_counts": {
            "cards": len(cards_df),
            "transfers": len(transfers_df),
            "bills": len(bills_df),
        },
        "valid_v1": len(valid_v1),
        "valid_v2": len(valid_v2),
        "errors": len(errors),
        "outputs": {
            "v1_parquet": str(OUT / "unified_payments_v1.parquet"),
            "v2_parquet": str(OUT / "unified_payments_v2.parquet"),
            "error_report": str(OUT / "validation_errors.json"),
        },
    }
    (OUT / "pipeline_summary.json").write_text(json.dumps(summary, indent=2))
    return summary


if __name__ == "__main__":
    summary = run()
    print(json.dumps(summary, indent=2))
