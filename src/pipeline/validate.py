"""Two-layer validation.

Layer 1 (Pandera) — DataFrame-level column checks catch shape/bulk issues
    cheaply before we spend Pydantic cycles per row.
Layer 2 (Pydantic) — record-level typed validation, the real contract.

Invalid records are *not* silently dropped; they're returned in an error
report so data-quality incidents are visible to downstream teams.
"""
from __future__ import annotations

from typing import Any, Dict, List, Tuple

import pandas as pd
import pandera.pandas as pa
from pandera.pandas import Column, DataFrameSchema, Check
from pydantic import ValidationError

from src.schema import PaymentEventV1

# Lightweight column-level sanity checks on the *raw* combined DataFrame.
_RAW_CHECKS = DataFrameSchema(
    {
        "__source_system__": Column(str, Check.isin(["cards", "transfers", "bills"])),
    },
    strict=False,
    coerce=True,
)


def pandera_precheck(df: pd.DataFrame) -> None:
    """Raise early on bulk column issues (cheap)."""
    _RAW_CHECKS.validate(df, lazy=True)


def validate_records(
    records: List[Dict[str, Any]],
) -> Tuple[List[PaymentEventV1], List[Dict[str, Any]]]:
    """Per-record Pydantic validation.

    Returns (valid_events, error_report). The error report preserves the
    offending record plus a human-readable message for triage.
    """
    valid: List[PaymentEventV1] = []
    errors: List[Dict[str, Any]] = []
    for rec in records:
        try:
            valid.append(PaymentEventV1.model_validate(rec))
        except ValidationError as exc:
            errors.append({
                "source_system": rec.get("source_system"),
                "source_reference_id": rec.get("source_reference_id"),
                "errors": exc.errors(include_url=False),
            })
    return valid, errors
