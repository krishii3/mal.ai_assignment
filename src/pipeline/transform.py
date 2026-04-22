"""Transform squad-specific DataFrames into canonical v1 records.

Each transformer maps squad column names to canonical field names, normalizes
status strings, pushes type-specific detail into ``extended_attributes``, and
generates deterministic UUIDs so reruns are idempotent.
"""
from __future__ import annotations
import uuid
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional
import pandas as pd

_CARD_STATUS = {"APPROVED": "COMPLETED", "DECLINED": "FAILED", "REVERSED": "REVERSED"}
_TRANSFER_STATUS = {"SUCCESS": "COMPLETED", "PENDING": "PENDING", "FAILED": "FAILED"}
_BILL_STATUS = {"PAID": "COMPLETED", "FAILED": "FAILED", "PENDING": "PENDING"}

_NAMESPACE = uuid.UUID("00000000-0000-0000-0000-000000000042")


def _deterministic_uuid(source: str, ref: str) -> uuid.UUID:
    return uuid.uuid5(_NAMESPACE, f"{source}:{ref}")


def _to_dict_records(df: pd.DataFrame) -> List[Dict[str, Any]]:
    return df.where(pd.notna(df), None).to_dict(orient="records")


def _to_decimal(value: Any) -> Optional[Decimal]:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def transform_cards(df: pd.DataFrame, processed_at: datetime) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in _to_dict_records(df):
        out.append({
            "payment_event_id": _deterministic_uuid("cards", row["txn_id"]),
            "source_system": "cards",
            "source_reference_id": row["txn_id"],
            "payment_type": "CARD",
            "payment_method": "CARD",
            "amount": _to_decimal(row.get("amount")),
            "currency": row.get("currency_code") or "AED",
            "status": _CARD_STATUS.get((row.get("txn_status") or "").upper(), "PENDING"),
            "customer_id": row.get("cust_id"),
            "timestamp": row.get("txn_datetime"),
            "processed_at": processed_at,
            "is_shariah_compliant": True,
            "extended_attributes": {
                "card_last4": row.get("card_last4"),
                "merchant_name": row.get("merchant_name"),
                "merchant_category": row.get("merchant_category"),
                "pos_entry_mode": row.get("pos_entry_mode"),
                "response_code": row.get("response_code"),
            },
            "schema_version": "1.0",
        })
    return out


def transform_transfers(df: pd.DataFrame, processed_at: datetime) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in _to_dict_records(df):
        out.append({
            "payment_event_id": _deterministic_uuid("transfers", row["ref_number"]),
            "source_system": "transfers",
            "source_reference_id": row["ref_number"],
            "payment_type": "TRANSFER",
            "payment_method": "BANK_TRANSFER",
            "amount": _to_decimal(row.get("transfer_amount")),
            "currency": row.get("currency") or "AED",
            "status": _TRANSFER_STATUS.get((row.get("status") or "").upper(), "PENDING"),
            "customer_id": row.get("sender_account"),
            "timestamp": row.get("value_date"),
            "processed_at": processed_at,
            "is_shariah_compliant": True,
            "extended_attributes": {
                "sender_name": row.get("sender_name"),
                "receiver_name": row.get("receiver_name"),
                "receiver_account": row.get("receiver_account"),
                "transfer_type": row.get("transfer_type"),
                "channel": row.get("channel"),
                "description": row.get("description"),
            },
            "schema_version": "1.0",
        })
    return out


def transform_bills(df: pd.DataFrame, processed_at: datetime) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in _to_dict_records(df):
        out.append({
            "payment_event_id": _deterministic_uuid("bills", row["bill_txn_id"]),
            "source_system": "bills",
            "source_reference_id": row["bill_txn_id"],
            "payment_type": "BILL_PAYMENT",
            "payment_method": "DIRECT_DEBIT",
            "amount": _to_decimal(row.get("bill_amount_aed")),
            "currency": "AED",
            "status": _BILL_STATUS.get((row.get("pay_status") or "").upper(), "PENDING"),
            "customer_id": row.get("customer_account_ref"),
            "timestamp": row.get("payment_date"),
            "processed_at": processed_at,
            "is_shariah_compliant": True,
            "extended_attributes": {
                "biller_code": row.get("biller_code"),
                "biller_name": row.get("biller_name"),
                "biller_category": row.get("biller_category"),
                "bill_date": row.get("bill_date"),
                "due_date": row.get("due_date"),
                "payment_channel": row.get("payment_channel"),
            },
            "schema_version": "1.0",
        })
    return out
