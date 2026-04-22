"""CSV ingestion: one reader per squad.

Ingest is deliberately dumb: read raw strings and tag the source. All
normalization happens in ``transform.py`` so failures are easy to isolate.
"""
from __future__ import annotations
from pathlib import Path
from typing import Tuple
import pandas as pd

_EXPECTED = {
    "cards": {
        "txn_id", "card_last4", "merchant_name", "merchant_category",
        "amount", "currency_code", "txn_status", "txn_datetime",
        "cust_id", "pos_entry_mode", "response_code",
    },
    "transfers": {
        "ref_number", "sender_account", "receiver_account",
        "sender_name", "receiver_name", "transfer_amount", "currency",
        "value_date", "transfer_type", "status", "channel", "description",
    },
    "bills": {
        "bill_txn_id", "biller_code", "biller_name", "biller_category",
        "customer_account_ref", "bill_amount_aed", "bill_date", "due_date",
        "payment_date", "pay_status", "payment_channel",
    },
}


def _read_csv(path: Path, source: str) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"{source} CSV not found at {path}")
    df = pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[""])
    missing = _EXPECTED[source] - set(df.columns)
    if missing:
        raise ValueError(f"{source} CSV missing columns: {missing}")
    df["__source_system__"] = source
    return df


def read_cards_csv(path: Path) -> pd.DataFrame:
    return _read_csv(path, "cards")


def read_transfers_csv(path: Path) -> pd.DataFrame:
    return _read_csv(path, "transfers")


def read_bills_csv(path: Path) -> pd.DataFrame:
    return _read_csv(path, "bills")


def read_all(raw_dir: Path) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    return (
        read_cards_csv(raw_dir / "cards_squad.csv"),
        read_transfers_csv(raw_dir / "transfers_squad.csv"),
        read_bills_csv(raw_dir / "bills_squad.csv"),
    )
