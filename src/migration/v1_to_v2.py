"""Data-contract migration v1.0 → v2.0.

Two classes of change are demonstrated in one function, matching what
a real versioning policy looks like:

1. Non-breaking additions: ``risk_score``, ``product_type``, ``profit_rate``
   get sensible defaults so a v1 producer + v2 consumer just works.

2. Breaking rename: ``extended_attributes`` → ``payment_metadata``.
   Downstream consumers upgrade to v2 *before* producers start writing
   v2; the migrator bridges the gap during the transition window.

In production this function would be wrapped behind a feature flag and
the two versions would coexist for ~1 release cycle.
"""
from __future__ import annotations

from decimal import Decimal
from typing import List

from src.schema import PaymentEventV1, PaymentEventV2, SCHEMA_VERSION_V2

# Product-type defaults — in a real system these would come from a
# reference table keyed on merchant_category / transfer_type. Here we
# encode the policy inline to keep the example self-contained.
_PRODUCT_BY_PAYMENT_TYPE = {
    "CARD": "STANDARD",
    "TRANSFER": "STANDARD",
    "BILL_PAYMENT": "STANDARD",
}


def migrate_one(event: PaymentEventV1) -> PaymentEventV2:
    """Transform a single v1 event into a v2 event."""
    return PaymentEventV2(
        payment_event_id=event.payment_event_id,
        source_system=event.source_system,
        source_reference_id=event.source_reference_id,
        payment_type=event.payment_type,
        payment_method=event.payment_method,
        amount=event.amount,
        currency=event.currency,
        status=event.status,
        customer_id=event.customer_id,
        timestamp=event.timestamp,
        processed_at=event.processed_at,
        is_shariah_compliant=event.is_shariah_compliant,
        # v2 non-breaking additions (defaults for historical records)
        risk_score=None,
        product_type=_PRODUCT_BY_PAYMENT_TYPE.get(event.payment_type, "STANDARD"),
        profit_rate=Decimal("0.00"),
        # v2 breaking rename: extended_attributes -> payment_metadata
        payment_metadata=event.extended_attributes,
        schema_version=SCHEMA_VERSION_V2,
    )


def migrate_batch(events: List[PaymentEventV1]) -> List[PaymentEventV2]:
    return [migrate_one(e) for e in events]
