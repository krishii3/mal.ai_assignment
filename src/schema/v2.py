"""Canonical Payment Event Schema v2.0.

Changes from v1.0
-----------------
Non-breaking additions (default-valued, downstream keeps working):
  * ``risk_score``          — optional fraud/risk model output
  * ``product_type``        — Islamic banking product (Murabaha/Tawarruq/Standard)
  * ``profit_rate``         — AAOIFI-aligned profit share (not interest)

Breaking change (requires explicit migration logic):
  * ``extended_attributes`` renamed to ``payment_metadata``

The migration module handles both in one pass so consumers on v2 never
see the legacy field name.
"""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, Literal, Optional
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator

from .v1 import PaymentMethod, PaymentStatus, PaymentType

SCHEMA_VERSION_V2 = "2.0"

ProductType = Literal["MURABAHA", "TAWARRUQ", "IJARA", "QARD", "STANDARD"]


class PaymentEventV2(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    payment_event_id: UUID
    source_system: str
    source_reference_id: str

    payment_type: PaymentType
    payment_method: PaymentMethod
    amount: Decimal = Field(..., gt=0, decimal_places=2)
    currency: str = Field(default="AED", min_length=3, max_length=3)
    status: PaymentStatus

    customer_id: str = Field(..., min_length=1)
    timestamp: datetime
    processed_at: datetime

    is_shariah_compliant: bool = Field(default=True)

    # v2 additions (non-breaking)
    risk_score: Optional[float] = Field(default=None, ge=0.0, le=1.0)
    product_type: Optional[ProductType] = Field(default="STANDARD")
    profit_rate: Optional[Decimal] = Field(default=None, ge=0)

    # v2 rename (breaking): extended_attributes -> payment_metadata
    payment_metadata: Dict[str, Any] = Field(default_factory=dict)

    schema_version: str = Field(default=SCHEMA_VERSION_V2)

    @field_validator("currency")
    @classmethod
    def _upper_currency(cls, v: str) -> str:
        return v.upper()
