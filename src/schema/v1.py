"""Canonical Payment Event Schema v1.0.

Flat core fields + ``extended_attributes`` dict. Mirrors the Stripe/Square
pattern: core fields support fast columnar queries while type-specific
metadata lives in a JSON blob, avoiding NULL sprawl and expensive joins.
"""
from __future__ import annotations
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, Literal, Optional
from uuid import UUID
from pydantic import BaseModel, ConfigDict, Field, field_validator

SCHEMA_VERSION_V1 = "1.0"

PaymentType = Literal["CARD", "TRANSFER", "BILL_PAYMENT"]
PaymentStatus = Literal["PENDING", "COMPLETED", "FAILED", "REVERSED"]
PaymentMethod = Literal["CARD", "BANK_TRANSFER", "DIRECT_DEBIT"]


class PaymentEventV1(BaseModel):
    """Canonical payment event v1.0 — the contract downstream teams depend on."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    payment_event_id: UUID = Field(..., description="Canonical surrogate key")
    source_system: str = Field(..., description="cards | transfers | bills")
    source_reference_id: str = Field(..., description="Original squad record id")

    payment_type: PaymentType
    payment_method: PaymentMethod
    amount: Decimal = Field(..., gt=0, decimal_places=2)
    currency: str = Field(default="AED", min_length=3, max_length=3)
    status: PaymentStatus

    customer_id: str = Field(..., min_length=1)
    timestamp: datetime
    processed_at: datetime

    is_shariah_compliant: bool = Field(default=True)
    extended_attributes: Dict[str, Any] = Field(default_factory=dict)
    schema_version: str = Field(default=SCHEMA_VERSION_V1)

    @field_validator("currency")
    @classmethod
    def _upper_currency(cls, v: str) -> str:
        return v.upper()
