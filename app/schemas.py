import uuid
from datetime import datetime
from decimal import Decimal
from typing import Annotated, Any

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, HttpUrl

from app.models import Currency, PaymentStatus

Amount = Annotated[Decimal, Field(gt=0, max_digits=18, decimal_places=2)]


class PaymentCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    amount: Amount
    currency: Currency
    description: str = Field(max_length=1000)
    metadata: dict[str, Any] = Field(default_factory=dict)
    webhook_url: HttpUrl


class PaymentAccepted(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    payment_id: uuid.UUID = Field(validation_alias=AliasChoices("payment_id", "id"))
    status: PaymentStatus
    created_at: datetime


class PaymentRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any] = Field(validation_alias=AliasChoices("metadata", "meta"))
    status: PaymentStatus
    idempotency_key: str
    webhook_url: str
    created_at: datetime
    processed_at: datetime | None
    notified_at: datetime | None


class PaymentCreatedEvent(BaseModel):
    payment_id: uuid.UUID


class WebhookPayload(BaseModel):
    payment_id: uuid.UUID
    status: PaymentStatus
    amount: Decimal
    currency: Currency
    processed_at: datetime | None
