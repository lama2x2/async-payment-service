import hashlib
import json
import uuid

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.messaging.topology import PAYMENTS_NEW_ROUTING_KEY
from app.models import OutboxMessage, Payment
from app.schemas import PaymentCreate, PaymentCreatedEvent

PAYMENT_CREATED_EVENT = "payment.created"


class IdempotencyConflictError(Exception):
    """Idempotency key was already used with a different request body."""


def request_fingerprint(data: PaymentCreate) -> str:
    body = json.dumps(data.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(body.encode()).hexdigest()


async def get_payment(session: AsyncSession, payment_id: uuid.UUID) -> Payment | None:
    return await session.get(Payment, payment_id)


async def get_by_idempotency_key(session: AsyncSession, key: str) -> Payment | None:
    result = await session.execute(select(Payment).where(Payment.idempotency_key == key))
    return result.scalar_one_or_none()


def _ensure_same_request(payment: Payment, fingerprint: str) -> Payment:
    if payment.request_hash != fingerprint:
        raise IdempotencyConflictError
    return payment


async def create_payment(
    session: AsyncSession, data: PaymentCreate, idempotency_key: str
) -> Payment:
    """Create a payment and its outbox event in one transaction.

    Repeated calls with the same key and body return the existing payment.
    """
    fingerprint = request_fingerprint(data)

    existing = await get_by_idempotency_key(session, idempotency_key)
    if existing is not None:
        return _ensure_same_request(existing, fingerprint)

    payment = Payment(
        id=uuid.uuid4(),
        amount=data.amount,
        currency=data.currency,
        description=data.description,
        meta=data.metadata,
        idempotency_key=idempotency_key,
        request_hash=fingerprint,
        webhook_url=str(data.webhook_url),
    )
    event = PaymentCreatedEvent(payment_id=payment.id)
    session.add(payment)
    session.add(
        OutboxMessage(
            event_type=PAYMENT_CREATED_EVENT,
            routing_key=PAYMENTS_NEW_ROUTING_KEY,
            payload=event.model_dump(mode="json"),
        )
    )

    try:
        await session.commit()
    except IntegrityError:
        # a concurrent request with the same key won the race
        await session.rollback()
        existing = await get_by_idempotency_key(session, idempotency_key)
        if existing is None:
            raise
        return _ensure_same_request(existing, fingerprint)

    await session.refresh(payment)
    return payment
