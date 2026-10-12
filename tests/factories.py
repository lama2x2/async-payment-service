import uuid
from decimal import Decimal

from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Currency, OutboxMessage, Payment, PaymentStatus


async def create_payment(
    session: AsyncSession, status: PaymentStatus = PaymentStatus.PENDING
) -> Payment:
    payment = Payment(
        id=uuid.uuid4(),
        amount=Decimal("99.90"),
        currency=Currency.USD,
        description="test payment",
        meta={},
        status=status,
        idempotency_key=str(uuid.uuid4()),
        request_hash="0" * 64,
        webhook_url="https://client.example.com/hook",
    )
    session.add(payment)
    await session.commit()
    return payment


async def create_outbox_messages(session: AsyncSession, count: int) -> list[OutboxMessage]:
    messages = [
        OutboxMessage(
            event_type="payment.created",
            routing_key="payments.new",
            payload={"payment_id": str(uuid.uuid4())},
        )
        for _ in range(count)
    ]
    session.add_all(messages)
    await session.commit()
    return messages
