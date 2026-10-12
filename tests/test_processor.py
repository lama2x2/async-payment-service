import uuid

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models import Payment, PaymentStatus
from app.worker.processor import PaymentProcessor
from app.worker.webhook import WebhookDeliveryError
from tests.factories import create_payment
from tests.fakes import FakeGateway, FakeWebhookSender


async def reload(
    session_factory: async_sessionmaker[AsyncSession], payment_id: uuid.UUID
) -> Payment:
    async with session_factory() as session:
        payment = await session.get(Payment, payment_id)
        assert payment is not None
        return payment


@pytest.mark.parametrize("result", [PaymentStatus.SUCCEEDED, PaymentStatus.FAILED])
async def test_processes_pending_payment(
    session: AsyncSession,
    session_factory: async_sessionmaker[AsyncSession],
    result: PaymentStatus,
) -> None:
    payment = await create_payment(session)
    webhooks = FakeWebhookSender()

    await PaymentProcessor(session_factory, FakeGateway(result), webhooks).process(payment.id)

    stored = await reload(session_factory, payment.id)
    assert stored.status is result
    assert stored.processed_at is not None
    assert stored.notified_at is not None

    [(url, payload)] = webhooks.sent
    assert url == payment.webhook_url
    assert payload.payment_id == payment.id
    assert payload.status is result
    assert payload.amount == payment.amount


async def test_webhook_failure_is_raised_and_status_is_kept(
    session: AsyncSession, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    payment = await create_payment(session)
    processor = PaymentProcessor(
        session_factory, FakeGateway(), FakeWebhookSender(WebhookDeliveryError("timeout"))
    )

    with pytest.raises(WebhookDeliveryError):
        await processor.process(payment.id)

    stored = await reload(session_factory, payment.id)
    assert stored.status is PaymentStatus.SUCCEEDED
    assert stored.notified_at is None


async def test_retry_after_webhook_failure_does_not_charge_again(
    session: AsyncSession, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    payment = await create_payment(session)
    gateway = FakeGateway()
    with pytest.raises(WebhookDeliveryError):
        await PaymentProcessor(
            session_factory, gateway, FakeWebhookSender(WebhookDeliveryError("timeout"))
        ).process(payment.id)

    webhooks = FakeWebhookSender()
    await PaymentProcessor(session_factory, gateway, webhooks).process(payment.id)

    assert len(gateway.calls) == 1
    assert len(webhooks.sent) == 1
    assert (await reload(session_factory, payment.id)).notified_at is not None


async def test_already_notified_payment_is_skipped(
    session: AsyncSession, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    payment = await create_payment(session)
    gateway = FakeGateway()
    webhooks = FakeWebhookSender()
    processor = PaymentProcessor(session_factory, gateway, webhooks)

    await processor.process(payment.id)
    await processor.process(payment.id)

    assert len(gateway.calls) == 1
    assert len(webhooks.sent) == 1


async def test_unknown_payment_is_ignored(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    gateway = FakeGateway()
    webhooks = FakeWebhookSender()

    await PaymentProcessor(session_factory, gateway, webhooks).process(uuid.uuid4())

    assert gateway.calls == []
    assert webhooks.sent == []
