import logging
import uuid
from datetime import UTC, datetime

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models import Payment, PaymentStatus
from app.schemas import WebhookPayload
from app.worker.gateway import PaymentGateway
from app.worker.webhook import WebhookSender

logger = logging.getLogger(__name__)


class PaymentProcessor:
    """Charges a pending payment and notifies the client.

    Safe to run several times for the same payment: the gateway is called only
    while the payment is pending, the webhook only until it was delivered once.
    """

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        gateway: PaymentGateway,
        webhook_sender: WebhookSender,
    ) -> None:
        self._session_factory = session_factory
        self._gateway = gateway
        self._webhook_sender = webhook_sender

    async def process(self, payment_id: uuid.UUID) -> None:
        async with self._session_factory() as session:
            # the row lock makes a duplicate message wait instead of charging twice
            payment = await session.get(Payment, payment_id, with_for_update=True)
            if payment is None:
                logger.warning("Payment %s not found, nothing to process", payment_id)
                return

            if payment.status is PaymentStatus.PENDING:
                payment.status = await self._gateway.charge(payment)
                payment.processed_at = datetime.now(UTC)
                await session.commit()
                logger.info("Payment %s processed: %s", payment.id, payment.status)

            if payment.notified_at is None:
                await self._notify(session, payment)

    async def _notify(self, session: AsyncSession, payment: Payment) -> None:
        payload = WebhookPayload(
            payment_id=payment.id,
            status=payment.status,
            amount=payment.amount,
            currency=payment.currency,
            processed_at=payment.processed_at,
        )
        await self._webhook_sender.send(payment.webhook_url, payload)
        payment.notified_at = datetime.now(UTC)
        await session.commit()
        logger.info("Webhook for payment %s delivered", payment.id)
