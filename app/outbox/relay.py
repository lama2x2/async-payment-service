import asyncio
import logging
from datetime import UTC, datetime
from typing import Protocol

from faststream.rabbit import RabbitBroker
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.messaging.topology import PAYMENTS_EXCHANGE
from app.models import OutboxMessage

logger = logging.getLogger(__name__)


class OutboxPublisher(Protocol):
    async def publish(self, message: OutboxMessage) -> None: ...


class RabbitOutboxPublisher:
    def __init__(self, broker: RabbitBroker) -> None:
        self._broker = broker

    async def publish(self, message: OutboxMessage) -> None:
        await self._broker.publish(
            message.payload,
            exchange=PAYMENTS_EXCHANGE,
            routing_key=message.routing_key,
            message_id=str(message.id),
            message_type=message.event_type,
            persist=True,
        )


class OutboxRelay:
    """Moves events from the outbox table to the broker.

    Rows are locked with SKIP LOCKED, so several relays can run side by side.
    Delivery is at-least-once: a crash between publish and commit re-sends the event.
    """

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        publisher: OutboxPublisher,
        batch_size: int = 100,
        poll_interval: float = 1.0,
    ) -> None:
        self._session_factory = session_factory
        self._publisher = publisher
        self._batch_size = batch_size
        self._poll_interval = poll_interval

    async def publish_pending(self) -> int:
        async with self._session_factory() as session:
            stmt = (
                select(OutboxMessage)
                .where(OutboxMessage.published_at.is_(None))
                .order_by(OutboxMessage.created_at)
                .limit(self._batch_size)
                .with_for_update(skip_locked=True)
            )
            messages = (await session.scalars(stmt)).all()

            published = 0
            for message in messages:
                try:
                    await self._publisher.publish(message)
                except Exception as exc:
                    logger.exception("Failed to publish outbox message %s", message.id)
                    message.attempts += 1
                    message.last_error = repr(exc)
                    # most likely the broker is unavailable, try the rest later
                    break
                message.published_at = datetime.now(UTC)
                published += 1

            await session.commit()
            return published

    async def run(self) -> None:
        logger.info("Outbox relay started")
        while True:
            try:
                published = await self.publish_pending()
            except Exception:
                logger.exception("Outbox relay iteration failed")
                published = 0

            if published < self._batch_size:
                await asyncio.sleep(self._poll_interval)
