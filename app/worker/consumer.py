import logging
from typing import Protocol

from faststream import AckPolicy
from faststream.rabbit import RabbitBroker, RabbitMessage

from app.messaging.topology import PAYMENTS_EXCHANGE, PAYMENTS_NEW_QUEUE, retry_queue_name
from app.schemas import PaymentCreatedEvent
from app.worker.processor import PaymentProcessor

logger = logging.getLogger(__name__)

ATTEMPT_HEADER = "x-attempt"


class RetryScheduler(Protocol):
    async def schedule(self, event: PaymentCreatedEvent, failed_attempt: int) -> None: ...


class RabbitRetryScheduler:
    def __init__(self, broker: RabbitBroker) -> None:
        self._broker = broker

    async def schedule(self, event: PaymentCreatedEvent, failed_attempt: int) -> None:
        await self._broker.publish(
            event.model_dump(mode="json"),
            queue=retry_queue_name(failed_attempt),
            headers={ATTEMPT_HEADER: failed_attempt + 1},
            persist=True,
        )


class PaymentEventHandler:
    """Runs the processor and decides what to do with a failed delivery.

    Attempts before the last one are parked in a retry queue; a failure on the
    last attempt is re-raised so the message gets rejected into the DLQ.
    """

    def __init__(
        self,
        processor: PaymentProcessor,
        retry_scheduler: RetryScheduler,
        max_attempts: int,
    ) -> None:
        self._processor = processor
        self._retry_scheduler = retry_scheduler
        self._max_attempts = max_attempts

    async def handle(self, event: PaymentCreatedEvent, attempt: int) -> None:
        try:
            await self._processor.process(event.payment_id)
        except Exception:
            if attempt >= self._max_attempts:
                # the traceback itself is logged by FastStream on reject
                logger.error(
                    "Payment %s failed on attempt %s/%s, sending to DLQ",
                    event.payment_id, attempt, self._max_attempts,
                )
                raise
            logger.warning(
                "Payment %s failed on attempt %s/%s, scheduling retry",
                event.payment_id, attempt, self._max_attempts, exc_info=True,
            )
            await self._retry_scheduler.schedule(event, attempt)


def register_subscriber(broker: RabbitBroker, handler: PaymentEventHandler) -> None:
    @broker.subscriber(
        PAYMENTS_NEW_QUEUE,
        PAYMENTS_EXCHANGE,
        ack_policy=AckPolicy.REJECT_ON_ERROR,
    )
    async def on_payment_created(event: PaymentCreatedEvent, message: RabbitMessage) -> None:
        attempt = int(message.headers.get(ATTEMPT_HEADER, 1))
        await handler.handle(event, attempt)
