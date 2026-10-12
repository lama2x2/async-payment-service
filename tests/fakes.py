import uuid

from app.models import OutboxMessage, Payment, PaymentStatus
from app.schemas import PaymentCreatedEvent, WebhookPayload


class FakeGateway:
    def __init__(self, result: PaymentStatus = PaymentStatus.SUCCEEDED) -> None:
        self.result = result
        self.calls: list[uuid.UUID] = []

    async def charge(self, payment: Payment) -> PaymentStatus:
        self.calls.append(payment.id)
        return self.result


class FakeWebhookSender:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.sent: list[tuple[str, WebhookPayload]] = []

    async def send(self, url: str, payload: WebhookPayload) -> None:
        self.sent.append((url, payload))
        if self.error is not None:
            raise self.error


class FakeOutboxPublisher:
    def __init__(self, fail_on: int | None = None) -> None:
        self.fail_on = fail_on
        self.published: list[OutboxMessage] = []

    async def publish(self, message: OutboxMessage) -> None:
        if self.fail_on is not None and len(self.published) == self.fail_on:
            raise ConnectionError("broker is down")
        self.published.append(message)


class FakeRetryScheduler:
    def __init__(self) -> None:
        self.scheduled: list[tuple[PaymentCreatedEvent, int]] = []

    async def schedule(self, event: PaymentCreatedEvent, failed_attempt: int) -> None:
        self.scheduled.append((event, failed_attempt))
