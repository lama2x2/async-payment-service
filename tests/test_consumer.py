import uuid

import pytest

from app.messaging.topology import retry_delay
from app.schemas import PaymentCreatedEvent
from app.worker.consumer import PaymentEventHandler
from tests.fakes import FakeRetryScheduler


class FlakyProcessor:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.calls = 0

    async def process(self, payment_id: uuid.UUID) -> None:
        self.calls += 1
        if self.error is not None:
            raise self.error


EVENT = PaymentCreatedEvent(payment_id=uuid.uuid4())


async def test_success_does_not_schedule_retry() -> None:
    scheduler = FakeRetryScheduler()
    handler = PaymentEventHandler(FlakyProcessor(), scheduler, max_attempts=3)

    await handler.handle(EVENT, attempt=1)

    assert scheduler.scheduled == []


@pytest.mark.parametrize("attempt", [1, 2])
async def test_failure_before_last_attempt_is_retried(attempt: int) -> None:
    scheduler = FakeRetryScheduler()
    handler = PaymentEventHandler(
        FlakyProcessor(RuntimeError("webhook down")), scheduler, max_attempts=3
    )

    await handler.handle(EVENT, attempt=attempt)

    assert scheduler.scheduled == [(EVENT, attempt)]


async def test_failure_on_last_attempt_is_raised_for_dlq() -> None:
    scheduler = FakeRetryScheduler()
    handler = PaymentEventHandler(
        FlakyProcessor(RuntimeError("webhook down")), scheduler, max_attempts=3
    )

    with pytest.raises(RuntimeError):
        await handler.handle(EVENT, attempt=3)

    assert scheduler.scheduled == []


def test_retry_delay_grows_exponentially() -> None:
    assert [retry_delay(attempt, base_delay=2.0) for attempt in (1, 2, 3)] == [2.0, 4.0, 8.0]
