import asyncio
import random
from typing import Protocol

from app.models import Payment, PaymentStatus


class PaymentGateway(Protocol):
    async def charge(self, payment: Payment) -> PaymentStatus: ...


class FakePaymentGateway:
    """Stand-in for an external payment provider: random delay and outcome."""

    def __init__(
        self,
        min_delay: float = 2.0,
        max_delay: float = 5.0,
        success_rate: float = 0.9,
        rng: random.Random | None = None,
    ) -> None:
        self._min_delay = min_delay
        self._max_delay = max_delay
        self._success_rate = success_rate
        self._rng = rng or random.Random()

    async def charge(self, payment: Payment) -> PaymentStatus:
        await asyncio.sleep(self._rng.uniform(self._min_delay, self._max_delay))
        if self._rng.random() < self._success_rate:
            return PaymentStatus.SUCCEEDED
        return PaymentStatus.FAILED
