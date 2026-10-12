"""RabbitMQ objects used by the service.

payments (direct) --payments.new--> payments.new --(reject)--> payments.dlx --> payments.new.dlq

Failed deliveries are parked in payments.new.retry.N queues. Each of them has
its own TTL (exponential backoff) and dead-letters back into payments.new.
"""

from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue

PAYMENTS_EXCHANGE_NAME = "payments"
DEAD_LETTER_EXCHANGE_NAME = "payments.dlx"

PAYMENTS_NEW_ROUTING_KEY = "payments.new"
DEAD_LETTER_ROUTING_KEY = "payments.new.dlq"

PAYMENTS_EXCHANGE = RabbitExchange(PAYMENTS_EXCHANGE_NAME, type=ExchangeType.DIRECT)
DEAD_LETTER_EXCHANGE = RabbitExchange(DEAD_LETTER_EXCHANGE_NAME, type=ExchangeType.DIRECT)

PAYMENTS_NEW_QUEUE = RabbitQueue(
    "payments.new",
    durable=True,
    routing_key=PAYMENTS_NEW_ROUTING_KEY,
    arguments={
        "x-dead-letter-exchange": DEAD_LETTER_EXCHANGE_NAME,
        "x-dead-letter-routing-key": DEAD_LETTER_ROUTING_KEY,
    },
)

DEAD_LETTER_QUEUE = RabbitQueue(
    "payments.new.dlq",
    durable=True,
    routing_key=DEAD_LETTER_ROUTING_KEY,
)


def retry_delay(attempt: int, base_delay: float) -> float:
    """Delay in seconds before the delivery that follows the given attempt."""
    return base_delay * 2 ** (attempt - 1)


def retry_queue_name(attempt: int) -> str:
    return f"payments.new.retry.{attempt}"


def retry_queue(attempt: int, base_delay: float) -> RabbitQueue:
    return RabbitQueue(
        retry_queue_name(attempt),
        durable=True,
        arguments={
            "x-message-ttl": int(retry_delay(attempt, base_delay) * 1000),
            "x-dead-letter-exchange": PAYMENTS_EXCHANGE_NAME,
            "x-dead-letter-routing-key": PAYMENTS_NEW_ROUTING_KEY,
        },
    )


async def declare_topology(
    broker: RabbitBroker, max_attempts: int, base_delay: float
) -> None:
    """Declare exchanges and queues. Safe to call from every process on startup."""
    exchange = await broker.declare_exchange(PAYMENTS_EXCHANGE)
    dlx = await broker.declare_exchange(DEAD_LETTER_EXCHANGE)

    queue = await broker.declare_queue(PAYMENTS_NEW_QUEUE)
    await queue.bind(exchange, routing_key=PAYMENTS_NEW_ROUTING_KEY)

    dlq = await broker.declare_queue(DEAD_LETTER_QUEUE)
    await dlq.bind(dlx, routing_key=DEAD_LETTER_ROUTING_KEY)

    # attempt N failed -> wait in retry.N; the last attempt goes to the DLQ instead
    for attempt in range(1, max_attempts):
        await broker.declare_queue(retry_queue(attempt, base_delay))
