from faststream.rabbit import RabbitBroker

from app.config import Settings
from app.messaging.topology import declare_topology


def create_broker(settings: Settings) -> RabbitBroker:
    return RabbitBroker(settings.rabbitmq_url)


async def connect_broker(broker: RabbitBroker, settings: Settings) -> None:
    await broker.connect()
    await declare_topology(
        broker,
        max_attempts=settings.max_delivery_attempts,
        base_delay=settings.retry_base_delay,
    )
