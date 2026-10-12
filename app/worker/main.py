import httpx
from faststream import FastStream

from app.config import Settings, get_settings
from app.db import create_engine, create_session_factory
from app.log import setup_logging
from app.messaging.broker import connect_broker, create_broker
from app.worker.consumer import PaymentEventHandler, RabbitRetryScheduler, register_subscriber
from app.worker.gateway import FakePaymentGateway
from app.worker.processor import PaymentProcessor
from app.worker.webhook import HttpWebhookSender


def create_app(settings: Settings | None = None) -> FastStream:
    setup_logging()
    settings = settings or get_settings()

    broker = create_broker(settings)
    engine = create_engine(settings.database_url)
    http_client = httpx.AsyncClient(timeout=settings.webhook_timeout)

    processor = PaymentProcessor(
        create_session_factory(engine),
        FakePaymentGateway(
            min_delay=settings.gateway_min_delay,
            max_delay=settings.gateway_max_delay,
            success_rate=settings.gateway_success_rate,
        ),
        HttpWebhookSender(http_client),
    )
    handler = PaymentEventHandler(
        processor,
        RabbitRetryScheduler(broker),
        max_attempts=settings.max_delivery_attempts,
    )
    register_subscriber(broker, handler)

    app = FastStream(broker)

    @app.on_startup
    async def declare() -> None:
        await connect_broker(broker, settings)

    @app.after_shutdown
    async def cleanup() -> None:
        await http_client.aclose()
        await engine.dispose()

    return app
