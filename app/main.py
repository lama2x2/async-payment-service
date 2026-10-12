import asyncio
import contextlib
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.payments import router as payments_router
from app.config import get_settings
from app.db import create_engine, create_session_factory
from app.messaging.broker import connect_broker, create_broker
from app.outbox.relay import OutboxRelay, RabbitOutboxPublisher


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()
    engine = create_engine(settings.database_url)
    session_factory = create_session_factory(engine)
    app.state.session_factory = session_factory

    broker = create_broker(settings)
    await connect_broker(broker, settings)

    relay = OutboxRelay(
        session_factory,
        RabbitOutboxPublisher(broker),
        batch_size=settings.outbox_batch_size,
        poll_interval=settings.outbox_poll_interval,
    )
    relay_task = asyncio.create_task(relay.run(), name="outbox-relay")

    try:
        yield
    finally:
        relay_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await relay_task
        await broker.stop()
        await engine.dispose()


def create_app() -> FastAPI:
    app = FastAPI(title="Payment Service", version="0.1.0", lifespan=lifespan)
    app.include_router(payments_router)

    @app.get("/health", tags=["service"])
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    return app


app = create_app()
