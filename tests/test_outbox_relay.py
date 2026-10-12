from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models import OutboxMessage
from app.outbox.relay import OutboxRelay
from tests.factories import create_outbox_messages
from tests.fakes import FakeOutboxPublisher


async def load_outbox(session_factory: async_sessionmaker[AsyncSession]) -> list[OutboxMessage]:
    async with session_factory() as session:
        result = await session.scalars(select(OutboxMessage).order_by(OutboxMessage.created_at))
        return list(result)


async def test_publishes_pending_messages(
    session: AsyncSession, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    await create_outbox_messages(session, 3)
    publisher = FakeOutboxPublisher()

    published = await OutboxRelay(session_factory, publisher).publish_pending()

    assert published == 3
    assert len(publisher.published) == 3
    assert all(m.published_at is not None for m in await load_outbox(session_factory))


async def test_does_not_publish_twice(
    session: AsyncSession, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    await create_outbox_messages(session, 2)
    publisher = FakeOutboxPublisher()
    relay = OutboxRelay(session_factory, publisher)

    await relay.publish_pending()
    second_run = await relay.publish_pending()

    assert second_run == 0
    assert len(publisher.published) == 2


async def test_respects_batch_size(
    session: AsyncSession, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    await create_outbox_messages(session, 5)

    published = await OutboxRelay(
        session_factory, FakeOutboxPublisher(), batch_size=2
    ).publish_pending()

    assert published == 2


async def test_broker_failure_keeps_message_for_next_run(
    session: AsyncSession, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    await create_outbox_messages(session, 3)

    published = await OutboxRelay(
        session_factory, FakeOutboxPublisher(fail_on=1)
    ).publish_pending()

    assert published == 1
    messages = await load_outbox(session_factory)
    unpublished = [m for m in messages if m.published_at is None]
    assert len(unpublished) == 2
    failed = [m for m in unpublished if m.attempts]
    assert len(failed) == 1
    assert "broker is down" in failed[0].last_error

    retry_publisher = FakeOutboxPublisher()
    assert await OutboxRelay(session_factory, retry_publisher).publish_pending() == 2
