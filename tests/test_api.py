import asyncio
import uuid

import httpx
import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import OutboxMessage, Payment

PAYMENT = {
    "amount": "150.25",
    "currency": "RUB",
    "description": "Order #42",
    "metadata": {"order_id": 42},
    "webhook_url": "https://shop.example.com/webhooks/payments",
}


async def create(client: httpx.AsyncClient, body: dict, key: str = "key-1") -> httpx.Response:
    return await client.post("/api/v1/payments", json=body, headers={"Idempotency-Key": key})


async def count(session: AsyncSession, model: type) -> int:
    return await session.scalar(select(func.count()).select_from(model))


async def test_create_payment(client: httpx.AsyncClient, session: AsyncSession) -> None:
    response = await create(client, PAYMENT)

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "pending"
    assert set(body) == {"payment_id", "status", "created_at"}

    payment = await session.get(Payment, uuid.UUID(body["payment_id"]))
    assert payment is not None
    assert str(payment.amount) == "150.25"
    assert payment.meta == {"order_id": 42}


async def test_create_payment_writes_outbox_event(
    client: httpx.AsyncClient, session: AsyncSession
) -> None:
    response = await create(client, PAYMENT)

    event = await session.scalar(select(OutboxMessage))
    assert event is not None
    assert event.event_type == "payment.created"
    assert event.routing_key == "payments.new"
    assert event.payload == {"payment_id": response.json()["payment_id"]}
    assert event.published_at is None


async def test_repeated_request_returns_same_payment(
    client: httpx.AsyncClient, session: AsyncSession
) -> None:
    first = await create(client, PAYMENT)
    second = await create(client, PAYMENT)

    assert second.status_code == 202
    assert second.json() == first.json()
    assert await count(session, Payment) == 1
    assert await count(session, OutboxMessage) == 1


async def test_concurrent_requests_with_same_key(
    client: httpx.AsyncClient, session: AsyncSession
) -> None:
    responses = await asyncio.gather(*(create(client, PAYMENT) for _ in range(5)))

    assert {r.status_code for r in responses} == {202}
    assert len({r.json()["payment_id"] for r in responses}) == 1
    assert await count(session, Payment) == 1


async def test_same_key_with_different_body_is_conflict(client: httpx.AsyncClient) -> None:
    await create(client, PAYMENT)
    response = await create(client, {**PAYMENT, "amount": "1.00"})

    assert response.status_code == 409


async def test_idempotency_key_is_required(client: httpx.AsyncClient) -> None:
    response = await client.post("/api/v1/payments", json=PAYMENT)

    assert response.status_code == 422


@pytest.mark.parametrize("api_key", [None, "wrong-key"])
async def test_api_key_is_required(client: httpx.AsyncClient, api_key: str | None) -> None:
    headers = {"Idempotency-Key": "key-1"}
    client.headers.pop("X-API-Key")
    if api_key is not None:
        headers["X-API-Key"] = api_key

    create_response = await client.post("/api/v1/payments", json=PAYMENT, headers=headers)
    get_response = await client.get(f"/api/v1/payments/{uuid.uuid4()}", headers=headers)

    assert create_response.status_code == 401
    assert get_response.status_code == 401


@pytest.mark.parametrize(
    "override",
    [
        {"amount": "0"},
        {"amount": "-10"},
        {"amount": "10.001"},
        {"currency": "GBP"},
        {"webhook_url": "not-a-url"},
        {"unexpected": "field"},
    ],
)
async def test_invalid_body(client: httpx.AsyncClient, override: dict) -> None:
    response = await create(client, {**PAYMENT, **override})

    assert response.status_code == 422


async def test_get_payment(client: httpx.AsyncClient) -> None:
    payment_id = (await create(client, PAYMENT)).json()["payment_id"]

    response = await client.get(f"/api/v1/payments/{payment_id}")

    assert response.status_code == 200
    body = response.json()
    assert body["id"] == payment_id
    assert body["amount"] == "150.25"
    assert body["currency"] == "RUB"
    assert body["description"] == "Order #42"
    assert body["metadata"] == {"order_id": 42}
    assert body["status"] == "pending"
    assert body["idempotency_key"] == "key-1"
    assert body["webhook_url"] == PAYMENT["webhook_url"]
    assert body["processed_at"] is None


async def test_get_unknown_payment(client: httpx.AsyncClient) -> None:
    response = await client.get(f"/api/v1/payments/{uuid.uuid4()}")

    assert response.status_code == 404
