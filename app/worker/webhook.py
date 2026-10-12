from typing import Protocol

import httpx

from app.schemas import WebhookPayload


class WebhookDeliveryError(Exception):
    pass


class WebhookSender(Protocol):
    async def send(self, url: str, payload: WebhookPayload) -> None: ...


class HttpWebhookSender:
    def __init__(self, client: httpx.AsyncClient) -> None:
        self._client = client

    async def send(self, url: str, payload: WebhookPayload) -> None:
        try:
            response = await self._client.post(url, json=payload.model_dump(mode="json"))
            response.raise_for_status()
        except httpx.HTTPError as exc:
            raise WebhookDeliveryError(f"Webhook to {url} failed: {exc}") from exc
