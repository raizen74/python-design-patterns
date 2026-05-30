from typing import Any
from uuid import UUID, uuid4

import httpx
from fastapi import APIRouter
from pydantic import BaseModel, HttpUrl


class Webhook(BaseModel):
    id: UUID
    url: HttpUrl


class WebhookCreate(BaseModel):
    url: HttpUrl


router = APIRouter()

webhooks: dict[UUID, Webhook] = {}


def send_webhooks(data: dict[str, Any]) -> None:
    for webhook in webhooks.values():
        print(f"Delivering webhook to {webhook.url} with payload {data}")
        deliver_webhook(webhook, data)


def deliver_webhook(webhook: Webhook, data: dict[str, Any]) -> None:
    httpx.post(
        str(webhook.url),
        json=data,
        timeout=5,
    )


@router.post("/webhooks")
def create_webhook(data: WebhookCreate) -> Webhook:
    """Populates the webhook dictionary"""
    webhook = Webhook(id=uuid4(), url=data.url)

    webhooks[webhook.id] = webhook
    print(f"{webhooks = }")
    return webhook


@router.get("/webhooks")
def list_webhooks() -> list[Webhook]:
    print(f"{webhooks = }")
    return list(webhooks.values())
