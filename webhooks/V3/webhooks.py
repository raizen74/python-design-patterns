from uuid import UUID, uuid4

import httpx
from events import Event, EventBus, EventType
from fastapi import APIRouter
from pydantic import BaseModel, HttpUrl


class Webhook(BaseModel):
    id: UUID
    url: HttpUrl
    events: list[EventType]


class WebhookCreate(BaseModel):
    url: HttpUrl
    events: list[EventType]


router = APIRouter()

webhooks: dict[UUID, Webhook] = {}
event_bus: EventBus | None = None


def configure(bus: EventBus) -> None:
    global event_bus
    event_bus = bus


def deliver_webhook(webhook: Webhook, event: Event) -> None:
    """Delivers the event to the webhook's URL."""
    httpx.post(
        str(webhook.url),
        json=event.model_dump(mode="json"),
        timeout=5,
    )


def attach_webhook_listener(webhook: Webhook) -> None:
    if event_bus is None:
        return

    for event_type in webhook.events:
        event_bus.subscribe(
            event_type,
            lambda event, webhook=webhook: deliver_webhook(webhook, event),
        )


@router.post("/webhooks")
def create_webhook(data: WebhookCreate) -> Webhook:
    webhook = Webhook(
        id=uuid4(),
        url=data.url,
        events=data.events,
    )

    webhooks[webhook.id] = webhook
    attach_webhook_listener(webhook)

    return webhook


@router.get("/webhooks")
def list_webhooks() -> list[Webhook]:
    return list(webhooks.values())

# uv run uvicorn webhook_receiver:app --reload --port 8001
## CREATE A WEBHOOK FOR webhook_receiver.py LINK CLICKS EVENT
## it will be triggered whenever a EventType.LINK_CLICKED is published -> links.py /{short_code}

# curl -X POST http://localhost:8000/webhooks \
#   -H "Content-Type: application/json" \
#   -d '{
#     "url": "http://localhost:8001/webhook",
#     "events": ["link.clicked"]
#   }

# curl localhost:8000/webhooks
