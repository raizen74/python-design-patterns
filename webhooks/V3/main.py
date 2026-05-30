# uv run uvicorn main:app --reload --port 8000
import links
import webhooks
from events import EventBus
from fastapi import FastAPI

app = FastAPI()


event_bus = EventBus()

# Shared event bus
links.configure(event_bus)
webhooks.configure(event_bus)

app.include_router(webhooks.router)
app.include_router(links.router)
