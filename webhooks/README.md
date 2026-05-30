Start the webhook receiver
`uvicorn webhook_receiver:app --reload --port 8001`

Start the main API
`cd V3 && uvicorn main:app --reload --port 8000`