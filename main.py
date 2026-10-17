
import base64
import json
import math
import secrets
import threading
import time
from collections import deque

from fastapi import FastAPI, Header, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel


# Your assigned values
TOTAL_ORDERS = 40
RATE_LIMIT = 19
WINDOW_SECONDS = 10

app = FastAPI(title="Orders API")

# Allow browser-based grading from other origins.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Demo state: kept in memory while this Python process is running.
# For a multi-instance production service, use shared persistent storage.
state_lock = threading.Lock()

created_orders = {}
idempotency_keys = {}
client_requests = {}


def catalog_order(order_id: int):
    """Return one of the 40 fixed catalog orders."""
    return {
        "id": order_id,
        "description": f"Catalog order {order_id}",
        "status": "available",
    }


def encode_cursor(offset: int) -> str:
    """Encode an offset as an opaque cursor."""
    raw = json.dumps({"offset": offset}).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def decode_cursor(cursor: str) -> int:
    """Decode a cursor returned by this API."""
    try:
        padded = cursor + "=" * (-len(cursor) % 4)
        raw = base64.urlsafe_b64decode(padded.encode("ascii"))
        data = json.loads(raw.decode("utf-8"))
        offset = data["offset"]

        if type(offset) is not int or not 0 <= offset <= TOTAL_ORDERS:
            raise ValueError("Invalid offset")

        return offset
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid cursor")


@app.middleware("http")
async def enforce_rate_limit(request: Request, call_next):
    # Browser CORS preflight must be allowed through.
    if request.method == "OPTIONS":
        return await call_next(request)

    client_id = request.headers.get("X-Client-Id", "anonymous")
    now = time.monotonic()

    with state_lock:
        timestamps = client_requests.setdefault(client_id, deque())

        # Discard requests that are outside the rolling 10-second window.
        while timestamps and now - timestamps[0] >= WINDOW_SECONDS:
            timestamps.popleft()

        if len(timestamps) >= RATE_LIMIT:
            retry_after = max(
                1, math.ceil(WINDOW_SECONDS - (now - timestamps[0]))
            )
            return JSONResponse(
                status_code=429,
                content={"detail": "Rate limit exceeded"},
                headers={"Retry-After": str(retry_after)},
            )

        # Count this request against this client's bucket.
        timestamps.append(now)

    return await call_next(request)


class OrderRequest(BaseModel):
    description: str = "New order"


@app.get("/")
def health():
    return {"status": "ok", "total_orders": TOTAL_ORDERS}


@app.post("/orders", status_code=201)
def create_order(
    body: OrderRequest,
    idempotency_key: str = Header(
        ..., alias="Idempotency-Key", min_length=1
    ),
):
    with state_lock:
        # Return the original order for a repeated key.
        if idempotency_key in idempotency_keys:
            order_id = idempotency_keys[idempotency_key]
            return created_orders[order_id]

        # Make a unique ID for this newly created order.
        order_id = "ord_" + secrets.token_urlsafe(12)

        order = {
            "id": order_id,
            "description": body.description,
            "status": "created",
        }

        created_orders[order_id] = order
        idempotency_keys[idempotency_key] = order_id

        return order


@app.get("/orders")
def list_orders(
    limit: int = Query(default=10, ge=1, le=100),
    cursor: str | None = Query(default=None),
):
    # The cursor represents how many catalog items have been served.
    offset = decode_cursor(cursor) if cursor else 0

    end = min(offset + limit, TOTAL_ORDERS)
    items = [
        catalog_order(order_id)
        for order_id in range(offset + 1, end + 1)
    ]

    next_cursor = encode_cursor(end) if end < TOTAL_ORDERS else None

    return {
        "items": items,
        "next_cursor": next_cursor,
    }
