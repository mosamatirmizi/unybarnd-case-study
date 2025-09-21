from __future__ import annotations

from pathlib import Path
import sys

from fastapi import FastAPI, HTTPException, Query

CURRENT_DIR = Path(__file__).resolve().parent
if str(CURRENT_DIR) not in sys.path:
    sys.path.append(str(CURRENT_DIR))

from amazon_order import (
    PayloadError,
    load_orders_payload,
    load_products_payload,
)


app = FastAPI(
    title="Amazon Dataset API",
    version="0.2.0",
    description="Serve denormalised product and order data sourced from dbt seeds.",
)


@app.get("/products")
def get_products(products_path: str | None = None) -> dict:
    """Return the full products seed as JSON with metadata."""

    path = Path(products_path) if products_path else None
    try:
        payload = load_products_payload(path)
    except PayloadError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return payload.to_dict()


@app.get("/orders")
def get_orders(
    products_path: str | None = None,
    customer_path: str | None = None,
    accounts_path: str | None = None,
    order_goal: int | None = Query(None, ge=1),
    seed: int | None = None,
) -> dict:
    """Return generated mock order payload using seed datasets."""

    kwargs = {
        "products_path": Path(products_path) if products_path else None,
        "customer_path": Path(customer_path) if customer_path else None,
        "accounts_path": Path(accounts_path) if accounts_path else None,
        "order_goal": order_goal,
        "seed": seed,
    }

    try:
        payload = load_orders_payload(**kwargs)
    except PayloadError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return payload.to_dict()


@app.get("/health")
def healthcheck() -> dict:
    return {"status": "ok"}


if __name__ == "__main__":  # pragma: no cover - manual launch helper
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
