from __future__ import annotations

import csv
import uuid
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from random import Random
from typing import Any, Dict, Iterable, List, Sequence


MIN_ITEMS_PER_ORDER = 3
MAX_ITEMS_PER_ORDER = 5
MAX_ORDERS_PER_CUSTOMER = 4


class PayloadError(RuntimeError):
    """Base error for payload generation issues."""


class ProductsPayloadError(PayloadError):
    """Raised when the products payload cannot be produced."""


class OrdersPayloadError(PayloadError):
    """Raised when the orders payload cannot be produced."""


@dataclass(frozen=True)
class DataPayload:
    metadata: Dict[str, Any]
    data: List[Dict[str, Any]]

    def to_dict(self) -> Dict[str, Any]:
        return {"metadata": self.metadata, "data": self.data}


@dataclass(frozen=True)
class Product:
    product_id: str
    sku: str
    price: Decimal


@dataclass(frozen=True)
class Buyer:
    identifier: str
    is_b2b: bool


def _default_seeds_dir(reference_file: Path) -> Path:
    return reference_file.resolve().parents[2] / "dbt" / "hitex-case-study" / "seeds"


def _default_products_path(reference_file: Path) -> Path:
    return _default_seeds_dir(reference_file) / "products.csv"


def _default_customer_path(reference_file: Path) -> Path:
    return _default_seeds_dir(reference_file) / "customer.csv"


def _default_accounts_path(reference_file: Path) -> Path:
    return _default_seeds_dir(reference_file) / "accounts.csv"


def _load_products(path: Path) -> List[Product]:
    if not path.exists():
        raise OrdersPayloadError(f"products seed not found at {path}")

    products: List[Product] = []
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if row.get("status") != "Available":
                continue
            try:
                price = Decimal(row["price"]).quantize(Decimal("0.01"))
            except Exception as exc:  # noqa: BLE001
                raise OrdersPayloadError("invalid price encountered in products seed") from exc
            products.append(Product(row["product_id"], row["seller_sku"], price))

    if len(products) < MIN_ITEMS_PER_ORDER:
        raise OrdersPayloadError("insufficient available products to build orders")

    return products


def _load_buyers(customer_path: Path, accounts_path: Path) -> List[Buyer]:
    buyers: List[Buyer] = []

    if not customer_path.exists():
        raise OrdersPayloadError(f"customer seed not found at {customer_path}")
    with customer_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            identifier = row.get("customer_id")
            if identifier:
                buyers.append(Buyer(identifier, False))

    if not accounts_path.exists():
        raise OrdersPayloadError(f"accounts seed not found at {accounts_path}")
    with accounts_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            identifier = row.get("account_id")
            if identifier:
                buyers.append(Buyer(identifier, True))

    if not buyers:
        raise OrdersPayloadError("no buyers available from customer or accounts seeds")

    return buyers


def _start_of_day(now: datetime) -> datetime:
    return datetime.combine(now.date(), time.min, tzinfo=now.tzinfo)


def _random_datetime_today(rng: Random, now: datetime) -> datetime:
    start = _start_of_day(now)
    span = (now - start).total_seconds()
    if span <= 0:
        return now
    offset = rng.uniform(0, span)
    return start + timedelta(seconds=offset)


def _iter_buyers(rng: Random, buyers: Sequence[Buyer], order_goal: int) -> Iterable[Buyer]:
    counts = {buyer.identifier: 0 for buyer in buyers}
    for _ in range(order_goal):
        eligible = [b for b in buyers if counts[b.identifier] < MAX_ORDERS_PER_CUSTOMER]
        if not eligible:
            break
        chosen = rng.choice(eligible)
        counts[chosen.identifier] += 1
        yield chosen


def _format_decimal(value: Decimal) -> str:
    return str(value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def load_products_payload(products_path: Path | None = None) -> DataPayload:
    base_path = Path(__file__)
    path = products_path or _default_products_path(base_path)

    if not path.exists():
        raise ProductsPayloadError(f"products seed not found at {path}")

    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        records = list(reader)
        columns = reader.fieldnames or []

    if not records:
        raise ProductsPayloadError("products seed is empty")

    metadata = {
        "source": str(path),
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "record_count": len(records),
        "columns": columns,
    }

    return DataPayload(metadata=metadata, data=records)


def load_orders_payload(
    *,
    products_path: Path | None = None,
    customer_path: Path | None = None,
    accounts_path: Path | None = None,
    order_goal: int | None = None,
    seed: int | None = None,
) -> DataPayload:
    reference = Path(__file__)
    products_file = products_path or _default_products_path(reference)
    customer_file = customer_path or _default_customer_path(reference)
    accounts_file = accounts_path or _default_accounts_path(reference)

    rng = Random(seed)
    processed_at = datetime.now(timezone.utc)

    products = _load_products(products_file)
    buyers = _load_buyers(customer_file, accounts_file)

    capacity = len(buyers) * MAX_ORDERS_PER_CUSTOMER
    desired_orders = order_goal if order_goal is not None else len(buyers) * 5
    desired_orders = max(0, min(desired_orders, capacity))
    if desired_orders == 0:
        raise OrdersPayloadError("order_goal resolved to zero; increase input parameters")

    max_items = min(MAX_ITEMS_PER_ORDER, len(products))

    rows: List[Dict[str, Any]] = []
    unique_orders = set()

    for buyer in _iter_buyers(rng, buyers, desired_orders):
        order_id = str(uuid.uuid4())
        unique_orders.add(order_id)
        created_at = _random_datetime_today(rng, processed_at)
        item_count = rng.randint(MIN_ITEMS_PER_ORDER, max_items)
        products_sample = rng.sample(products, item_count)
        for product in products_sample:
            quantity = rng.randint(1, 5)
            total_price = (product.price * Decimal(quantity)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
            rows.append(
                {
                    "order_id": order_id,
                    "created_at": created_at.replace(microsecond=0),
                    "processed_at": processed_at.replace(microsecond=0),
                    "order_item_id": str(uuid.uuid4()),
                    "product_id": product.product_id,
                    "SKU": product.sku,
                    "quantity": quantity,
                    "price": _format_decimal(product.price),
                    "currency": "EUR",
                    "total_price": _format_decimal(total_price),
                    "customer_id": buyer.identifier,
                    "is_b2b": buyer.is_b2b,
                }
            )

    columns = [
        "order_id",
        "created_at",
        "processed_at",
        "order_item_id",
        "product_id",
        "SKU",
        "quantity",
        "price",
        "currency",
        "total_price",
        "customer_id",
        "is_b2b",
    ]

    metadata = {
        "sources": {
            "products": str(products_file),
            "customers": str(customer_file),
            "accounts": str(accounts_file),
        },
        "generated_at": processed_at.isoformat(timespec="seconds"),
        "record_count": len(rows),
        "unique_orders": len(unique_orders),
        "columns": columns,
        "constraints": {
            "max_orders_per_customer": MAX_ORDERS_PER_CUSTOMER,
            "item_count_range": [MIN_ITEMS_PER_ORDER, max_items],
        },
        "parameters": {
            "order_goal": desired_orders,
            "seed": seed,
        },
    }

    return DataPayload(metadata=metadata, data=rows)
