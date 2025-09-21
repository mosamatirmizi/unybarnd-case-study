from __future__ import annotations

import argparse
import csv
import uuid
from dataclasses import dataclass
from datetime import datetime, time, timezone, timedelta
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from random import Random
from typing import Iterable, Sequence


MAX_ITEMS_PER_ORDER = 10
MIN_ITEMS_PER_ORDER = 3
MAX_ORDERS_PER_CUSTOMER = 14  


@dataclass(frozen=True)
class Product:
    product_id: str
    sku: str
    price: Decimal


@dataclass(frozen=True)
class Buyer:
    identifier: str
    is_b2b: bool


def load_csv_rows(path: Path) -> Sequence[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def load_products(path: Path) -> list[Product]:
    products = []
    for row in load_csv_rows(path):
        if row.get("status") != "Available":
            continue
        price = Decimal(row["price"]).quantize(Decimal("0.01"))
        products.append(Product(row["product_id"], row["seller_sku"], price))
    if not products:
        raise ValueError("No available products found in products seed.")
    return products


def load_buyers(customer_path: Path, account_path: Path) -> list[Buyer]:
    buyers: list[Buyer] = []
    for row in load_csv_rows(customer_path):
        buyers.append(Buyer(row["customer_id"], False))
    for row in load_csv_rows(account_path):
        buyers.append(Buyer(row["account_id"], True))
    if not buyers:
        raise ValueError("No buyers found across customer and accounts seeds.")
    return buyers


def random_datetime_today(rng: Random, now: datetime) -> datetime:
    start_of_day = datetime.combine(now.date(), time.min, tzinfo=now.tzinfo)
    total_seconds = (now - start_of_day).total_seconds()
    if total_seconds <= 0:
        return now
    offset = rng.uniform(0, total_seconds)
    return start_of_day + timedelta(seconds=offset)


def iter_order_buyers(
    rng: Random, buyers: Sequence[Buyer], order_goal: int
) -> Iterable[Buyer]:
    if order_goal <= 0:
        return
    capacity = len(buyers) * MAX_ORDERS_PER_CUSTOMER
    order_goal = min(order_goal, capacity)
    order_counts = {buyer.identifier: 0 for buyer in buyers}

    for _ in range(order_goal):
        eligible = [
            buyer for buyer in buyers if order_counts[buyer.identifier] < MAX_ORDERS_PER_CUSTOMER
        ]
        if not eligible:
            break
        buyer = rng.choice(eligible)
        order_counts[buyer.identifier] += 1
        yield buyer


def format_decimal(value: Decimal) -> str:
    return str(value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def build_orders(
    rng: Random,
    buyers: Sequence[Buyer],
    products: Sequence[Product],
    processed_at: datetime,
    total_orders: int,
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    max_items = min(MAX_ITEMS_PER_ORDER, len(products))
    if max_items < MIN_ITEMS_PER_ORDER:
        raise ValueError("Not enough distinct products to populate each order.")

    for buyer in iter_order_buyers(rng, buyers, total_orders):
        order_id = str(uuid.uuid4())
        created_at = random_datetime_today(rng, processed_at)
        item_count = rng.randint(MIN_ITEMS_PER_ORDER, max_items)
        for product in rng.sample(products, item_count):
            quantity = rng.randint(1, 5)
            total_price = (product.price * Decimal(quantity)).quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP
            )
            rows.append(
                {
                    "order_id": order_id,
                    "created_at": created_at.isoformat(timespec="seconds"),
                    "processed_at": processed_at.isoformat(timespec="seconds"),
                    "order_item_id": str(uuid.uuid4()),
                    "product_id": product.product_id,
                    "SKU": product.sku,
                    "quantity": str(quantity),
                    "price": format_decimal(product.price),
                    "currency": "EUR",
                    "total_price": format_decimal(total_price),
                    "customer_id": buyer.identifier,
                    "is_b2b": str(buyer.is_b2b).lower(),
                }
            )
    return rows


def write_csv(rows: Sequence[dict[str, str]], output_file: Path) -> None:
    fieldnames = [
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
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with output_file.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def parse_args() -> argparse.Namespace:
    default_dir = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(description="Generate mock Shopify order data.")
    parser.add_argument(
        "--seed-dir",
        type=Path,
        default=None,
        help="Path to seed directory.",
    )
    parser.add_argument(
        "--orders",
        type=int,
        default=None,
        help="Total number of orders to generate (defaults to 5x buyers, capped at capacity).",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=default_dir,
        help="Directory for the generated CSV (defaults to the seed directory).",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Random seed for reproducible output.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    rng = Random(args.seed)
    processed_at = datetime.now(timezone.utc)

    products = load_products(args.seed_dir / "products.csv")
    buyers = load_buyers(args.seed_dir / "customer.csv", args.seed_dir / "accounts.csv")

    capacity = len(buyers) * MAX_ORDERS_PER_CUSTOMER
    default_orders = len(buyers) * 5
    total_orders = args.orders if args.orders is not None else default_orders
    total_orders = max(0, min(total_orders, capacity))

    rows = build_orders(rng, buyers, products, processed_at, total_orders)

    timestamp_label = processed_at.strftime("%Y%m%d_%H")
    output_file = args.output_dir / f"shopify_order_{timestamp_label}.csv"
    write_csv(rows, output_file)


if __name__ == "__main__":
    main()
