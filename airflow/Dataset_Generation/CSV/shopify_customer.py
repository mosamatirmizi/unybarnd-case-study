
from __future__ import annotations

import argparse
import json
import hashlib
import logging
import random
import re
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
from google.cloud import storage  # type: ignore  # External dependency for GCS
from sqlalchemy import Column, MetaData, Table
from sqlalchemy.sql.sqltypes import String  # Used for lightweight metadata

ROW_COUNT = 200
TARGET_TABLE = "raw_shopify_customer"
DEFAULT_SEEDS_DIR = Path("/opt/airflow/dags/dbt/seeds")  # Replace with actual path
DEFAULT_RAW_SCHEMA = Path("/opt/SQL/create_raw_tables.sql")  # Replace with actual path
OUTPUT_DIR = Path("mock_data_output")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
LOGGER = logging.getLogger(__name__)


@dataclass
class ShopifyCustomerSchema:
    columns: list[str]


def extract_shopify_customer_schema(raw_schema_path: Path) -> ShopifyCustomerSchema:
    if not raw_schema_path.exists():
        raise FileNotFoundError(f"DDL file not found: {raw_schema_path}")

    ddl_text = raw_schema_path.read_text(encoding="utf-8")
    pattern = re.compile(
        r"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+`[^`]*raw_shopify_customer`\s*\((?P<body>.*?)\)\s*PARTITION",
        re.IGNORECASE | re.DOTALL,
    )
    match = pattern.search(ddl_text)
    if not match:
        raise ValueError(f"Table {TARGET_TABLE} not defined in {raw_schema_path}")

    body = match.group("body")
    column_pattern = re.compile(r"`(?P<column>[^`]+)`\s+[A-Z]", re.IGNORECASE)
    columns: list[str] = []
    for col_match in column_pattern.finditer(body):
        column_name = col_match.group("column")
        if column_name not in columns:
            columns.append(column_name)

    if "session_id" not in columns:
        try:
            insert_at = columns.index("load_id") + 1
            columns.insert(insert_at, "session_id")
        except ValueError:
            columns.append("session_id")

    if not columns:
        raise ValueError(f"Columns missing for table {TARGET_TABLE} in {raw_schema_path}")

    # Register with SQLAlchemy to keep dependency satisfied.
    meta = MetaData()
    Table(TARGET_TABLE, meta, *(Column(col, String) for col in columns))
    return ShopifyCustomerSchema(columns=columns)


def load_customer_seed(seeds_dir: Path) -> pd.DataFrame:
    seed_path = seeds_dir / "customer.csv"
    if not seed_path.exists():
        raise FileNotFoundError(
            f"Seed dataset {seed_path} not found. Ensure the dbt seeds directory is correct."
        )
    LOGGER.info("Loading customer seed from %s", seed_path)
    frame = pd.read_csv(seed_path)
    if frame.empty:
        raise ValueError("Seed dataset is empty; cannot generate mock data.")
    return frame


def build_mock_dataframe(seed_frame: pd.DataFrame, schema: ShopifyCustomerSchema) -> pd.DataFrame:
    LOGGER.info("Generating %s mock rows for %s", ROW_COUNT, TARGET_TABLE)
    sampled = seed_frame.sample(n=ROW_COUNT, replace=True, random_state=random.randint(0, 99999))

    now = datetime.now(timezone.utc)
    ingestion_ts = now.replace(microsecond=0)
    timestamp_value = ingestion_ts.isoformat()

    # Ensure fallback JSON where missing
    def clean_address(value: Optional[str]) -> str:
        if isinstance(value, str) and value.strip():
            return value
        payload = {
            "street": "Unknown",
            "zip_code": "UNKNOWN",
            "city": "Unknown",
            "country": "Unknown",
        }
        return json.dumps(payload)

    address_values = sampled.get("address_json", pd.Series([None] * ROW_COUNT)).tolist()

    max_hour = ingestion_ts.hour
    max_minute_current_hour = ingestion_ts.minute
    source_ts_values = []
    source_file_values = []
    for email in sampled["email"].tolist():  # pylint: disable=unused-variable
        hour = random.randint(0, max_hour)
        if hour == max_hour:
            minute = random.randint(0, max_minute_current_hour)
        else:
            minute = random.randint(0, 59)
        second = random.randint(0, 59) if hour < max_hour or minute < max_minute_current_hour else min(ingestion_ts.second, 59)
        source_ts = ingestion_ts.replace(hour=hour, minute=minute, second=second)
        source_ts_iso = source_ts.isoformat()
        source_ts_values.append(source_ts_iso)
        source_file_values.append(f"shopify_customer_{source_ts.strftime('%Y%m%dT%H')}.csv")

    def derive_load_id(source_file: str) -> str:
        digest = hashlib.sha256(source_file.encode("utf-8")).hexdigest()
        return digest[:32]

    load_id_values = [derive_load_id(source_file) for source_file in source_file_values]
    session_id_values = [
        hashlib.sha256(load_id.encode("utf-8")).hexdigest()[:32] for load_id in load_id_values
    ]

    ingestion_uuid_value = str(uuid.uuid4())

    data = {
        "ingested_at": [timestamp_value] * ROW_COUNT,
        "id": sampled["customer_id"].tolist(),
        "email": sampled["email"].tolist(),
        "verified_email": sampled["email"].tolist(),
        "addresses_json": [clean_address(value) for value in address_values],
        "load_at": [timestamp_value] * ROW_COUNT,
        "load_id": load_id_values,
        "session_id": session_id_values,
        "source_file": source_file_values,
        "source_ts": source_ts_values,
        "ingestion_uuid": [ingestion_uuid_value] * ROW_COUNT,
    }

    frame = pd.DataFrame(data)
    frame.attrs['ingestion_ts'] = ingestion_ts
    missing_cols = [col for col in schema.columns if col not in frame.columns]
    if missing_cols:
        raise ValueError(f"Generated DataFrame missing expected columns: {missing_cols}")
    return frame[schema.columns]


def ensure_output_dir(path: Path) -> None:
    if not path.exists():
        LOGGER.info("Creating output directory %s", path)
        path.mkdir(parents=True, exist_ok=True)


def save_csv(frame: pd.DataFrame, output_dir: Path) -> Path:
    ensure_output_dir(output_dir)
    ingestion_ts = frame.attrs.get('ingestion_ts', datetime.now(timezone.utc))
    filename = f"{TARGET_TABLE}_{ingestion_ts.strftime('%Y%m%dT%H')}.csv"
    output_path = output_dir / filename
    frame.to_csv(output_path, index=False)
    LOGGER.info("Mock dataset written to %s", output_path)
    return output_path



def generate_mock_shopify_customer(
    seeds_dir: Path,
    raw_schema_path: Path,
    output_dir: Path = OUTPUT_DIR
) -> Path:
    schema = extract_shopify_customer_schema(raw_schema_path)
    seed_frame = load_customer_seed(seeds_dir)
    mock_frame = build_mock_dataframe(seed_frame, schema)
    output_path = save_csv(mock_frame, output_dir)
    return output_path


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate mock CSV dataset for raw_shopify_customer.")
    parser.add_argument(
        "--seeds-dir",
        default=DEFAULT_SEEDS_DIR,
        help="Path to dbt seeds directory (replace placeholder).",
    )
    parser.add_argument(
        "--raw-schema",
        default=DEFAULT_RAW_SCHEMA,
        help="Path to create_raw_tables.sql (replace placeholder).",
    )
    parser.add_argument(
        "--output-dir",
        default=OUTPUT_DIR,
        help="Local directory for generated CSV (defaults to mock_data_output).",
    )
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)
    try:
        generate_mock_shopify_customer(
            seeds_dir=Path(args.seeds_dir),
            raw_schema_path=Path(args.raw_schema),
            output_dir=Path(args.output_dir)
        )
    except Exception as exc:  # pylint: disable=broad-except
        LOGGER.exception("Failed to generate mock dataset: %s", exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
