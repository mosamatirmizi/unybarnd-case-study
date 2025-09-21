from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List


class ProductsPayloadError(RuntimeError):
    """Raised when the products seed cannot be converted into a payload."""


@dataclass(frozen=True)
class ProductsPayload:
    metadata: Dict[str, Any]
    data: List[Dict[str, Any]]

    def to_dict(self) -> Dict[str, Any]:
        return {"metadata": self.metadata, "data": self.data}


def _default_products_path(reference_file: Path) -> Path:
    airflow_dir = reference_file.resolve().parents[2]
    return airflow_dir / "dbt" / "hitex-case-study" / "seeds" / "products.csv"


def load_products_payload(products_path: Path | None = None) -> ProductsPayload:
    """Read the Shopify products seed and return a structured JSON payload."""

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

    return ProductsPayload(metadata=metadata, data=records)
