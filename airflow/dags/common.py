from __future__ import annotations

import json
import logging
import re
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict

import pendulum
from airflow.models import Variable
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from jinja2 import Template

LOGGER = logging.getLogger(__name__)

AIRFLOW_ROOT = Path(__file__).resolve().parents[1]
PROJECT_ROOT = AIRFLOW_ROOT.parent
DDL_PATH = PROJECT_ROOT / "SQL" / "create_raw_tables.sql"

DAG_USER_AGENT = "unybarnd-airflow-ingestion"

DEFAULT_ARGS = {
    "owner": "osama",
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=5),
}

_DDL_CACHE: Dict[str, str] = {}


def get_bool_variable(name: str, default: bool = False) -> bool:
    """Return Airflow Variable as boolean with sane defaults."""
    raw_value = Variable.get(name, default_var=str(default)).strip().lower()
    return raw_value in {"true", "1", "yes", "y"}


def get_gcp_project() -> str:
    return Variable.get("gcp_project")


def get_bq_dataset() -> str:
    return Variable.get("bq_dataset_raw", default_var="raw")


def get_raw_bucket() -> str:
    return Variable.get("gcs_bucket_raw")


def get_bq_location() -> str:
    return Variable.get("gcp_bigquery_location", default_var="EU")


def sanitize_run_id(run_id: str) -> str:
    """Sanitize Airflow run_id for use in load identifiers."""
    return re.sub(r"[^0-9a-zA-Z_]+", "_", run_id)


def render_json_template(template_str: str, context: Dict[str, Any]) -> Dict[str, Any]:
    if not template_str:
        return {}
    rendered = Template(template_str).render(**context)
    if not rendered:
        return {}
    return json.loads(rendered)


def json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return value


def load_table_ddl(table_name: str, project: str, dataset: str) -> str:
    """Fetch and cache CREATE TABLE statement for the requested table."""
    cache_key = f"{project}.{dataset}.{table_name}"
    cached = _DDL_CACHE.get(cache_key)
    if cached:
        return cached

    ddl_text = DDL_PATH.read_text(encoding="utf-8")
    pattern = re.compile(rf"(CREATE TABLE IF NOT EXISTS\s+`[^`]*{table_name}`.*?;)", re.DOTALL | re.IGNORECASE)
    match = pattern.search(ddl_text)
    if not match:
        raise ValueError(f"DDL for table {table_name} not found in {DDL_PATH}")

    statement = match.group(1)
    qualified_identifier = f"`{project}.{dataset}.{table_name}`"
    statement = re.sub(rf"`[^`]*{table_name}`", qualified_identifier, statement)

    _DDL_CACHE[cache_key] = statement
    return statement


def partition_has_rows(table_name: str, context: Dict[str, Any]) -> bool:
    """Return whether the DATE(ingested_at) partition already has data."""
    project = get_gcp_project()
    dataset = get_bq_dataset()
    location = get_bq_location()

    hook = BigQueryHook(gcp_conn_id="google_cloud_default", location=location)
    client = hook.get_client(project_id=project)

    query = (
        f"SELECT COUNT(1) AS row_count FROM `{project}.{dataset}.{table_name}` "
        "WHERE DATE(ingested_at) = @partition_date"
    )
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("partition_date", "DATE", context["ds"])
        ]
    )
    try:
        result = client.query(query, job_config=job_config, location=location)
        rows = list(result)
    except NotFound:
        return False
    if not rows:
        return False
    return int(rows[0][0]) > 0


def seed_from_context(context: Dict[str, Any]) -> int:
    """Deterministic seed derived from data interval/logical date."""
    data_interval_start = context.get("data_interval_start")
    if hasattr(data_interval_start, "format"):
        return int(data_interval_start.format("YYYYMMDDHH"))
    logical_date = context.get("logical_date")
    if hasattr(logical_date, "format"):
        return int(logical_date.format("YYYYMMDDHH"))
    return int(context["ds_nodash"])




def ingestion_ts_from_context(context: Dict[str, Any]) -> str:
    """Resolve the ingested_at timestamp string for a DAG run."""
    data_interval_start = context.get("data_interval_start")
    if data_interval_start is None:
        return f"{context['ds']}T00:00:00+00:00"
    return data_interval_start.isoformat()
