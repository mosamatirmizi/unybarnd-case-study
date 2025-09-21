from __future__ import annotations

import sys
from pathlib import Path

DAGS_ROOT = Path(__file__).resolve().parent
if str(DAGS_ROOT) not in sys.path:
    sys.path.append(str(DAGS_ROOT))

import hashlib
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict

import pandas as pd
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from common import (
    DEFAULT_ARGS,
    get_bq_dataset,
    get_bq_location,
    get_gcp_project,
    get_raw_bucket,
    ingestion_ts_from_context,
    load_table_ddl,
)

LOGGER = logging.getLogger(__name__)

SHOPIFY_CUSTOMER_GCS_TEMPLATE = (
    "datasets/source/shopify_customers/dt={date}/hr={hour}/shopify_customers_{timestamp}.csv"
)
SHOPIFY_CUSTOMER_SCRIPT = DAGS_ROOT.parent / "Dataset_Generation" / "CSV" / "shopify_customer.py"
SHOPIFY_SEED_DIR = Path(
    Variable.get(
        "shopify_seed_dir",
        default_var=str(DAGS_ROOT.parent / "dbt" / "hitex-case-study" / "seeds"),
    )
).expanduser()
SHOPIFY_OUTPUT_DIR = Path(
    Variable.get("shopify_customer_csv_output_dir", default_var="/opt/airflow/data/shopify/customers")
).expanduser()
SHOPIFY_SCHEMA_PATH = Path(
    Variable.get(
        "shopify_raw_schema_path",
        default_var=str(DAGS_ROOT.parent.parent / "SQL" / "create_raw_tables.sql"),
    )
).expanduser()

if not SHOPIFY_CUSTOMER_SCRIPT.exists():
    raise FileNotFoundError(f"Shopify customer generator script not found: {SHOPIFY_CUSTOMER_SCRIPT}")


def _format_timestamp_parts(context: Dict[str, Any]) -> Dict[str, str]:
    data_interval_start = context.get("data_interval_start")
    if data_interval_start is None:
        logical_date = context.get("logical_date")
        if logical_date is None:
            raise ValueError("Unable to derive data interval for Shopify customer ingestion")
        reference = logical_date
    else:
        reference = data_interval_start
    reference = reference.in_timezone("UTC")
    return {
        "date": reference.strftime("%Y-%m-%d"),
        "hour": reference.strftime("%H"),
        "timestamp": reference.strftime("%Y%m%dT%H%M%S"),
    }


def _normalize_verified_email(series: pd.Series) -> pd.Series:
    as_str = series.fillna("").astype(str).str.strip().str.lower()
    truthy = {"true", "1", "yes", "y", "t"}
    return as_str.apply(lambda value: bool(value) and (value in truthy or "@" in value))


def _clean_string(series: pd.Series) -> pd.Series:
    cleaned = series.fillna("").astype(str).str.strip()
    cleaned = cleaned.replace({"": None, "nan": None, "none": None})
    return cleaned


def prepare_shopify_customers(**context: Dict[str, Any]) -> Dict[str, str]:
    ti = context["ti"]
    generated_path = Path(ti.xcom_pull(task_ids="generate_shopify_customers"))
    if not generated_path.exists():
        raise FileNotFoundError(f"Generated Shopify customer CSV not found at {generated_path}")

    expected_columns = {
        "ingested_at",
        "id",
        "email",
        "verified_email",
        "addresses_json",
        "load_id",
        "session_id",
        "source_file",
        "source_ts",
        "ingestion_uuid",
    }

    df = pd.read_csv(generated_path)
    missing = expected_columns.difference(df.columns)
    if missing:
        raise ValueError(f"Shopify customer CSV missing required columns: {sorted(missing)}")
    if df.empty:
        raise ValueError("Shopify customer generator returned no rows")

    timestamp_parts = _format_timestamp_parts(context)
    gcs_object = SHOPIFY_CUSTOMER_GCS_TEMPLATE.format(**timestamp_parts)
    bucket = get_raw_bucket()
    source_uri = f"gs://{bucket}/{gcs_object}"

    ingested_at_iso = ingestion_ts_from_context(context)
    load_at_dt = datetime.now(timezone.utc)
    load_at_iso = load_at_dt.isoformat()
    load_id = hashlib.sha256(gcs_object.encode("utf-8")).hexdigest()

    verified_series = _normalize_verified_email(df["verified_email"])
    source_ts_series = pd.to_datetime(df["source_ts"], errors="coerce", utc=True)
    source_ts_series = source_ts_series.fillna(pd.Timestamp(load_at_dt))

    processed = pd.DataFrame(
        {
            "ingested_at": ingested_at_iso,
            "id": _clean_string(df["id"]),
            "email": _clean_string(df["email"]),
            "verified_email": verified_series,
            "addresses_json": _clean_string(df["addresses_json"]),
            "load_at": load_at_iso,
            "load_id": load_id,
            "session_id": _clean_string(df["session_id"]),
            "source_file": source_uri,
            "source_ts": source_ts_series.astype(str),
            "ingestion_uuid": [str(uuid.uuid4()) for _ in range(len(df))],
        }
    )

    column_order = [
        "ingested_at",
        "id",
        "email",
        "verified_email",
        "addresses_json",
        "load_at",
        "load_id",
        "session_id",
        "source_file",
        "source_ts",
        "ingestion_uuid",
    ]
    processed = processed[column_order]

    processed_path = SHOPIFY_OUTPUT_DIR / f"shopify_customers_{timestamp_parts['timestamp']}.csv"
    processed_path.parent.mkdir(parents=True, exist_ok=True)
    processed.to_csv(processed_path, index=False)

    LOGGER.info(
        "Prepared %s Shopify customer records at %s (original: %s)",
        len(processed),
        processed_path,
        generated_path,
    )

    return {
        "local_path": str(processed_path),
        "gcs_object": gcs_object,
        "load_id": load_id,
        "ingested_at": ingested_at_iso,
    }


with DAG(
    dag_id="raw_shopify_customer_ingestion",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["raw", "shopify", "customer", "mock"],
    render_template_as_native_obj=True,
) as SHOPIFY_CUSTOMER_DAG:
    project = get_gcp_project()
    dataset = get_bq_dataset()
    location = get_bq_location()
    bucket = get_raw_bucket()

    create_table = BigQueryInsertJobOperator(
        task_id="create_shopify_customer_table",
        configuration={
            "query": {
                "query": load_table_ddl("raw_shopify_customer", project, dataset),
                "useLegacySql": False,
            }
        },
        location=location,
        gcp_conn_id="google_cloud_default",
    )

    generate_customers = BashOperator(
        task_id="generate_shopify_customers",
        do_xcom_push=True,
        bash_command=(
            "set -euo pipefail\n"
            "OUTPUT_DIR=\"{{ params.output_dir }}\"\n"
            "mkdir -p \"$OUTPUT_DIR\"\n"
            "{{ params.python_bin }} {{ params.script_path }} "
            "--seeds-dir {{ params.seed_dir }} "
            "--raw-schema {{ params.schema_path }} "
            "--output-dir \"$OUTPUT_DIR\"\n"
            "GENERATED_FILE=$(ls -t \"$OUTPUT_DIR\"/raw_shopify_customer_*.csv | head -n1)\n"
            "if [ -z \"$GENERATED_FILE\" ]; then\n"
            "  echo \"No Shopify customer CSV produced\" >&2\n"
            "  exit 1\n"
            "fi\n"
            "echo \"$GENERATED_FILE\"\n"
        ),
        params={
            "script_path": str(SHOPIFY_CUSTOMER_SCRIPT),
            "seed_dir": str(SHOPIFY_SEED_DIR),
            "schema_path": str(SHOPIFY_SCHEMA_PATH),
            "output_dir": str(SHOPIFY_OUTPUT_DIR),
            "python_bin": sys.executable or "python3",
        },
    )

    prepare_customers = PythonOperator(
        task_id="prepare_shopify_customers",
        python_callable=prepare_shopify_customers,
    )

    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_shopify_customers_to_gcs",
        gcp_conn_id="google_cloud_default",
        src="{{ ti.xcom_pull(task_ids='prepare_shopify_customers')['local_path'] }}",
        dst="{{ ti.xcom_pull(task_ids='prepare_shopify_customers')['gcs_object'] }}",
        bucket=bucket,
        mime_type="text/csv",
    )

    insert_into_raw = BigQueryInsertJobOperator(
        task_id="load_shopify_customer_raw",
        location=location,
        configuration={
            "load": {
                "sourceUris": [
                    f"gs://{bucket}/{{{{ ti.xcom_pull(task_ids='prepare_shopify_customers')['gcs_object'] }}}}"
                ],
                "destinationTable": {
                    "projectId": project,
                    "datasetId": dataset,
                    "tableId": "raw_shopify_customer",
                },
                "sourceFormat": "CSV",
                "skipLeadingRows": 1,
                "writeDisposition": "WRITE_APPEND",
                "createDisposition": "CREATE_NEVER",
                "schemaUpdateOptions": [
                    "ALLOW_FIELD_ADDITION",
                    "ALLOW_FIELD_RELAXATION",
                ],
                "fieldDelimiter": ",",
                "allowQuotedNewlines": True,
                "ignoreUnknownValues": True,
            }
        },
        gcp_conn_id="google_cloud_default",
    )

    row_count_check = BigQueryCheckOperator(
        task_id="shopify_customer_row_count_check",
        sql=(
            f"SELECT COUNT(1) FROM `{project}.{dataset}.raw_shopify_customer` "
            "WHERE DATE(ingested_at) = DATE('{{ ds }}')"
        ),
        use_legacy_sql=False,
        location=location,
        gcp_conn_id="google_cloud_default",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    [create_table >> generate_customers] >> prepare_customers >> upload_to_gcs >> insert_into_raw >> row_count_check
