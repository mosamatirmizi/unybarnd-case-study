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

SHOPIFY_GCS_TEMPLATE = (
    "datasets/source/shopify_orders/dt={date}/hr={hour}/shopify_orders_{timestamp}.csv"
)
SHOPIFY_MOCK_SCRIPT = DAGS_ROOT.parent / "Dataset_Generation" / "CSV" / "shopify_order.py"
SHOPIFY_SEED_DIR = Path(
    Variable.get(
        "shopify_seed_dir",
        default_var=str(DAGS_ROOT.parent / "dbt" / "hitex-case-study" / "seeds"),
    )
).expanduser()
SHOPIFY_OUTPUT_DIR = Path(
    Variable.get("shopify_csv_output_dir", default_var="/opt/airflow/data/shopify")
).expanduser()
SHOPIFY_ORDER_TARGET = int(Variable.get("shopify_order_total", default_var="10"))


if not SHOPIFY_MOCK_SCRIPT.exists():
    raise FileNotFoundError(f"Shopify generator script not found: {SHOPIFY_MOCK_SCRIPT}")


def _format_timestamp_parts(context: Dict[str, Any]) -> Dict[str, str]:
    data_interval_start = context.get("data_interval_start")
    if data_interval_start is None:
        # Fallback to logical date with midnight when data interval is unavailable
        logical_date = context.get("logical_date")
        if logical_date is None:
            raise ValueError("Unable to derive data interval for Shopify ingestion")
        reference = logical_date
    else:
        reference = data_interval_start
    reference = reference.in_timezone("UTC")
    return {
        "date": reference.strftime("%Y-%m-%d"),
        "hour": reference.strftime("%H"),
        "timestamp": reference.strftime("%Y%m%dT%H%M%S"),
    }


def prepare_shopify_orders(**context: Dict[str, Any]) -> Dict[str, str]:
    ti = context["ti"]
    generated_path = Path(ti.xcom_pull(task_ids="generate_shopify_orders"))
    if not generated_path.exists():
        raise FileNotFoundError(f"Generated Shopify CSV not found at {generated_path}")

    required_columns = {
        "order_id",
        "created_at",
        "processed_at",
        "quantity",
        "currency",
        "total_price",
        "customer_id",
        "is_b2b",
    }

    df = pd.read_csv(generated_path)
    missing = required_columns.difference(df.columns)
    if missing:
        raise ValueError(f"Shopify CSV missing required columns: {sorted(missing)}")
    if df.empty:
        raise ValueError("Shopify generator returned no rows")

    order_df = (
        df.groupby(["order_id","created_at","processed_at","currency","customer_id","is_b2b"], as_index=False)
        .agg(
            product_count=("product_id", "nunique"),
            total_quantity=("quantity", "sum"),
            total_price=("total_price", "sum")
        )
    )

    timestamp_parts = _format_timestamp_parts(context)
    gcs_object = SHOPIFY_GCS_TEMPLATE.format(**timestamp_parts)
    bucket = get_raw_bucket()
    source_uri = f"gs://{bucket}/{gcs_object}"

    ingested_at_iso = ingestion_ts_from_context(context)
    load_at_iso = datetime.now(timezone.utc).isoformat()
    load_id = hashlib.sha256(gcs_object.encode("utf-8")).hexdigest()

    processed = pd.DataFrame(
        {
            "ingested_at": ingested_at_iso,
            "id": order_df["order_id"].astype(str).str.strip(),
            "created_at": pd.to_datetime(order_df["created_at"], errors="raise"),
            "processed_at": pd.to_datetime(order_df["processed_at"], errors="raise"),
            "financial_status" : "PAID",
            "fulfillment_status" : "Fulfilled",
            "currency": order_df["currency"].astype(str).str.strip(),
            "total_items": pd.to_numeric(order_df["total_quantity"], errors="raise"),
            "total_price": pd.to_numeric(order_df["total_price"], errors="raise").round(2),
            "customer_id": order_df["customer_id"].astype(str).str.strip(), 
            "source_name": "Shopify",
            "utm_source" : "na",
            "utm_medium" : "na",
            "utm_campaign" : "na",
            "load_at": load_at_iso,
            "load_id": load_id,
            "source_file": source_uri,
            "source_ts": load_at_iso,
            "ingestion_uuid": [str(uuid.uuid4()) for _ in range(len(order_df))],
        }
    )

    processed_path = SHOPIFY_OUTPUT_DIR / f"shopify_orders_{timestamp_parts['timestamp']}.csv"
    processed_path.parent.mkdir(parents=True, exist_ok=True)
    processed.to_csv(processed_path, index=False)

    LOGGER.info(
        "Prepared %s Shopify order records at %s (original: %s)",
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
    dag_id="raw_shopify_order_ingestion",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["raw", "shopify", "mock"],
    render_template_as_native_obj=True,
) as SHOPIFY_DAG:
    project = get_gcp_project()
    dataset = get_bq_dataset()
    location = get_bq_location()
    bucket = get_raw_bucket()

    create_table = BigQueryInsertJobOperator(
        task_id="create_shopify_order_table",
        configuration={
            "query": {
                "query": load_table_ddl("raw_shopify_order", project, dataset),
                "useLegacySql": False,
            }
        },
        location=location,
        gcp_conn_id="google_cloud_default",
    )

    generate_orders = BashOperator(
        task_id="generate_shopify_orders",
        do_xcom_push=True,
        bash_command=(
            "set -euo pipefail\n"
            "OUTPUT_DIR=\"{{ params.output_dir }}\"\n"
            "mkdir -p \"$OUTPUT_DIR\"\n"
            "{{ params.python_bin }} {{ params.script_path }} "
            "--seed-dir {{ params.seed_dir }} "
            "--output-dir \"$OUTPUT_DIR\" "
            "--orders {{ params.orders }} "
            "--seed {{ (data_interval_start or logical_date).strftime('%Y%m%d%H') }}\n"
            "GENERATED_FILE=$(ls -t \"$OUTPUT_DIR\"/shopify_order_*.csv | head -n1)\n"
            "if [ -z \"$GENERATED_FILE\" ]; then\n"
            "  echo \"No Shopify CSV produced\" >&2\n"
            "  exit 1\n"
            "fi\n"
            "echo \"$GENERATED_FILE\"\n"
        ),
        params={
            "script_path": str(SHOPIFY_MOCK_SCRIPT),
            "seed_dir": str(SHOPIFY_SEED_DIR),
            "output_dir": str(SHOPIFY_OUTPUT_DIR),
            "orders": SHOPIFY_ORDER_TARGET,
            "python_bin": sys.executable or "python3",
        },
    )

    prepare_orders = PythonOperator(
        task_id="prepare_shopify_orders",
        python_callable=prepare_shopify_orders,
    )

    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_shopify_to_gcs",
        gcp_conn_id="google_cloud_default",
        src="{{ ti.xcom_pull(task_ids='prepare_shopify_orders')['local_path'] }}",
        dst="{{ ti.xcom_pull(task_ids='prepare_shopify_orders')['gcs_object'] }}",
        bucket=bucket,
        mime_type="text/csv",
    )

    insert_into_raw = BigQueryInsertJobOperator(
        task_id="load_shopify_order_raw",
        location=location,
        configuration={
            "load": {
                "sourceUris": [
                    f"gs://{bucket}/{{{{ ti.xcom_pull(task_ids='prepare_shopify_orders')['gcs_object'] }}}}"
                ],
                "destinationTable": {
                    "projectId": project,
                    "datasetId": dataset,
                    "tableId": "raw_shopify_order",
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
        task_id="shopify_order_row_count_check",
        sql=(
            f"SELECT COUNT(1) FROM `{project}.{dataset}.raw_shopify_order` "
            "WHERE DATE(ingested_at) = DATE('{{ ds }}')"
        ),
        use_legacy_sql=False,
        location=location,
        gcp_conn_id="google_cloud_default",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    [create_table >> generate_orders] >> prepare_orders >> upload_to_gcs >> insert_into_raw >> row_count_check
