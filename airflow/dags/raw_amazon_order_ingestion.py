from __future__ import annotations

import sys
from pathlib import Path

DAGS_ROOT = Path(__file__).resolve().parent
if str(DAGS_ROOT) not in sys.path:
    sys.path.append(str(DAGS_ROOT))

import pandas as pd
import hashlib
import json
import logging
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List

import pendulum
import requests
from airflow import DAG
from airflow.models import Variable
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.utils.trigger_rule import TriggerRule

from common import (
    DAG_USER_AGENT,
    DEFAULT_ARGS,
    get_bq_dataset,
    get_bq_location,
    get_gcp_project,
    get_raw_bucket,
    ingestion_ts_from_context,
    json_default,
    load_table_ddl,
    render_json_template,
)

LOGGER = logging.getLogger(__name__)

AMAZON_GCS_TEMPLATE = (
    "datasets/source/amazon_orders/dt={date}/hr={hour}/amazon_orders_{timestamp}.json"
)

AMAZON_SCHEMA: List[Dict[str, str]] = [
    {"name": "ingested_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "amazon_order_id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "purchase_date", "type": "DATE", "mode": "NULLABLE"},
    {"name": "last_update_date", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "order_status", "type": "STRING", "mode": "NULLABLE"},
    {"name": "fulfillment_channel", "type": "STRING", "mode": "NULLABLE"},
    {"name": "sales_channel", "type": "STRING", "mode": "NULLABLE"},
    {"name": "buyer_name", "type": "STRING", "mode": "NULLABLE"},
    {"name": "buyer_email", "type": "STRING", "mode": "NULLABLE"},
    {"name": "shipping_address_json", "type": "STRING", "mode": "NULLABLE"},
    {"name": "currency", "type": "STRING", "mode": "NULLABLE"},
    {"name": "order_total", "type": "STRING", "mode": "NULLABLE"},
    {"name": "is_b2b", "type": "BOOLEAN", "mode": "NULLABLE"},
    {"name": "load_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "load_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "source_file", "type": "STRING", "mode": "NULLABLE"},
    {"name": "source_ts", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "ingestion_uuid", "type": "STRING", "mode": "NULLABLE"},
]


def _format_timestamp_parts(context: Dict[str, Any]) -> Dict[str, str]:
    data_interval_start = context.get("data_interval_start")
    if data_interval_start is None:
        logical_date = context.get("logical_date")
        if logical_date is None:
            raise ValueError("Unable to derive data interval for Amazon order ingestion")
        reference = logical_date
    else:
        reference = data_interval_start
    reference = reference.in_timezone("UTC")
    return {
        "date": reference.strftime("%Y-%m-%d"),
        "hour": reference.strftime("%H"),
        "timestamp": reference.strftime("%Y%m%dT%H%M%S"),
    }


def fetch_amazon_orders(**context: Dict[str, Any]) -> Dict[str, str]:
    timestamp_parts = _format_timestamp_parts(context)

    bucket = get_raw_bucket()
    output_dir = Path(Variable.get("amazon_json_output_dir", default_var="/opt/airflow/data/amazon")).expanduser()
    output_dir.mkdir(parents=True, exist_ok=True)
    local_path = output_dir / f"amazon_orders_{timestamp_parts['timestamp']}.json"
    gcs_object = AMAZON_GCS_TEMPLATE.format(**timestamp_parts)

    def get_orders_set() -> pd.DataFrame:
        api_url = Variable.get("amazon_order_api_endpoint_path", default_var="http://host.docker.internal:8000/orders")
        params_template = Variable.get("amazon_order_api_query_params", default_var="{}")
        query_params = render_json_template(params_template, context)
        timeout_seconds = int(Variable.get("amazon_api_timeout_seconds", default_var="30"))

        headers = {
            "Accept": "application/json",
            "dag_user_agent": context["dag"].dag_id,
            "User-Agent": DAG_USER_AGENT,
        }

        try:
            response = requests.get(api_url, params=query_params, headers=headers, timeout=timeout_seconds)
        except requests.RequestException as exc:
            raise RuntimeError("Failed to call Amazon orders API") from exc

        if response.status_code >= 400:
            raise RuntimeError(f"Amazon API responded with status {response.status_code}")

        try:
            payload = response.json()
        except json.JSONDecodeError as exc:
            raise ValueError("Amazon API returned invalid JSON") from exc

        data_rows = payload.get("data") or []
        if not isinstance(data_rows, list):
            raise ValueError("Amazon API payload 'data' should be a list")
        if not data_rows:
            raise ValueError("Amazon API returned no order rows")

        metadata = payload.get("metadata") or {}
        required_columns = [
            "order_id",
            "customer_id",
            "created_at",
            "processed_at",
            "currency",
            "total_price",
            "is_b2b"
        ]
        orders_df = pd.DataFrame(data_rows)[required_columns].drop_duplicates()
        


        if orders_df.empty:
            raise ValueError("Amazon API returned no usable order rows")

        for column in required_columns:
            if column not in orders_df.columns:
                orders_df[column] = None
    
        orders_df['created_at'] = pd.to_datetime(orders_df['created_at']).dt.date
    
        return orders_df

    

    seeds_root = DAGS_ROOT.parent / "dbt" / "hitex-case-study" / "seeds"

    def load_seed_df(filename: str) -> pd.DataFrame:
        path = seeds_root / filename
        if not path.exists():
            LOGGER.warning("Seed file %s not found; continuing without lookups", path)
            return pd.DataFrame()
        return pd.read_csv(path)

    customers_df = load_seed_df("customer.csv")[["customer_id", "first_name", "last_name", "email" ,"address_json"]]
    accounts_df = load_seed_df("accounts.csv")[["account_id", "company_name", "company_email" ,"company_address"]]
    orders_df = get_orders_set()

    ingested_at_iso = ingestion_ts_from_context(context)
    load_at_iso = datetime.now(timezone.utc).isoformat()
    load_id = hashlib.sha256(gcs_object.encode("utf-8")).hexdigest()
    source_uri = f"gs://{bucket}/{gcs_object}"
    source_ts_value = load_at_iso

    


    merged = (
        orders_df
        .merge(customers_df, on="customer_id", how="left")
        .merge(accounts_df, left_on="customer_id", right_on="account_id", how="left")
    )

    
    result_df = pd.DataFrame({
        "amazon_order_id": merged["order_id"],
        "purchase_date": merged["created_at"],
        "last_update_date": merged["processed_at"],
        "order_status": "Dispatched",
        "fulfillment_channel": "AFN",
        "sales_channel": "Amazon.de",
        "buyer_name":
            merged["company_name"].combine_first(
                (merged["first_name"].fillna("") + " " + merged["last_name"].fillna("")).str.strip()
            ),
        "buyer_email": merged["email"].combine_first(merged["company_email"]),
        "shipping_address_json": merged["address_json"].combine_first(merged["company_address"]),
        "currency": merged["currency"],
        "order_total": merged["total_price"],
        "is_b2b": merged["is_b2b"]
    }).assign(
        ingested_at=ingested_at_iso,
        load_at=datetime.now(timezone.utc).isoformat(),
        load_id=load_id,
        source_file=source_uri,
        source_ts=source_ts_value,
        ingestion_uuid=[str(uuid.uuid4()) for _ in range(len(merged))]
    )

    

    

    output_columns = [
        "ingested_at",
        "amazon_order_id",
        "purchase_date",
        "last_update_date",
        "order_status",
        "fulfillment_channel",
        "sales_channel",
        "buyer_name",
        "buyer_email",
        "shipping_address_json",
        "currency",
        "order_total",
        "is_b2b",
        "load_at",
        "load_id",
        "source_file",
        "source_ts",
        "ingestion_uuid",
    ]
    aggregated = result_df[output_columns]

    records: List[Dict[str, Any]] = []
    for record in aggregated.to_dict(orient="records"):
        cleaned: Dict[str, Any] = {}
        for key, value in record.items():
            if value is None:
                cleaned[key] = None
            elif isinstance(value, float) and pd.isna(value):
                cleaned[key] = None
            elif pd.isna(value):
                cleaned[key] = None
            else:
                cleaned[key] = value
        records.append(cleaned)

    LOGGER.info(
        "Constructed %s Amazon orders from %s API records with CSV enrichment",
        len(records),
        orders_df.count(),
    )

    with local_path.open("w", encoding="utf-8") as handle:
        for order in records:
            handle.write(json.dumps(order, default=json_default))
            handle.write("\n")

    LOGGER.info("Persisted %s Amazon order records to %s", len(records), local_path)

    return {
        "local_path": str(local_path),
        "gcs_object": gcs_object,
        "load_id": load_id,
        "ingested_at": ingested_at_iso,
    }




with DAG(
    dag_id="raw_amazon_order_ingestion",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["raw", "amazon", "mock"],
    render_template_as_native_obj=True,
) as AMAZON_DAG:
    project = get_gcp_project()
    dataset = get_bq_dataset()
    location = get_bq_location()
    bucket = get_raw_bucket()

    create_table = BigQueryInsertJobOperator(
        task_id="create_amazon_table",
        configuration={
            "query": {
                "query": load_table_ddl("raw_amazon_order", project, dataset),
                "useLegacySql": False,
            }
        },
        location=location,
        gcp_conn_id="google_cloud_default",
    )


    fetch_orders = PythonOperator(
        task_id="fetch_amazon_orders",
        python_callable=fetch_amazon_orders,
        do_xcom_push=True,
    )

    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_amazon_to_gcs",
        gcp_conn_id="google_cloud_default",
        src="{{ ti.xcom_pull(task_ids='fetch_amazon_orders')['local_path'] }}",
        dst="{{ ti.xcom_pull(task_ids='fetch_amazon_orders')['gcs_object'] }}",
        bucket=bucket,
        mime_type="application/json",
    )

    insert_into_raw = BigQueryInsertJobOperator(
        task_id="insert_amazon_raw",
        location=location,
        configuration={
            "load": {
                "sourceUris": [
                    f"gs://{bucket}/{{{{ ti.xcom_pull(task_ids='fetch_amazon_orders')['gcs_object'] }}}}"
                ],
                "destinationTable": {
                    "projectId": project,
                    "datasetId": dataset,
                    "tableId": "raw_amazon_order",
                },
                "sourceFormat": "NEWLINE_DELIMITED_JSON",
                "writeDisposition": "WRITE_APPEND",
                "createDisposition": "CREATE_NEVER",
                "schemaUpdateOptions": ["ALLOW_FIELD_ADDITION", "ALLOW_FIELD_RELAXATION"],
                "ignoreUnknownValues": True,
                "maxBadRecords": 0,
            }
        },
        gcp_conn_id="google_cloud_default",
    )

    row_count_check = BigQueryCheckOperator(
        task_id="amazon_row_count_check",
        sql=f"SELECT COUNT(1) FROM `{project}.{dataset}.raw_amazon_order` WHERE DATE(ingested_at) = DATE('{{{{ ds }}}}')",
        use_legacy_sql=False,
        location=location,
        gcp_conn_id="google_cloud_default",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    [ create_table >> fetch_orders ] >> upload_to_gcs >> insert_into_raw >> row_count_check
