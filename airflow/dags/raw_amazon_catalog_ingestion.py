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
from airflow.operators.python import ShortCircuitOperator
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
    partition_has_rows,
    render_json_template,
)

LOGGER = logging.getLogger(__name__)

AMAZON_GCS_TEMPLATE = "datasets/source/amazon_catalog/dt={ds}/amazon_catalog_{ds_nodash}.json"

AMAZON_SCHEMA: List[Dict[str, str]] = [
    {"name": "ingested_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "asin", "type": "STRING", "mode": "REQUIRED"},
    {"name": "seller_sku", "type": "STRING", "mode": "NULLABLE"},
    {"name": "title", "type": "STRING", "mode": "NULLABLE"},
    {"name": "brand", "type": "STRING", "mode": "NULLABLE"},
    {"name": "model", "type": "STRING", "mode": "NULLABLE"},
    {"name": "load_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "load_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "source_file", "type": "STRING", "mode": "NULLABLE"},
    {"name": "source_ts", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "ingestion_uuid", "type": "STRING", "mode": "NULLABLE"},
]
def _should_run_today(table_name: str, **context: Dict[str, Any]) -> bool:
    already_loaded = partition_has_rows(table_name, context)
    if already_loaded:
        LOGGER.info(
            "Skipping %s ingestion for %s because data already exists",
            table_name,
            context.get("ds"),
        )
        return False
    return True


def fetch_amazon_catalog(**context: Dict[str, Any]) -> Dict[str, str]:
    ds = context["ds"]
    ds_nodash = context["ds_nodash"]

    bucket = get_raw_bucket()
    output_dir = Path(Variable.get("amazon_json_output_dir", default_var="/opt/airflow/data/amazon")).expanduser()
    output_dir.mkdir(parents=True, exist_ok=True)
    local_path = output_dir / f"amazon_catalog_{ds_nodash}.json"
    gcs_object = AMAZON_GCS_TEMPLATE.format(ds=ds, ds_nodash=ds_nodash)


    print("local_path", local_path)

    ingested_at_iso = ingestion_ts_from_context(context)
    load_at_iso = datetime.now(timezone.utc).isoformat()
    load_id = hashlib.sha256(gcs_object.encode("utf-8")).hexdigest()
    source_uri = f"gs://{bucket}/{gcs_object}"
    source_ts_value = load_at_iso
    
    api_url = Variable.get("amazon_products_api_endpoint_path", default_var="http://host.docker.internal:8000/products")
    timeout_seconds = int(Variable.get("amazon_api_timeout_seconds", default_var="30"))

    headers = {
        "Accept": "application/json",
        "dag_user_agent": context["dag"].dag_id,
        "User-Agent": DAG_USER_AGENT,
    }

    try:
        response = requests.get(api_url, headers=headers, timeout=timeout_seconds)
    except requests.RequestException as exc:
        raise RuntimeError("Failed to call Amazon Products API") from exc

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
        "product_id",
        "asin",
        "seller_sku",
        "title",
        "brand",
        "model",
        "product_type",
        "status",
        "quantity",
        "attributes_json",
        "price",
        "currency"
    ]
    products_df = pd.DataFrame(data_rows)[required_columns].drop_duplicates().assign(
        ingested_at=ingested_at_iso,
        load_at=load_at_iso,
        load_id=load_id,
        source_file=source_uri,
        source_ts=source_ts_value,
        ingestion_uuid=[str(uuid.uuid4()) for _ in range(len(data_rows))],
    )
    

    if products_df.empty:
        raise ValueError("Amazon API returned no usable order rows")

    for column in required_columns:
        if column not in products_df.columns:
            products_df[column] = None    


    records: List[Dict[str, Any]] = []
    for record in products_df.to_dict(orient="records"):
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
        products_df.count(),
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
    dag_id="raw_amazon_catalog_ingestion",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule="@daily",
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
                "query": load_table_ddl("raw_amazon_catalog", project, dataset),
                "useLegacySql": False,
            }
        },
        location=location,
        gcp_conn_id="google_cloud_default",
    )

    skip_if_loaded = ShortCircuitOperator(
        task_id="skip_if_catalog_already_loaded",
        python_callable=_should_run_today,
        op_kwargs={"table_name": "raw_amazon_catalog"},
    )

    fetch_catalog = PythonOperator(
        task_id="fetch_amazon_products",
        python_callable=fetch_amazon_catalog,
        do_xcom_push=True,
    )

    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_amazon_to_gcs",
        gcp_conn_id="google_cloud_default",
        src="{{ ti.xcom_pull(task_ids='fetch_amazon_products')['local_path'] }}",
        dst="{{ ti.xcom_pull(task_ids='fetch_amazon_products')['gcs_object'] }}",
        bucket=bucket,
        mime_type="application/json",
    )

    insert_into_raw = BigQueryInsertJobOperator(
        task_id="insert_amazon_raw",
        location=location,
        configuration={
            "load": {
                "sourceUris": [
                    f"gs://{bucket}/{{{{ ti.xcom_pull(task_ids='fetch_amazon_products')['gcs_object'] }}}}"
                ],
                "destinationTable": {
                    "projectId": project,
                    "datasetId": dataset,
                    "tableId": "raw_amazon_catalog",
                },
                "sourceFormat": "NEWLINE_DELIMITED_JSON",
                "writeDisposition": "WRITE_TRUNCATE",
                "createDisposition": "CREATE_NEVER",
                # "schemaUpdateOptions": ["ALLOW_FIELD_ADDITION", "ALLOW_FIELD_RELAXATION"],
                "schemaUpdateOptions" : None,
                "ignoreUnknownValues": True,
                "maxBadRecords": 0,
            }
        },
        gcp_conn_id="google_cloud_default",
    )

    row_count_check = BigQueryCheckOperator(
        task_id="amazon_row_count_check",
        sql=f"SELECT COUNT(1) FROM `{project}.{dataset}.raw_amazon_catalog` WHERE DATE(ingested_at) = DATE('{{{{ ds }}}}')",
        use_legacy_sql=False,
        location=location,
        gcp_conn_id="google_cloud_default",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    create_table >> skip_if_loaded
    skip_if_loaded >> fetch_catalog >> upload_to_gcs >> insert_into_raw >> row_count_check
