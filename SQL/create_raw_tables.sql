-- Auto-generated BigQuery DDL for raw landing tables.
-- Review column types, partitioning, clustering, and constraints before running in production.
-- Duplicates and late-arriving data should be handled via downstream deduplication views (MAX(ingested_at) per key).
-- Schedule table maintenance to recluster recent partitions and preserve partition pruning performance.

-- Source: raw_amazon
-- Amazon Marketplace order header (FBA/MCF).
CREATE TABLE IF NOT EXISTS `raw.raw_amazon_order` (
  `ingested_at` TIMESTAMP,
  `amazon_order_id` STRING NOT NULL,
  `purchase_date` DATE,
  `last_update_date` TIMESTAMP,
  `order_status` STRING,
  `fulfillment_channel` STRING,
  `sales_channel` STRING,
  `buyer_name` STRING,
  `buyer_email` STRING,
  `shipping_address_json` STRING,
  `currency` STRING,
  `order_total` NUMERIC,
  `load_at` TIMESTAMP,
  `load_id` STRING,
  `source_file` STRING,
  `source_ts` TIMESTAMP,
  `ingestion_uuid` STRING
)
PARTITION BY DATE(`ingested_at`)
OPTIONS (
  description = "Source table raw_amazon_order from raw_amazon",
  require_partition_filter = true
);

-- Source: raw_amazon
CREATE TABLE IF NOT EXISTS `raw.raw_amazon_order_item` (
  `ingested_at` TIMESTAMP,
  `order_date` DATE,
  `order_item_id` STRING NOT NULL,
  `amazon_order_id` STRING NOT NULL,
  `asin` STRING,
  `seller_sku` STRING,
  `quantity_ordered` INT64,
  `item_price_amount` NUMERIC,
  `tax_amount` NUMERIC,
  `load_at` TIMESTAMP,
  `load_id` STRING,
  `source_file` STRING,
  `source_ts` TIMESTAMP,
  `ingestion_uuid` STRING
)
PARTITION BY DATE(`ingested_at`)
CLUSTER BY `amazon_order_id`
OPTIONS (
  description = "Source table raw_amazon_order_item from raw_amazon",
  require_partition_filter = true
);

-- Source: raw_amazon
CREATE TABLE IF NOT EXISTS `raw.raw_amazon_shipment_item` (
  `ingested_at` TIMESTAMP,
  `shipment_item_id` STRING NOT NULL,
  `shipment_id` STRING,
  `amazon_order_id` STRING,
  `ship_date` DATE,
  `delivery_date` DATE,
  `load_at` TIMESTAMP,
  `load_id` STRING,
  `source_file` STRING,
  `source_ts` TIMESTAMP,
  `ingestion_uuid` STRING
)
PARTITION BY DATE(`ingested_at`)
CLUSTER BY `amazon_order_id`
OPTIONS (
  description = "Source table raw_amazon_shipment_item from raw_amazon",
  require_partition_filter = true
);

-- Source: raw_amazon
CREATE TABLE IF NOT EXISTS `raw.raw_amazon_settlement_tx` (
  `ingested_at` TIMESTAMP,
  `settlement_id` STRING,
  `posted_date` DATE,
  `transaction_type` STRING,
  `amazon_order_id` STRING,
  `amount` NUMERIC,
  `fee_type` STRING,
  `fee_amount` NUMERIC,
  `load_at` TIMESTAMP,
  `load_id` STRING,
  `source_file` STRING,
  `source_ts` TIMESTAMP,
  `ingestion_uuid` STRING
)
PARTITION BY DATE(`ingested_at`)
CLUSTER BY `settlement_id`, `amazon_order_id`
OPTIONS (
  description = "Source table raw_amazon_settlement_tx from raw_amazon",
  require_partition_filter = true
);

-- Source: raw_amazon
CREATE TABLE IF NOT EXISTS `raw.raw_amazon_catalog` (
  `ingested_at` TIMESTAMP,
  `asin` STRING,
  `seller_sku` STRING,
  `title` STRING,
  `brand` STRING,
  `model` STRING,
  `attributes_json` STRING,
  `load_at` TIMESTAMP,
  `load_id` STRING,
  `source_file` STRING,
  `source_ts` TIMESTAMP,
  `ingestion_uuid` STRING
)

OPTIONS (
  description = "Source table raw_amazon_catalog from raw_amazon"
);

-- Source: raw_amazon
CREATE TABLE IF NOT EXISTS `raw.raw_amazon_inventory_daily` (
  `snapshot_date` DATE,
  `seller_sku` STRING,
  `fulfillment_center_id` STRING,
  `on_hand` INT64,
  `reserved` INT64,
  `inbound` INT64,
  `load_at` TIMESTAMP,
  `load_id` STRING,
  `source_file` STRING,
  `source_ts` TIMESTAMP,
  `ingestion_uuid` STRING
)
PARTITION BY `snapshot_date`
CLUSTER BY `seller_sku`, `fulfillment_center_id`
OPTIONS (
  description = "Source table raw_amazon_inventory_daily from raw_amazon",
  require_partition_filter = true
);

-- Source: raw_amazon
-- Amazon FBA customer returns data.
CREATE TABLE IF NOT EXISTS `raw.raw_amazon_returns` (
  `return_date` DATE,
  `amazon_order_id` STRING,
  `sku` STRING,
  `quantity` INT64,
  `reason` STRING,
  `status` STRING,
  `load_at` TIMESTAMP,
  `load_id` STRING,
  `source_file` STRING,
  `source_ts` TIMESTAMP,
  `ingestion_uuid` STRING
)
PARTITION BY `return_date`
CLUSTER BY `amazon_order_id`, `sku`
OPTIONS (
  description = "Source table raw_amazon_returns from raw_amazon",
  require_partition_filter = true
);

-- Source: raw_amazon
-- Amazon FBA reimbursements data.
CREATE TABLE IF NOT EXISTS `raw.raw_amazon_reimbursements` (
  `ingested_at` TIMESTAMP,
  `reimbursement_id` STRING NOT NULL,
  `amazon_order_id` STRING,
  `amount` NUMERIC,
  `reason` STRING,
  `approval_date` DATE,
  `load_at` TIMESTAMP,
  `load_id` STRING,
  `source_file` STRING,
  `source_ts` TIMESTAMP,
  `ingestion_uuid` STRING
)
PARTITION BY DATE(`ingested_at`)
OPTIONS (
  description = "Source table raw_amazon_reimbursements from raw_amazon",
  require_partition_filter = true
);

-- Source: raw_amazon
-- Amazon FBA aged inventory data.
CREATE TABLE IF NOT EXISTS `raw.raw_amazon_inventory_aged` (
  `snapshot_date` DATE,
  `sku` STRING NOT NULL,
  `quantity` INT64,
  `age_category` STRING,
  `storage_fee` NUMERIC,
  `load_at` TIMESTAMP,
  `load_id` STRING,
  `source_file` STRING,
  `source_ts` TIMESTAMP,
  `ingestion_uuid` STRING
)
PARTITION BY `snapshot_date`
CLUSTER BY `sku`
OPTIONS (
  description = "Source table raw_amazon_inventory_aged from raw_amazon",
  require_partition_filter = true
);

-- Source: raw_amazon
-- Amazon FBA stranded inventory data.
CREATE TABLE IF NOT EXISTS `raw.raw_amazon_stranded_inventory` (
  `ingested_at` TIMESTAMP,
  `sku` STRING NOT NULL,
  `quantity` INT64,
  `reason` STRING,
  `resolution_status` STRING,
  `load_at` TIMESTAMP,
  `load_id` STRING,
  `source_file` STRING,
  `source_ts` TIMESTAMP,
  `ingestion_uuid` STRING
)
PARTITION BY DATE(`ingested_at`)
CLUSTER BY `sku`
OPTIONS (
  description = "Source table raw_amazon_stranded_inventory from raw_amazon",
  require_partition_filter = true
);

-- Source: raw_amazon
-- Amazon FBA storage fees data.
CREATE TABLE IF NOT EXISTS `raw.raw_amazon_storage_fees` (
  `fee_date` DATE,
  `sku` STRING,
  `quantity` INT64,
  `fee_amount` NUMERIC,
  `currency` STRING,
  `load_at` TIMESTAMP,
  `load_id` STRING,
  `source_file` STRING,
  `source_ts` TIMESTAMP,
  `ingestion_uuid` STRING
)
PARTITION BY `fee_date`
CLUSTER BY `sku`
OPTIONS (
  description = "Source table raw_amazon_storage_fees from raw_amazon",
  require_partition_filter = true
);

-- Source: raw_shopify
-- Shopify order header (DTC web/app).
CREATE TABLE IF NOT EXISTS `raw.raw_shopify_order` (
  `ingested_at` TIMESTAMP,
  `id` STRING NOT NULL,
  `created_at` TIMESTAMP,
  `processed_at` TIMESTAMP,
  `financial_status` STRING,
  `fulfillment_status` STRING,
  `currency` STRING,
  `total_items` NUMERIC,
  `total_price` NUMERIC,
  `customer_id` STRING,
  `source_name` STRING,
  `utm_source` STRING,
  `utm_medium` STRING,
  `utm_campaign` STRING,
  `load_at` TIMESTAMP,
  `load_id` STRING,
  `source_file` STRING,
  `source_ts` TIMESTAMP,
  `ingestion_uuid` STRING
)
PARTITION BY DATE(`ingested_at`)
OPTIONS (
  description = "Source table raw_shopify_order from raw_shopify",
  require_partition_filter = true
);

-- Source: raw_shopify
CREATE TABLE IF NOT EXISTS `raw.raw_shopify_order_line` (
  `ingested_at` TIMESTAMP,
  `id` STRING NOT NULL,
  `order_id` STRING NOT NULL,
  `product_id` STRING,
  `variant_id` STRING,
  `sku` STRING,
  `quantity` INT64,
  `price` NUMERIC,
  `load_at` TIMESTAMP,
  `load_id` STRING,
  `source_file` STRING,
  `source_ts` TIMESTAMP,
  `ingestion_uuid` STRING
)
PARTITION BY DATE(`ingested_at`)
CLUSTER BY `order_id`
OPTIONS (
  description = "Source table raw_shopify_order_line from raw_shopify",
  require_partition_filter = true
);

-- Source: raw_shopify
CREATE TABLE IF NOT EXISTS `raw.raw_shopify_fulfillment` (
  `ingested_at` TIMESTAMP,
  `id` STRING NOT NULL,
  `order_id` STRING,
  `status` STRING,
  `tracking_number` STRING,
  `created_at` TIMESTAMP,
  `load_at` TIMESTAMP,
  `load_id` STRING,
  `source_file` STRING,
  `source_ts` TIMESTAMP,
  `ingestion_uuid` STRING
)
PARTITION BY DATE(`ingested_at`)
OPTIONS (
  description = "Source table raw_shopify_fulfillment from raw_shopify",
  require_partition_filter = true
);

-- Source: raw_shopify
CREATE TABLE IF NOT EXISTS `raw.raw_shopify_transaction` (
  `ingested_at` TIMESTAMP,
  `id` STRING NOT NULL,
  `order_id` STRING,
  `kind` STRING,
  `gateway` STRING,
  `amount` NUMERIC,
  `processed_at` TIMESTAMP,
  `load_at` TIMESTAMP,
  `load_id` STRING,
  `source_file` STRING,
  `source_ts` TIMESTAMP,
  `ingestion_uuid` STRING
)
PARTITION BY DATE(`ingested_at`)
OPTIONS (
  description = "Source table raw_shopify_transaction from raw_shopify",
  require_partition_filter = true
);


-- Source: raw_shopify
CREATE TABLE IF NOT EXISTS `raw.raw_shopify_customer` (
  `ingested_at` TIMESTAMP,
  `id` STRING NOT NULL,
  `email` STRING,
  `verified_email` BOOL,
  `addresses_json` STRING,
  `load_at` TIMESTAMP,
  `load_id` STRING,
  `session_id` STRING,
  `source_file` STRING,
  `source_ts` TIMESTAMP,
  `ingestion_uuid` STRING
)
PARTITION BY DATE(`ingested_at`)
OPTIONS (
  description = "Source table raw_shopify_customer from raw_shopify",
  require_partition_filter = true
);

-- Source: raw_shopify
CREATE TABLE IF NOT EXISTS `raw.raw_shopify_product` (
  `ingested_at` TIMESTAMP,
  `id` STRING NOT NULL,
  `title` STRING,
  `vendor` STRING,
  `product_type` STRING,
  `status` STRING,
  `load_at` TIMESTAMP,
  `load_id` STRING,
  `source_file` STRING,
  `source_ts` TIMESTAMP,
  `ingestion_uuid` STRING
)
PARTITION BY DATE(`ingested_at`)
OPTIONS (
  description = "Source table raw_shopify_product from raw_shopify",
  require_partition_filter = true
);

-- Source: raw_shopify
CREATE TABLE IF NOT EXISTS `raw.raw_shopify_inventory_daily` (
  `snapshot_date` DATE,
  `inventory_item_id` STRING,
  `location_id` STRING,
  `available` INT64,
  `load_at` TIMESTAMP,
  `load_id` STRING,
  `source_file` STRING,
  `source_ts` TIMESTAMP,
  `ingestion_uuid` STRING
)
PARTITION BY `snapshot_date`
CLUSTER BY `inventory_item_id`, `location_id`
OPTIONS (
  description = "Source table raw_shopify_inventory_daily from raw_shopify",
  require_partition_filter = true
);

-- Source: raw_shopify
-- Shopify refunds data.
CREATE TABLE IF NOT EXISTS `raw.raw_shopify_refunds` (
  `ingested_at` TIMESTAMP,
  `id` STRING NOT NULL,
  `order_id` STRING,
  `created_at` TIMESTAMP,
  `amount` NUMERIC,
  `refund_line_items` STRING,
  `transactions` STRING,
  `load_at` TIMESTAMP,
  `load_id` STRING,
  `source_file` STRING,
  `source_ts` TIMESTAMP,
  `ingestion_uuid` STRING
)
PARTITION BY DATE(`ingested_at`)
OPTIONS (
  description = "Source table raw_shopify_refunds from raw_shopify",
  require_partition_filter = true
);

-- Source: raw_shopify
-- Shopify discount codes data.
CREATE TABLE IF NOT EXISTS `raw.raw_shopify_discount_codes` (
  `ingested_at` TIMESTAMP,
  `id` STRING NOT NULL,
  `code` STRING,
  `value` NUMERIC,
  `usage_limit` STRING,
  `starts_at` TIMESTAMP,
  `ends_at` TIMESTAMP,
  `applies_to` STRING,
  `load_at` TIMESTAMP,
  `load_id` STRING,
  `source_file` STRING,
  `source_ts` TIMESTAMP,
  `ingestion_uuid` STRING
)
PARTITION BY DATE(`ingested_at`)
OPTIONS (
  description = "Source table raw_shopify_discount_codes from raw_shopify",
  require_partition_filter = true
);

-- Source: raw_shopify
-- Shopify inventory levels data.
CREATE TABLE IF NOT EXISTS `raw.raw_shopify_inventory_levels` (
  `ingested_at` TIMESTAMP,
  `inventory_item_id` STRING NOT NULL,
  `location_id` STRING,
  `available` INT64,
  `updated_at` TIMESTAMP,
  `load_at` TIMESTAMP,
  `load_id` STRING,
  `source_file` STRING,
  `source_ts` TIMESTAMP,
  `ingestion_uuid` STRING
)
PARTITION BY DATE(`ingested_at`)
CLUSTER BY `inventory_item_id`, `location_id`
OPTIONS (
  description = "Source table raw_shopify_inventory_levels from raw_shopify",
  require_partition_filter = true
);

-- Source: raw_shopify
-- Shopify abandoned checkouts data.
CREATE TABLE IF NOT EXISTS `raw.raw_shopify_abandoned_checkouts` (
  `ingested_at` TIMESTAMP,
  `id` STRING NOT NULL,
  `cart_token` STRING,
  `created_at` TIMESTAMP,
  `updated_at` TIMESTAMP,
  `total_price` NUMERIC,
  `line_items` STRING,
  `load_at` TIMESTAMP,
  `load_id` STRING,
  `source_file` STRING,
  `source_ts` TIMESTAMP,
  `ingestion_uuid` STRING
)
PARTITION BY DATE(`ingested_at`)
OPTIONS (
  description = "Source table raw_shopify_abandoned_checkouts from raw_shopify",
  require_partition_filter = true
);

-- Source: raw_shopify
-- Shopify locations data.
CREATE TABLE IF NOT EXISTS `raw.raw_shopify_locations` (
  `ingested_at` TIMESTAMP,
  `id` STRING NOT NULL,
  `name` STRING,
  `address1` STRING,
  `city` STRING,
  `country` STRING,
  `active` BOOL,
  `load_at` TIMESTAMP,
  `load_id` STRING,
  `source_file` STRING,
  `source_ts` TIMESTAMP,
  `ingestion_uuid` STRING
)
PARTITION BY DATE(`ingested_at`)
OPTIONS (
  description = "Source table raw_shopify_locations from raw_shopify",
  require_partition_filter = true
);

-- Source: raw_pos
CREATE TABLE IF NOT EXISTS `raw.raw_pos_receipt` (
  `receipt_id` STRING NOT NULL,
  `business_date` DATE,
  `store_id` STRING,
  `transaction_ts` TIMESTAMP,
  `subtotal` NUMERIC,
  `tax_amount` NUMERIC,
  `total_amount` NUMERIC,
  `load_at` TIMESTAMP,
  `load_id` STRING,
  `source_file` STRING,
  `source_ts` TIMESTAMP,
  `ingestion_uuid` STRING
)
PARTITION BY `business_date`
CLUSTER BY `store_id`
OPTIONS (
  description = "Source table raw_pos_receipt from raw_pos",
  require_partition_filter = true
);

-- Source: raw_pos
CREATE TABLE IF NOT EXISTS `raw.raw_pos_receipt_line` (
  `ingested_at` TIMESTAMP,
  `receipt_line_id` STRING NOT NULL,
  `receipt_id` STRING NOT NULL,
  `product_id` STRING NOT NULL,
  `quantity` INT64,
  `unit_price` NUMERIC,
  `load_at` TIMESTAMP,
  `load_id` STRING,
  `source_file` STRING,
  `source_ts` TIMESTAMP,
  `ingestion_uuid` STRING
)
PARTITION BY DATE(`ingested_at`)
OPTIONS (
  description = "Source table raw_pos_receipt_line from raw_pos",
  require_partition_filter = true
);

-- Source: raw_pos
CREATE TABLE IF NOT EXISTS `raw.raw_pos_tender` (
  `ingested_at` TIMESTAMP,
  `tender_id` STRING NOT NULL,
  `receipt_id` STRING NOT NULL,
  `tender_type` STRING,
  `amount` NUMERIC,
  `load_at` TIMESTAMP,
  `load_id` STRING,
  `source_file` STRING,
  `source_ts` TIMESTAMP,
  `ingestion_uuid` STRING
)
PARTITION BY DATE(`ingested_at`)
OPTIONS (
  description = "Source table raw_pos_tender from raw_pos",
  require_partition_filter = true
);

-- Source: raw_pos
CREATE TABLE IF NOT EXISTS `raw.raw_pos_store` (
  `ingested_at` TIMESTAMP,
  `store_id` STRING NOT NULL,
  `store_name` STRING,
  `address_json` STRING,
  `latitude` FLOAT64,
  `longitude` FLOAT64,
  `timezone` STRING,
  `load_at` TIMESTAMP,
  `load_id` STRING,
  `source_file` STRING,
  `source_ts` TIMESTAMP,
  `ingestion_uuid` STRING
)
PARTITION BY DATE(`ingested_at`)
OPTIONS (
  description = "Source table raw_pos_store from raw_pos",
  require_partition_filter = true
);

-- Source: raw_pos
CREATE TABLE IF NOT EXISTS `raw.raw_pos_inventory_daily` (
  `snapshot_date` DATE,
  `store_id` STRING,
  `product_id` STRING NOT NULL,
  `on_hand` INT64,
  `load_at` TIMESTAMP,
  `load_id` STRING,
  `source_file` STRING,
  `source_ts` TIMESTAMP,
  `ingestion_uuid` STRING
)
PARTITION BY `snapshot_date`
CLUSTER BY `product_id`, `store_id`
OPTIONS (
  description = "Source table raw_pos_inventory_daily from raw_pos",
  require_partition_filter = true
);

-- Source: raw_pos
-- POS returns data.
CREATE TABLE IF NOT EXISTS `raw.raw_pos_returns` (
  `ingested_at` TIMESTAMP,
  `return_id` STRING NOT NULL,
  `receipt_id` STRING,
  `product_id` STRING NOT NULL,
  `quantity` INT64,
  `return_date` DATE,
  `reason` STRING,
  `amount` NUMERIC,
  `load_at` TIMESTAMP,
  `load_id` STRING,
  `source_file` STRING,
  `source_ts` TIMESTAMP,
  `ingestion_uuid` STRING
)
PARTITION BY DATE(`ingested_at`)
OPTIONS (
  description = "Source table raw_pos_returns from raw_pos",
  require_partition_filter = true
);

-- Source: raw_pos
-- POS product category data.
CREATE TABLE IF NOT EXISTS `raw.raw_pos_product_category` (
  `ingested_at` TIMESTAMP,
  `category_id` STRING NOT NULL,
  `category_name` STRING,
  `description` STRING,
  `load_at` TIMESTAMP,
  `load_id` STRING,
  `source_file` STRING,
  `source_ts` TIMESTAMP,
  `ingestion_uuid` STRING
)
PARTITION BY DATE(`ingested_at`)
OPTIONS (
  description = "Source table raw_pos_product_category from raw_pos",
  require_partition_filter = true
);

-- Source: raw_pos
-- POS customer loyalty data.
CREATE TABLE IF NOT EXISTS `raw.raw_pos_customer_loyalty` (
  `ingested_at` TIMESTAMP,
  `customer_id` STRING NOT NULL,
  `name` STRING,
  `email` STRING,
  `contact_info` STRING,
  `points_balance` NUMERIC,
  `last_purchase_date` DATE,
  `load_at` TIMESTAMP,
  `load_id` STRING,
  `source_file` STRING,
  `source_ts` TIMESTAMP,
  `ingestion_uuid` STRING
)
PARTITION BY DATE(`ingested_at`)
OPTIONS (
  description = "Source table raw_pos_customer_loyalty from raw_pos",
  require_partition_filter = true
);

-- Source: raw_pos
-- POS purchase orders data.
CREATE TABLE IF NOT EXISTS `raw.raw_pos_purchase_orders` (
  `ingested_at` TIMESTAMP,
  `order_id` STRING NOT NULL,
  `supplier_id` STRING,
  `order_date` DATE,
  `product_id` STRING NOT NULL,
  `quantity` INT64,
  `delivery_date` DATE,
  `load_at` TIMESTAMP,
  `load_id` STRING,
  `source_file` STRING,
  `source_ts` TIMESTAMP,
  `ingestion_uuid` STRING
)
PARTITION BY DATE(`ingested_at`)
OPTIONS (
  description = "Source table raw_pos_purchase_orders from raw_pos",
  require_partition_filter = true
);

-- Source: raw_pos
-- POS discounts data.
CREATE TABLE IF NOT EXISTS `raw.raw_pos_discounts` (
  `ingested_at` TIMESTAMP,
  `discount_id` STRING NOT NULL,
  `receipt_id` STRING,
  `type` STRING,
  `value` NUMERIC,
  `applied_date` DATE,
  `load_at` TIMESTAMP,
  `load_id` STRING,
  `source_file` STRING,
  `source_ts` TIMESTAMP,
  `ingestion_uuid` STRING
)
PARTITION BY DATE(`ingested_at`)
CLUSTER BY `discount_id`, `receipt_id`
OPTIONS (
  description = "Source table raw_pos_discounts from raw_pos",
  require_partition_filter = true
);

-- Source: raw_pos
-- POS product details
CREATE TABLE IF NOT EXISTS `raw.raw_pos_product` (
  `ingested_at` TIMESTAMP,
  `product_id` STRING NOT NULL,
  `sku` STRING,
  `product_name` STRING,
  `category_id` STRING NOT NULL,
  `description` STRING,
  `unit_price` NUMERIC,
  `weight` FLOAT64,
  `dimensions` STRING,
  `created_at` TIMESTAMP,
  `updated_at` TIMESTAMP,
  `status` STRING,
  `load_at` TIMESTAMP,
  `load_id` STRING,
  `source_file` STRING,
  `source_ts` TIMESTAMP,
  `ingestion_uuid` STRING
)
PARTITION BY DATE(`ingested_at`)
OPTIONS (
  description = "Source table raw_pos_product from raw_pos",
  require_partition_filter = true
);

-- Source: raw_wholesale
CREATE TABLE IF NOT EXISTS `raw.raw_wholesale_account` (
  `ingested_at` TIMESTAMP,
  `org_account_id` STRING NOT NULL,
  `account_name` STRING,
  `billing_terms` STRING,
  `credit_limit` NUMERIC,
  `load_at` TIMESTAMP,
  `load_id` STRING,
  `source_file` STRING,
  `source_ts` TIMESTAMP,
  `ingestion_uuid` STRING
)
PARTITION BY DATE(`ingested_at`)
OPTIONS (
  description = "Source table raw_wholesale_account from raw_wholesale",
  require_partition_filter = true
);

-- Source: raw_wholesale
CREATE TABLE IF NOT EXISTS `raw.raw_wholesale_contract` (
  `ingested_at` TIMESTAMP,
  `contract_id` STRING NOT NULL,
  `org_account_id` STRING,
  `start_date` DATE,
  `discount_type` STRING,
  `discount_value` INT64,
  `load_at` TIMESTAMP,
  `load_id` STRING,
  `source_file` STRING,
  `source_ts` TIMESTAMP,
  `ingestion_uuid` STRING
)
PARTITION BY DATE(`ingested_at`)
OPTIONS (
  description = "Source table raw_wholesale_contract from raw_wholesale",
  require_partition_filter = true
);

-- Source: raw_wholesale
CREATE TABLE IF NOT EXISTS `raw.raw_wholesale_price_list` (
  `ingested_at` TIMESTAMP,
  `price_list_id` STRING NOT NULL,
  `product_id` STRING,
  `currency` STRING,
  `contract_price` NUMERIC,
  `effective_from` DATE,
  `load_at` TIMESTAMP,
  `load_id` STRING,
  `source_file` STRING,
  `source_ts` TIMESTAMP,
  `ingestion_uuid` STRING
)
PARTITION BY DATE(`ingested_at`)
OPTIONS (
  description = "Source table raw_wholesale_price_list from raw_wholesale",
  require_partition_filter = true
);

-- Source: raw_wholesale
CREATE TABLE IF NOT EXISTS `raw.raw_wholesale_order` (
  `order_id` STRING NOT NULL,
  `order_number` STRING,
  `org_account_id` STRING,
  `order_ts` TIMESTAMP,
  `status` STRING,
  `currency` STRING,
  `load_at` TIMESTAMP,
  `load_id` STRING,
  `source_file` STRING,
  `source_ts` TIMESTAMP,
  `ingestion_uuid` STRING
)
PARTITION BY DATE(`order_ts`)
OPTIONS (
  description = "Source table raw_wholesale_order from raw_wholesale",
  require_partition_filter = true
);

-- Source: raw_wholesale
CREATE TABLE IF NOT EXISTS `raw.raw_wholesale_order_line` (
  `ingested_at` TIMESTAMP,
  `order_line_id` STRING NOT NULL,
  `order_id` STRING NOT NULL,
  `product_id` STRING,
  `quantity` INT64,
  `unit_price` NUMERIC,
  `load_at` TIMESTAMP,
  `load_id` STRING,
  `source_file` STRING,
  `source_ts` TIMESTAMP,
  `ingestion_uuid` STRING
)
PARTITION BY DATE(`ingested_at`)
CLUSTER BY `order_id`
OPTIONS (
  description = "Source table raw_wholesale_order_line from raw_wholesale",
  require_partition_filter = true
);

-- Source: raw_wholesale
CREATE TABLE IF NOT EXISTS `raw.raw_wholesale_shipment_item` (
  `ingested_at` TIMESTAMP,
  `shipment_item_id` STRING NOT NULL,
  `shipment_id` STRING,
  `order_id` STRING,
  `ship_date` DATE,
  `quantity` INT64,
  `load_at` TIMESTAMP,
  `load_id` STRING,
  `source_file` STRING,
  `source_ts` TIMESTAMP,
  `ingestion_uuid` STRING
)
PARTITION BY DATE(`ingested_at`)
CLUSTER BY `order_id`
OPTIONS (
  description = "Source table raw_wholesale_shipment_item from raw_wholesale",
  require_partition_filter = true
);

-- Source: raw_wholesale
CREATE TABLE IF NOT EXISTS `raw.raw_wholesale_invoice` (
  `ingested_at` TIMESTAMP,
  `invoice_id` STRING NOT NULL,
  `order_id` STRING,
  `org_account_id` STRING,
  `invoice_date` DATE,
  `due_date` DATE,
  `amount_due` NUMERIC,
  `load_at` TIMESTAMP,
  `load_id` STRING,
  `source_file` STRING,
  `source_ts` TIMESTAMP,
  `ingestion_uuid` STRING
)
PARTITION BY DATE(`ingested_at`)
OPTIONS (
  description = "Source table raw_wholesale_invoice from raw_wholesale",
  require_partition_filter = true
);

-- Source: raw_wholesale
CREATE TABLE IF NOT EXISTS `raw.raw_wholesale_payment_tx` (
  `payment_tx_id` STRING NOT NULL,
  `invoice_id` STRING,
  `order_id` STRING,
  `tx_type` STRING,
  `amount` NUMERIC,
  `tx_ts` TIMESTAMP,
  `load_at` TIMESTAMP,
  `load_id` STRING,
  `source_file` STRING,
  `source_ts` TIMESTAMP,
  `ingestion_uuid` STRING
)
PARTITION BY DATE(`tx_ts`)
OPTIONS (
  description = "Source table raw_wholesale_payment_tx from raw_wholesale",
  require_partition_filter = true
);

-- Source: raw_wholesale
-- Wholesale returns data.
CREATE TABLE IF NOT EXISTS `raw.raw_wholesale_returns` (
  `ingested_at` TIMESTAMP,
  `return_id` STRING NOT NULL,
  `order_id` STRING,
  `product_id` STRING,
  `quantity` INT64,
  `return_date` DATE,
  `reason` STRING,
  `amount` NUMERIC,
  `load_at` TIMESTAMP,
  `load_id` STRING,
  `source_file` STRING,
  `source_ts` TIMESTAMP,
  `ingestion_uuid` STRING
)
PARTITION BY DATE(`ingested_at`)
OPTIONS (
  description = "Source table raw_wholesale_returns from raw_wholesale",
  require_partition_filter = true
);

-- Source: raw_wholesale
-- Wholesale purchase orders data.
CREATE TABLE IF NOT EXISTS `raw.raw_wholesale_purchase_orders` (
  `ingested_at` TIMESTAMP,
  `po_id` STRING NOT NULL,
  `supplier_id` STRING,
  `order_date` DATE,
  `product_id` STRING,
  `quantity` INT64,
  `expected_delivery` STRING,
  `status` STRING,
  `load_at` TIMESTAMP,
  `load_id` STRING,
  `source_file` STRING,
  `source_ts` TIMESTAMP,
  `ingestion_uuid` STRING
)
PARTITION BY DATE(`ingested_at`)
OPTIONS (
  description = "Source table raw_wholesale_purchase_orders from raw_wholesale",
  require_partition_filter = true
);

-- Source: raw_wholesale
-- Wholesale quotes data.
CREATE TABLE IF NOT EXISTS `raw.raw_wholesale_quotes` (
  `ingested_at` TIMESTAMP,
  `quote_id` STRING NOT NULL,
  `org_account_id` STRING,
  `quote_date` DATE,
  `product_id` STRING,
  `quantity` INT64,
  `unit_price` NUMERIC,
  `expiration_date` DATE,
  `load_at` TIMESTAMP,
  `load_id` STRING,
  `source_file` STRING,
  `source_ts` TIMESTAMP,
  `ingestion_uuid` STRING
)
PARTITION BY DATE(`ingested_at`)
OPTIONS (
  description = "Source table raw_wholesale_quotes from raw_wholesale",
  require_partition_filter = true
);

-- Source: raw_wholesale
-- Wholesale inventory transactions data.
CREATE TABLE IF NOT EXISTS `raw.raw_wholesale_inventory_transactions` (
  `ingested_at` TIMESTAMP,
  `tx_id` STRING NOT NULL,
  `product_id` STRING,
  `location_id` STRING,
  `quantity` INT64,
  `tx_type` STRING,
  `tx_date` DATE,
  `load_at` TIMESTAMP,
  `load_id` STRING,
  `source_file` STRING,
  `source_ts` TIMESTAMP,
  `ingestion_uuid` STRING
)
PARTITION BY DATE(`ingested_at`)
CLUSTER BY `product_id`, `location_id`
OPTIONS (
  description = "Source table raw_wholesale_inventory_transactions from raw_wholesale",
  require_partition_filter = true
);

-- Source: raw_wholesale
-- Wholesale warehouse data.
CREATE TABLE IF NOT EXISTS `raw.raw_wholesale_warehouse` (
  `ingested_at` TIMESTAMP,
  `warehouse_id` STRING NOT NULL,
  `name` STRING,
  `address` STRING,
  `capacity` STRING,
  `current_utilization` STRING,
  `load_at` TIMESTAMP,
  `load_id` STRING,
  `source_file` STRING,
  `source_ts` TIMESTAMP,
  `ingestion_uuid` STRING
)
PARTITION BY DATE(`ingested_at`)
OPTIONS (
  description = "Source table raw_wholesale_warehouse from raw_wholesale",
  require_partition_filter = true
);

-- Source: raw_common
CREATE TABLE IF NOT EXISTS `raw.raw_brand` (
  `ingested_at` TIMESTAMP,
  `brand_id` STRING,
  `brand_name` STRING,
  `load_at` TIMESTAMP,
  `load_id` STRING,
  `source_file` STRING,
  `source_ts` TIMESTAMP,
  `ingestion_uuid` STRING
)
PARTITION BY DATE(`ingested_at`)
CLUSTER BY `brand_id`
OPTIONS (
  description = "Source table raw_brand from raw_common",
  require_partition_filter = true
);

-- Source: raw_common
CREATE TABLE IF NOT EXISTS `raw.raw_supplier` (
  `ingested_at` TIMESTAMP,
  `supplier_id` STRING,
  `supplier_name` STRING,
  `country` STRING,
  `load_at` TIMESTAMP,
  `load_id` STRING,
  `source_file` STRING,
  `source_ts` TIMESTAMP,
  `ingestion_uuid` STRING
)
PARTITION BY DATE(`ingested_at`)
CLUSTER BY `supplier_id`
OPTIONS (
  description = "Source table raw_supplier from raw_common",
  require_partition_filter = true
);

-- Source: raw_common
CREATE TABLE IF NOT EXISTS `raw.raw_campaign` (
  `ingested_at` TIMESTAMP,
  `campaign_id` STRING,
  `name` STRING,
  `channel` STRING,
  `start_ts` TIMESTAMP,
  `end_ts` TIMESTAMP,
  `load_at` TIMESTAMP,
  `load_id` STRING,
  `source_file` STRING,
  `source_ts` TIMESTAMP,
  `ingestion_uuid` STRING
)
PARTITION BY DATE(`ingested_at`)
CLUSTER BY `campaign_id`
OPTIONS (
  description = "Source table raw_campaign from raw_common",
  require_partition_filter = true
);

-- Source: raw_common
CREATE TABLE IF NOT EXISTS `raw.raw_promotion` (
  `ingested_at` TIMESTAMP,
  `promotion_id` STRING,
  `promo_code` STRING,
  `promo_type` STRING,
  `discount_type` STRING,
  `discount_value` INT64,
  `start_ts` TIMESTAMP,
  `end_ts` TIMESTAMP,
  `load_at` TIMESTAMP,
  `load_id` STRING,
  `source_file` STRING,
  `source_ts` TIMESTAMP,
  `ingestion_uuid` STRING
)
PARTITION BY DATE(`ingested_at`)
CLUSTER BY `promotion_id`
OPTIONS (
  description = "Source table raw_promotion from raw_common",
  require_partition_filter = true
);

-- Source: raw_common
CREATE TABLE IF NOT EXISTS `raw.raw_fx_rate` (
  `ingested_at` TIMESTAMP,
  `from_currency` STRING,
  `to_currency` STRING,
  `rate` NUMERIC,
  `valid_from` DATE,
  `valid_to` DATE,
  `load_at` TIMESTAMP,
  `load_id` STRING,
  `source_file` STRING,
  `source_ts` TIMESTAMP,
  `ingestion_uuid` STRING
)
PARTITION BY DATE(`ingested_at`)
OPTIONS (
  description = "Source table raw_fx_rate from raw_common",
  require_partition_filter = true
);

-- Source: raw_common
CREATE TABLE IF NOT EXISTS `raw.raw_tax_rule` (
  `ingested_at` TIMESTAMP,
  `jurisdiction` STRING,
  `tax_category` STRING,
  `rate` NUMERIC,
  `valid_from` DATE,
  `valid_to` DATE,
  `load_at` TIMESTAMP,
  `load_id` STRING,
  `source_file` STRING,
  `source_ts` TIMESTAMP,
  `ingestion_uuid` STRING
)
PARTITION BY DATE(`ingested_at`)
OPTIONS (
  description = "Source table raw_tax_rule from raw_common",
  require_partition_filter = true
);
ALTER TABLE
  `raw.raw_amazon_order`
ADD PRIMARY KEY
  (`amazon_order_id`) NOT ENFORCED;
ALTER TABLE
  `raw.raw_amazon_order_item`
ADD PRIMARY KEY
  (`order_item_id`) NOT ENFORCED;
ALTER TABLE
  `raw.raw_amazon_shipment_item`
ADD PRIMARY KEY
  (`shipment_item_id`) NOT ENFORCED;
ALTER TABLE
  `raw.raw_amazon_reimbursements`
ADD PRIMARY KEY
  (`reimbursement_id`) NOT ENFORCED;
ALTER TABLE
  `raw.raw_shopify_order`
ADD PRIMARY KEY
  (`id`) NOT ENFORCED;
ALTER TABLE
  `raw.raw_shopify_order_line`
ADD PRIMARY KEY
  (`id`) NOT ENFORCED;
ALTER TABLE
  `raw.raw_shopify_fulfillment`
ADD PRIMARY KEY
  (`id`) NOT ENFORCED;
ALTER TABLE
  `raw.raw_shopify_transaction`
ADD PRIMARY KEY
  (`id`) NOT ENFORCED;
ALTER TABLE
  `raw.raw_shopify_customer`
ADD PRIMARY KEY
  (`id`) NOT ENFORCED;
ALTER TABLE
  `raw.raw_shopify_product`
ADD PRIMARY KEY
  (`id`) NOT ENFORCED;
ALTER TABLE
  `raw.raw_shopify_refunds`
ADD PRIMARY KEY
  (`id`) NOT ENFORCED;
ALTER TABLE
  `raw.raw_shopify_discount_codes`
ADD PRIMARY KEY
  (`id`) NOT ENFORCED;
ALTER TABLE
  `raw.raw_shopify_abandoned_checkouts`
ADD PRIMARY KEY
  (`id`) NOT ENFORCED;
ALTER TABLE
  `raw.raw_shopify_locations`
ADD PRIMARY KEY
  (`id`) NOT ENFORCED;
ALTER TABLE
  `raw.raw_pos_receipt`
ADD PRIMARY KEY
  (`receipt_id`) NOT ENFORCED;
ALTER TABLE
  `raw.raw_pos_receipt_line`
ADD PRIMARY KEY
  (`receipt_line_id`) NOT ENFORCED;
ALTER TABLE
  `raw.raw_pos_tender`
ADD PRIMARY KEY
  (`tender_id`) NOT ENFORCED;
ALTER TABLE
  `raw.raw_pos_store`
ADD PRIMARY KEY
  (`store_id`) NOT ENFORCED;
ALTER TABLE
  `raw.raw_pos_returns`
ADD PRIMARY KEY
  (`return_id`) NOT ENFORCED;
ALTER TABLE
  `raw.raw_pos_product_category`
ADD PRIMARY KEY
  (`category_id`) NOT ENFORCED;
ALTER TABLE
  `raw.raw_pos_customer_loyalty`
ADD PRIMARY KEY
  (`customer_id`) NOT ENFORCED;
ALTER TABLE
  `raw.raw_pos_purchase_orders`
ADD PRIMARY KEY
  (`order_id`) NOT ENFORCED;
ALTER TABLE
  `raw.raw_pos_product`
ADD PRIMARY KEY
  (`product_id`) NOT ENFORCED;
ALTER TABLE
  `raw.raw_wholesale_account`
ADD PRIMARY KEY
  (`org_account_id`) NOT ENFORCED;
ALTER TABLE
  `raw.raw_wholesale_contract`
ADD PRIMARY KEY
  (`contract_id`) NOT ENFORCED;
ALTER TABLE
  `raw.raw_wholesale_price_list`
ADD PRIMARY KEY
  (`price_list_id`) NOT ENFORCED;
ALTER TABLE
  `raw.raw_wholesale_order`
ADD PRIMARY KEY
  (`order_id`) NOT ENFORCED;
ALTER TABLE
  `raw.raw_wholesale_order_line`
ADD PRIMARY KEY
  (`order_line_id`) NOT ENFORCED;
ALTER TABLE
  `raw.raw_wholesale_shipment_item`
ADD PRIMARY KEY
  (`shipment_item_id`) NOT ENFORCED;
ALTER TABLE
  `raw.raw_wholesale_invoice`
ADD PRIMARY KEY
  (`invoice_id`) NOT ENFORCED;
ALTER TABLE
  `raw.raw_wholesale_payment_tx`
ADD PRIMARY KEY
  (`payment_tx_id`) NOT ENFORCED;
ALTER TABLE
  `raw.raw_wholesale_returns`
ADD PRIMARY KEY
  (`return_id`) NOT ENFORCED;
ALTER TABLE
  `raw.raw_wholesale_purchase_orders`
ADD PRIMARY KEY
  (`po_id`) NOT ENFORCED;
ALTER TABLE
  `raw.raw_wholesale_quotes`
ADD PRIMARY KEY
  (`quote_id`) NOT ENFORCED;
ALTER TABLE
  `raw.raw_wholesale_inventory_transactions`
ADD PRIMARY KEY
  (`tx_id`) NOT ENFORCED;
ALTER TABLE
  `raw.raw_wholesale_warehouse`
ADD PRIMARY KEY
  (`warehouse_id`) NOT ENFORCED;
ALTER TABLE
  `raw.raw_amazon_order_item`
ADD FOREIGN KEY
  (`amazon_order_id`)
REFERENCES
  `raw.raw_amazon_order`(`amazon_order_id`) NOT ENFORCED;
ALTER TABLE
  `raw.raw_amazon_shipment_item`
ADD FOREIGN KEY
  (`amazon_order_id`)
REFERENCES
  `raw.raw_amazon_order`(`amazon_order_id`) NOT ENFORCED;
ALTER TABLE
  `raw.raw_amazon_settlement_tx`
ADD FOREIGN KEY
  (`amazon_order_id`)
REFERENCES
  `raw.raw_amazon_order`(`amazon_order_id`) NOT ENFORCED;
ALTER TABLE
  `raw.raw_amazon_returns`
ADD FOREIGN KEY
  (`amazon_order_id`)
REFERENCES
  `raw.raw_amazon_order`(`amazon_order_id`) NOT ENFORCED;
ALTER TABLE
  `raw.raw_amazon_reimbursements`
ADD FOREIGN KEY
  (`amazon_order_id`)
REFERENCES
  `raw.raw_amazon_order`(`amazon_order_id`) NOT ENFORCED;
ALTER TABLE
  `raw.raw_shopify_order`
ADD FOREIGN KEY
  (`id`)
REFERENCES
  `raw.raw_shopify_order_line`(`id`) NOT ENFORCED;
ALTER TABLE
  `raw.raw_shopify_order`
ADD FOREIGN KEY
  (`customer_id`)
REFERENCES
  `raw.raw_shopify_customer`(`id`) NOT ENFORCED;
ALTER TABLE
  `raw.raw_shopify_order_line`
ADD FOREIGN KEY
  (`order_id`)
REFERENCES
  `raw.raw_shopify_order`(`id`) NOT ENFORCED;
ALTER TABLE
  `raw.raw_shopify_order_line`
ADD FOREIGN KEY
  (`id`)
REFERENCES
  `raw.raw_shopify_fulfillment`(`id`) NOT ENFORCED;
ALTER TABLE
  `raw.raw_shopify_order_line`
ADD FOREIGN KEY
  (`product_id`)
REFERENCES
  `raw.raw_shopify_product`(`id`) NOT ENFORCED;
ALTER TABLE
  `raw.raw_shopify_fulfillment`
ADD FOREIGN KEY
  (`order_id`)
REFERENCES
  `raw.raw_shopify_order`(`id`) NOT ENFORCED;
ALTER TABLE
  `raw.raw_shopify_fulfillment`
ADD FOREIGN KEY
  (`id`)
REFERENCES
  `raw.raw_shopify_order_line`(`id`) NOT ENFORCED;
ALTER TABLE
  `raw.raw_shopify_transaction`
ADD FOREIGN KEY
  (`order_id`)
REFERENCES
  `raw.raw_shopify_order`(`id`) NOT ENFORCED;
ALTER TABLE
  `raw.raw_shopify_transaction`
ADD FOREIGN KEY
  (`id`)
REFERENCES
  `raw.raw_shopify_order_line`(`id`) NOT ENFORCED;
ALTER TABLE
  `raw.raw_shopify_customer`
ADD FOREIGN KEY
  (`id`)
REFERENCES
  `raw.raw_shopify_order`(`id`) NOT ENFORCED;
ALTER TABLE
  `raw.raw_shopify_product`
ADD FOREIGN KEY
  (`id`)
REFERENCES
  `raw.raw_shopify_order`(`id`) NOT ENFORCED;
ALTER TABLE
  `raw.raw_shopify_refunds`
ADD FOREIGN KEY
  (`order_id`)
REFERENCES
  `raw.raw_shopify_order`(`id`) NOT ENFORCED;
ALTER TABLE
  `raw.raw_shopify_refunds`
ADD FOREIGN KEY
  (`id`)
REFERENCES
  `raw.raw_shopify_order_line`(`id`) NOT ENFORCED;
ALTER TABLE
  `raw.raw_shopify_discount_codes`
ADD FOREIGN KEY
  (`id`)
REFERENCES
  `raw.raw_shopify_order`(`id`) NOT ENFORCED;
ALTER TABLE
  `raw.raw_shopify_abandoned_checkouts`
ADD FOREIGN KEY
  (`id`)
REFERENCES
  `raw.raw_shopify_order`(`id`) NOT ENFORCED;
ALTER TABLE
  `raw.raw_shopify_locations`
ADD FOREIGN KEY
  (`id`)
REFERENCES
  `raw.raw_shopify_order`(`id`) NOT ENFORCED;
ALTER TABLE
  `raw.raw_pos_receipt`
ADD FOREIGN KEY
  (`store_id`)
REFERENCES
  `raw.raw_pos_store`(`store_id`) NOT ENFORCED;
ALTER TABLE
  `raw.raw_pos_receipt_line`
ADD FOREIGN KEY
  (`receipt_id`)
REFERENCES
  `raw.raw_pos_receipt`(`receipt_id`) NOT ENFORCED;
ALTER TABLE
  `raw.raw_pos_receipt_line`
ADD FOREIGN KEY
  (`product_id`)
REFERENCES
  `raw.raw_pos_product`(`product_id`) NOT ENFORCED;
ALTER TABLE
  `raw.raw_pos_tender`
ADD FOREIGN KEY
  (`receipt_id`)
REFERENCES
  `raw.raw_pos_receipt`(`receipt_id`) NOT ENFORCED;
ALTER TABLE
  `raw.raw_pos_inventory_daily`
ADD FOREIGN KEY
  (`store_id`)
REFERENCES
  `raw.raw_pos_store`(`store_id`) NOT ENFORCED;
ALTER TABLE
  `raw.raw_pos_inventory_daily`
ADD FOREIGN KEY
  (`product_id`)
REFERENCES
  `raw.raw_pos_product`(`product_id`) NOT ENFORCED;
ALTER TABLE
  `raw.raw_pos_returns`
ADD FOREIGN KEY
  (`receipt_id`)
REFERENCES
  `raw.raw_pos_receipt`(`receipt_id`) NOT ENFORCED;
ALTER TABLE
  `raw.raw_pos_returns`
ADD FOREIGN KEY
  (`product_id`)
REFERENCES
  `raw.raw_pos_product`(`product_id`) NOT ENFORCED;
ALTER TABLE
  `raw.raw_pos_purchase_orders`
ADD FOREIGN KEY
  (`product_id`)
REFERENCES
  `raw.raw_pos_product`(`product_id`) NOT ENFORCED;
ALTER TABLE
  `raw.raw_pos_discounts`
ADD FOREIGN KEY
  (`receipt_id`)
REFERENCES
  `raw.raw_pos_receipt`(`receipt_id`) NOT ENFORCED;
ALTER TABLE
  `raw.raw_pos_product`
ADD FOREIGN KEY
  (`category_id`)
REFERENCES
  `raw.raw_pos_product_category`(`category_id`) NOT ENFORCED;
ALTER TABLE
  `raw.raw_wholesale_contract`
ADD FOREIGN KEY
  (`org_account_id`)
REFERENCES
  `raw.raw_wholesale_account`(`org_account_id`) NOT ENFORCED;
ALTER TABLE
  `raw.raw_wholesale_order`
ADD FOREIGN KEY
  (`org_account_id`)
REFERENCES
  `raw.raw_wholesale_account`(`org_account_id`) NOT ENFORCED;
ALTER TABLE
  `raw.raw_wholesale_order_line`
ADD FOREIGN KEY
  (`order_id`)
REFERENCES
  `raw.raw_wholesale_order`(`order_id`) NOT ENFORCED;
ALTER TABLE
  `raw.raw_wholesale_shipment_item`
ADD FOREIGN KEY
  (`order_id`)
REFERENCES
  `raw.raw_wholesale_order`(`order_id`) NOT ENFORCED;
ALTER TABLE
  `raw.raw_wholesale_invoice`
ADD FOREIGN KEY
  (`org_account_id`)
REFERENCES
  `raw.raw_wholesale_account`(`org_account_id`) NOT ENFORCED;
ALTER TABLE
  `raw.raw_wholesale_invoice`
ADD FOREIGN KEY
  (`order_id`)
REFERENCES
  `raw.raw_wholesale_order`(`order_id`) NOT ENFORCED;
ALTER TABLE
  `raw.raw_wholesale_payment_tx`
ADD FOREIGN KEY
  (`order_id`)
REFERENCES
  `raw.raw_wholesale_order`(`order_id`) NOT ENFORCED;
ALTER TABLE
  `raw.raw_wholesale_payment_tx`
ADD FOREIGN KEY
  (`invoice_id`)
REFERENCES
  `raw.raw_wholesale_invoice`(`invoice_id`) NOT ENFORCED;
ALTER TABLE
  `raw.raw_wholesale_returns`
ADD FOREIGN KEY
  (`order_id`)
REFERENCES
  `raw.raw_wholesale_order`(`order_id`) NOT ENFORCED;
ALTER TABLE
  `raw.raw_wholesale_quotes`
ADD FOREIGN KEY
  (`org_account_id`)
REFERENCES
  `raw.raw_wholesale_account`(`org_account_id`) NOT ENFORCED;
ALTER TABLE
  `raw.raw_wholesale_inventory_transactions`
ADD FOREIGN KEY
  (`tx_id`)
REFERENCES
  `raw.raw_wholesale_payment_tx`(`payment_tx_id`) NOT ENFORCED;
