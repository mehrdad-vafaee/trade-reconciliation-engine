-- BigQuery dataset and tables for the reconciliation engine.
-- Columns are NULLABLE because the Parquet files written by Spark are nullable
-- and BigQuery rejects loads into REQUIRED columns. Data quality is enforced by Spark.

CREATE SCHEMA IF NOT EXISTS trade_reconciliation_dev OPTIONS (location = 'EU');

-- Rows matched A↔B by id, with valid values (even if they differ from each other).
-- status: MATCHED | AMOUNT_BREAK | DATE_BREAK | CLIENT_BREAK | CARD_BREAK | MERCHANT_BREAK
--         | MULTI_BREAK (more than one differing column: should not happen, 1 error per row)
CREATE TABLE IF NOT EXISTS trade_reconciliation_dev.reconciled (
  id            INT64,
  date_a        TIMESTAMP,
  date_b        TIMESTAMP,
  client_id_a   INT64,
  client_id_b   INT64,
  card_id_a     INT64,
  card_id_b     INT64,
  amount_a      NUMERIC,
  amount_b      NUMERIC,
  merchant_id_a INT64,
  merchant_id_b INT64,
  status        STRING,
  run_id        STRING,
  loaded_at     TIMESTAMP
)
PARTITION BY TIMESTAMP_TRUNC(date_a, MONTH)  -- monthly: 10 years of daily partitions ≈ 3600, too close to the 4000 limit
CLUSTER BY status;

-- Orphan or invalid rows. Raw values stored as STRING because they may not be parseable.
-- reason: MISSING_IN_A | MISSING_IN_B | DUPLICATE_ID | NULL_VALUE | UNPARSEABLE | OUT_OF_DOMAIN
-- The source side of orphans is given by reason (MISSING_IN_A / MISSING_IN_B).
CREATE TABLE IF NOT EXISTS trade_reconciliation_dev.quarantine (
  id            STRING,
  date_a        STRING,
  date_b        STRING,
  client_id_a   STRING,
  client_id_b   STRING,
  card_id_a     STRING,
  card_id_b     STRING,
  amount_a      STRING,
  amount_b      STRING,
  merchant_id_a STRING,
  merchant_id_b STRING,
  reason        STRING,
  reason_column STRING,   -- column that caused the quarantine (null for MISSING_IN_*)
  run_id        STRING,
  loaded_at     TIMESTAMP
)
CLUSTER BY reason;
