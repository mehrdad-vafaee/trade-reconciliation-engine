-- Views read by the Looker Studio dashboard.
-- Run after schema.sql, in this order (each view depends on the previous ones).

-- One row per reconciled pair, with the absolute amount difference between A and B.
CREATE OR REPLACE VIEW `trade_reconciliation_dev.fct_daily_reconciliation` AS
SELECT
  id AS trade_id,
  date_a,
  date_b,
  client_id_a,
  client_id_b,
  card_id_a,
  card_id_b,
  amount_a,
  amount_b,
  -- Calculate absolute dollar break amount
  ABS(COALESCE(amount_a, 0) - COALESCE(amount_b, 0)) AS break_amount_usd,
  merchant_id_a,
  merchant_id_b,
  status AS reconciliation_status,
  run_id,
  loaded_at
FROM `trade_reconciliation_dev.reconciled`;

-- Quarantined rows, used by the quarantine table in the dashboard.
CREATE OR REPLACE VIEW `trade_reconciliation_dev.fct_quarantine_exceptions` AS
SELECT
  id AS raw_trade_id,
  date_a,
  date_b,
  client_id_a,
  client_id_b,
  amount_a,
  amount_b,
  reason AS quarantine_reason,
  reason_column,
  run_id,
  loaded_at
FROM `trade_reconciliation_dev.quarantine`;

-- Per client: trades, breaks, break rate, exposure and breaks by type.
CREATE OR REPLACE VIEW `trade_reconciliation_dev.dim_counterparty_risk` AS
SELECT
  COALESCE(client_id_a, client_id_b) AS client_id,
  COUNT(trade_id) AS total_trades,

  -- Count matched trades vs breaks
  COUNTIF(reconciliation_status = 'MATCHED') AS matched_trades,
  COUNTIF(reconciliation_status != 'MATCHED') AS total_breaks,

  -- Calculate break rate percentage
  ROUND(
    SAFE_DIVIDE(COUNTIF(reconciliation_status != 'MATCHED'), COUNT(trade_id)) * 100,
    2
  ) AS break_rate_pct,

  -- Total financial exposure ($)
  SUM(break_amount_usd) AS total_exposure_usd,

  -- Break breakdown by type
  COUNTIF(reconciliation_status = 'AMOUNT_BREAK') AS amount_breaks,
  COUNTIF(reconciliation_status = 'DATE_BREAK') AS date_breaks,
  COUNTIF(reconciliation_status = 'CLIENT_BREAK') AS client_breaks,
  COUNTIF(reconciliation_status = 'CARD_BREAK') AS card_breaks,
  COUNTIF(reconciliation_status = 'MERCHANT_BREAK') AS merchant_breaks,
  COUNTIF(reconciliation_status = 'MULTI_BREAK') AS multi_breaks

FROM `trade_reconciliation_dev.fct_daily_reconciliation`
GROUP BY client_id;

-- Number of breaks per type, for the pie chart.
CREATE OR REPLACE VIEW `trade_reconciliation_dev.v_break_type_distribution` AS
SELECT 'Amount Break' AS break_type, SUM(amount_breaks) AS break_count FROM `trade_reconciliation_dev.dim_counterparty_risk`
UNION ALL
SELECT 'Date Break' AS break_type, SUM(date_breaks) AS break_count FROM `trade_reconciliation_dev.dim_counterparty_risk`
UNION ALL
SELECT 'Client Break' AS break_type, SUM(client_breaks) AS break_count FROM `trade_reconciliation_dev.dim_counterparty_risk`
UNION ALL
SELECT 'Card Break' AS break_type, SUM(card_breaks) AS break_count FROM `trade_reconciliation_dev.dim_counterparty_risk`
UNION ALL
SELECT 'Merchant Break' AS break_type, SUM(merchant_breaks) AS break_count FROM `trade_reconciliation_dev.dim_counterparty_risk`
UNION ALL
SELECT 'Multi Break' AS break_type, SUM(multi_breaks) AS break_count FROM `trade_reconciliation_dev.dim_counterparty_risk`;
