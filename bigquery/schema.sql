-- Dataset e tabelle BigQuery per il motore di riconciliazione.
-- Le colonne sono NULLABLE perché i Parquet scritti da Spark sono nullable
-- e BigQuery rifiuta il load su colonne REQUIRED. La qualità dei dati la garantisce Spark.

CREATE SCHEMA IF NOT EXISTS recon OPTIONS (location = 'EU');

-- Righe abbinate A↔B per id, con valori validi (anche se diversi tra loro).
-- status: MATCHED | AMOUNT_BREAK | DATE_BREAK | CLIENT_BREAK | CARD_BREAK | MERCHANT_BREAK
--         | MULTI_BREAK (più di una colonna diversa: non dovrebbe capitare, 1 errore per riga)
CREATE TABLE IF NOT EXISTS recon.reconciled (
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
PARTITION BY DATE_TRUNC(date_a, MONTH)  -- mensile: 10 anni giornalieri ≈ 3600 partizioni, troppo vicino al limite di 4000
CLUSTER BY status;

-- Righe orfane o invalide. Valori raw come STRING perché possono non essere parsabili.
-- reason: MISSING_IN_A | MISSING_IN_B | DUPLICATE_ID | NULL_VALUE | UNPARSEABLE | OUT_OF_DOMAIN | CARD_CLIENT_MISMATCH
-- Il lato di provenienza degli orfani è indicato da reason (MISSING_IN_A / MISSING_IN_B).
CREATE TABLE IF NOT EXISTS recon.quarantine (
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
  reason_column STRING,   -- colonna che ha causato la quarantena (null per MISSING_IN_*)
  run_id        STRING,
  loaded_at     TIMESTAMP
)
CLUSTER BY reason;
