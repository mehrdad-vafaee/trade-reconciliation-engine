"""Riconcilia feed A e B.

1. Legge i due feed come stringhe, converte i tipi e valida ogni campo.
2. FULL OUTER JOIN su id. Le righe senza controparte vanno in quarantena.
3. Le righe abbinate con un valore invalido vanno in quarantena, le altre vengono classificate.
Scrive reconciled/ e quarantine/ in Parquet (un file ciascuno, per il load su BigQuery).
"""
import json
import os
import sys
from datetime import datetime, timezone
from decimal import Decimal

from pyspark import StorageLevel
from pyspark.sql import SparkSession, Window, functions as F

RAW, OUT = "/opt/data/raw", "/opt/data/output"
FIELDS = ["date", "client_id", "card_id", "amount", "merchant_id"]
ID_RANGES = {"client_id": (0, 1998), "card_id": (0, 6144), "merchant_id": (0, 100342)}  # dal profiling di A
MIN_DATE = "2010-01-01"
MAX_AMOUNT = 50_000
DATE_TOLERANCE_SEC = 60
AMOUNT_TOLERANCE = Decimal("0.01")

spark = (SparkSession.builder.appName("reconcile")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")  # INT96 non serve a BigQuery
         .getOrCreate())


def load(side):
    """Feed tipizzato + valori raw + primo problema trovato (struct reason/column, null se la riga è valida)."""
    raw = spark.read.csv(f"{RAW}/feed_{side}", header=True)
    typed = {
        "id": F.col("id").try_cast("bigint"),
        "date": F.try_to_timestamp("date", F.lit("yyyy-MM-dd HH:mm:ss")),
        "amount": F.regexp_replace("amount", r"^\$", "").try_cast("decimal(12,2)"),
        **{c: F.col(c).try_cast("bigint") for c in ID_RANGES},
    }
    valid = {
        "id": typed["id"] > 0,
        "date": typed["date"].between(F.lit(MIN_DATE).cast("timestamp"), F.current_timestamp()),
        "amount": F.abs(typed["amount"]) <= MAX_AMOUNT,
        **{c: typed[c].between(lo, hi) for c, (lo, hi) in ID_RANGES.items()},
    }

    def problem(c):
        return (F.when(F.col(c).isNull(), "NULL_VALUE")
                .when(typed[c].isNull(), "UNPARSEABLE")
                .when(~valid[c], "OUT_OF_DOMAIN"))

    duplicate = F.when(F.count("*").over(Window.partitionBy("id")) > 1, "DUPLICATE_ID")
    checks = [(problem("id"), "id"), (duplicate, "id")] + [(problem(c), c) for c in FIELDS]
    first = F.coalesce(*[F.when(r.isNotNull(), F.struct(r.alias("reason"), F.lit(f"{c}_{side}").alias("column")))
                         for r, c in checks])
    return raw.select(*[F.col(c).alias(f"raw_{c}_{side}") for c in ["id"] + FIELDS],
                      *[typed[c].alias(f"{c}_{side}") for c in ["id"] + FIELDS],
                      first.alias(f"issue_{side}"))


def bad_id(side):  # id nullo, invalido o duplicato: la riga non può partecipare al join
    return F.coalesce(F.col(f"issue_{side}.column") == f"id_{side}", F.lit(False))


def one_sided(df, side):
    return df.where(bad_id(side)).select("*", F.col(f"issue_{side}.reason").alias("reason"),
                                         F.col(f"issue_{side}.column").alias("reason_column"))


a, b = load("a"), load("b")
both = F.col("id_a").isNotNull() & F.col("id_b").isNotNull()
run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

rows = (a.where(~bad_id("a"))
        .join(b.where(~bad_id("b")), F.col("id_a") == F.col("id_b"), "full_outer")
        .withColumn("reason", F.when(F.col("id_b").isNull(), "MISSING_IN_B")
                    .when(F.col("id_a").isNull(), "MISSING_IN_A")
                    .otherwise(F.coalesce(F.col("issue_a.reason"), F.col("issue_b.reason"))))
        .withColumn("reason_column", F.when(both, F.coalesce(F.col("issue_a.column"), F.col("issue_b.column"))))
        .unionByName(one_sided(a, "a"), allowMissingColumns=True)
        .unionByName(one_sided(b, "b"), allowMissingColumns=True)
        .withColumn("run_id", F.lit(run_id))
        .withColumn("loaded_at", F.current_timestamp())
        .persist(StorageLevel.DISK_ONLY))

breaks = {
    "DATE_BREAK": F.abs(F.unix_timestamp("date_a") - F.unix_timestamp("date_b")) > DATE_TOLERANCE_SEC,
    "AMOUNT_BREAK": F.abs(F.col("amount_a") - F.col("amount_b")) > F.lit(AMOUNT_TOLERANCE),
    "CLIENT_BREAK": F.col("client_id_a") != F.col("client_id_b"),
    "CARD_BREAK": F.col("card_id_a") != F.col("card_id_b"),
    "MERCHANT_BREAK": F.col("merchant_id_a") != F.col("merchant_id_b"),
}
n_breaks = sum(c.cast("int") for c in breaks.values())
status = F.when(n_breaks == 0, "MATCHED").when(n_breaks > 1, "MULTI_BREAK")
for name, cond in breaks.items():
    status = status.when(cond, name)

pairs = [f"{c}_{s}" for c in FIELDS for s in "ab"]
(rows.where(F.col("reason").isNull())
 .select(F.col("id_a").alias("id"), *pairs, status.alias("status"), "run_id", "loaded_at")
 .repartition(1).write.mode("overwrite").parquet(f"{OUT}/reconciled"))
(rows.where(F.col("reason").isNotNull())
 .select(F.coalesce("raw_id_a", "raw_id_b").alias("id"), *[F.col(f"raw_{p}").alias(p) for p in pairs],
         "reason", "reason_column", "run_id", "loaded_at")
 .repartition(1).write.mode("overwrite").parquet(f"{OUT}/quarantine"))

# Verifica: i conteggi per status/reason devono coincidere con quelli attesi dal generatore.
actual = {r[0]: r[1] for t, c in (("reconciled", "status"), ("quarantine", "reason"))
          for r in spark.read.parquet(f"{OUT}/{t}").groupBy(c).count().collect()}
print(f"run_id {run_id}")
expected_path = f"{RAW}/expected_counts.json"
expected = json.load(open(expected_path))["expected"] if os.path.exists(expected_path) else {}
print(f"{'esito':<16}{'trovati':>12}{'attesi':>12}")
for label in sorted(actual.keys() | expected.keys()):
    ok = "" if not expected or actual.get(label) == expected.get(label) else "  <-- DIVERSO"
    print(f"{label:<16}{actual.get(label, 0):>12,}{expected.get(label, 0):>12,}{ok}")
spark.stop()
if expected and actual != expected:
    sys.exit("Conteggi diversi da quelli attesi")
