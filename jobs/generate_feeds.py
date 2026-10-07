"""Simulate the two broker feeds.

A = source CSV reduced to 6 columns (clean reference).
B = copy of A with at most one error per row; the seed and the weight of each error type change on every run.
Also writes expected_counts.json: the expected outcome of every row, used by reconcile.py to check itself.
"""
import json
import random
from collections import Counter
from decimal import Decimal
from itertools import accumulate

from pyspark.sql import SparkSession, functions as F

SRC = "/opt/data/source/transactions_data-selected-columns.csv"
OUT = "/opt/data/raw"
COLS = ["id", "date", "client_id", "card_id", "amount", "merchant_id"]
SEED = random.randrange(2**31)  # saved in expected_counts.json
ERROR_RATE, ID_RATE, NULL_RATE = 0.75, 0.005, 0.02  # ID and NULL are included in the 75%

# error type -> expected outcome (status in reconciled or reason in quarantine)
VALUE_ERRORS = {
    "date_jitter": "MATCHED",                # ±1-59 s, within tolerance
    "date_shift": "DATE_BREAK",              # +2 min .. 3 h
    "date_future": "OUT_OF_DOMAIN",          # +100 years (multiple of 4: Feb 29 stays valid)
    "date_malformed": "UNPARSEABLE",         # dd/MM/yyyy HH:mm format
    "amount_rounding": "MATCHED",            # ±0.01, within tolerance
    "amount_change": "AMOUNT_BREAK",         # ±1 .. 100
    "amount_fat_finger": "OUT_OF_DOMAIN",    # +1,000,000
    "amount_malformed": "UNPARSEABLE",       # "#VALUE!"
    "client_change": "CLIENT_BREAK",         # another valid client
    "client_out_of_range": "OUT_OF_DOMAIN",
    "card_change": "CARD_BREAK",
    "card_out_of_range": "OUT_OF_DOMAIN",
    "merchant_change": "MERCHANT_BREAK",
    "merchant_out_of_range": "OUT_OF_DOMAIN",
}
NULL_ERRORS = {f"null_{c}": "NULL_VALUE" for c in COLS[1:]}
DOMAIN_SIZE = {"client_id": 1999, "card_id": 6145, "merchant_id": 100343}  # valid ids: 0 .. N-1

rng = random.Random(SEED)


def split(names, total):  # a random weight for each name, summing to total
    w = {n: rng.random() for n in names}
    return {n: x / sum(w.values()) * total for n, x in w.items()}


# probability of each error type: ID and NULL have fixed shares, the rest goes to value errors
WEIGHTS = {"id_shift": ID_RATE, **split(NULL_ERRORS, NULL_RATE),
           **split(VALUE_ERRORS, ERROR_RATE - ID_RATE - NULL_RATE)}

spark = (SparkSession.builder.appName("generate_feeds")
         .config("spark.sql.session.timeZone", "UTC").getOrCreate())

a = spark.read.csv(SRC, header=True).select(COLS)
a.write.mode("overwrite").csv(f"{OUT}/feed_a", header=True)


# Random numbers materialized as columns: each row always uses the same values.
b = a.select("*", *[F.rand(SEED + i).alias(n) for i, n in enumerate("ums")])
u, m = F.col("u"), F.col("m")

# u falls into one of the intervals [0, w1), [w1, w1+w2), ...: above ERROR_RATE the row stays clean.
names, bounds = list(WEIGHTS), list(accumulate(WEIGHTS.values()))
error = F.when(u < bounds[0], names[0])
for name, bound in zip(names[1:], bounds[1:]):
    error = error.when(u < bound, name)
b = b.withColumn("error", error)

e = F.col("error")
sign = F.when(F.col("s") < 0.5, -1).otherwise(1)
secs = F.unix_timestamp("date")
amount = F.regexp_replace("amount", r"^\$", "").cast("decimal(12,2)")


def ts(seconds):
    return F.date_format(F.timestamp_seconds(seconds), "yyyy-MM-dd HH:mm:ss")


def money(x):
    return F.concat(F.lit("$"), x.cast("decimal(12,2)").cast("string"))


def plus(c, n):
    return (F.col(c).cast("bigint") + n).cast("string")


def other(c):  # another id in the domain, always different from the original
    n = DOMAIN_SIZE[c]
    return ((F.col(c).cast("bigint") + 1 + (m * (n - 1)).cast("bigint")) % n).cast("string")


mutated = {
    "id": F.when(e == "id_shift", plus("id", 100_000_000)),  # beyond A's max: no collisions
    "date": F.when(e == "date_jitter", ts(secs + sign * (1 + (m * 59).cast("int"))))
             .when(e == "date_shift", ts(secs + 120 + (m * 10_680).cast("int")))
             .when(e == "date_future", F.concat(plus("date_year", 100), F.substring("date", 5, 15)))
             .when(e == "date_malformed", F.date_format(F.to_timestamp("date"), "dd/MM/yyyy HH:mm")),
    "amount": F.when(e == "amount_rounding", money(amount + sign * F.lit(Decimal("0.01"))))
               .when(e == "amount_change", money(amount + sign * (1 + F.round(m * 99, 2))))
               .when(e == "amount_fat_finger", money(amount + 1_000_000))
               .when(e == "amount_malformed", F.lit("#VALUE!")),
    "client_id": F.when(e == "client_change", other("client_id"))
                  .when(e == "client_out_of_range", plus("client_id", 100_000)),
    "card_id": F.when(e == "card_change", other("card_id"))
                .when(e == "card_out_of_range", plus("card_id", 1_000_000)),
    "merchant_id": F.when(e == "merchant_change", other("merchant_id"))
                    .when(e == "merchant_out_of_range", plus("merchant_id", 1_000_000)),
}

b = b.withColumn("date_year", F.substring("date", 1, 4)).cache()
b.select(*[F.when(e == f"null_{c}", F.lit(None)).otherwise(F.coalesce(mutated[c], F.col(c))).alias(c)
           for c in COLS]).write.mode("overwrite").csv(f"{OUT}/feed_b", header=True)

errors = {r["error"] or "none": r["count"] for r in b.groupBy("error").count().collect()}
outcome = {**VALUE_ERRORS, **NULL_ERRORS, "none": "MATCHED"}
expected = Counter()
for err, n in errors.items():
    for label in (["MISSING_IN_A", "MISSING_IN_B"] if err == "id_shift" else [outcome[err]]):
        expected[label] += n

with open(f"{OUT}/expected_counts.json", "w") as f:
    json.dump({"seed": SEED, "weights": WEIGHTS, "rows": sum(errors.values()), "errors": errors,
               "expected": expected}, f, indent=2)

print(f"seed {SEED}")

for err, n in sorted(errors.items()):
    print(f"{err:<24}{n:>12,}")
spark.stop()
