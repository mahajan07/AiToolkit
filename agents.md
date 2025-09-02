# Snowpark: character-based fuzzy match, no location filters
from snowflake.snowpark.functions import (
    col, upper, regexp_replace, trim, length, greatest, lit,
    call_function, when
)
from snowflake.snowpark.window import Window

# ---- configure your sources ----
DIR_TBL = '"SBOX_CONSUMERBANKING_DB"."DEPOSITS_CARDS_CHNL_PROD"."TYSONS_STORE_DIR_DM"'
DIR_COL = "BRANDS"                     # brand names column

TX_TBL  = "PTX_DB.DBO.RAW_SCH.AUTHFILE"
TX_COL  = "DAF_MERCH_NAME"             # merchant names column
TX_FILTER = "PERIODODATE >= 20250101"  # optional; or set to None

# weights / thresholds
JW_WT = 0.6         # weight for Jaro–Winkler (character similarity)
LEV_WT = 0.4        # weight for normalized Levenshtein similarity
HIGH = 0.92         # high-confidence if combined score ≥ HIGH
MED  = 0.86         # medium-confidence if combined score ≥ MED

# ---- helpers (character-only normalization) ----
def char_only(expr):
    # Uppercase and keep letters only; collapse whitespace (very light cleaning)
    s = upper(expr)
    s = regexp_replace(s, r'\s+', '')            # drop spaces
    s = regexp_replace(s, r'[^A-Z]', '')         # keep only A–Z
    return s

def prefix(expr, n=3):
    return expr.substr(lit(1), lit(n))

# ---- load & prepare ----
vd = session.table(DIR_TBL).select(
        col(DIR_COL).alias("BRAND_RAW")
    ).with_columns({
        "BRAND_CHARS": char_only(col("BRAND_RAW")),
    }).with_column("PFX", prefix(col("BRAND_CHARS"), 3)) \
     .filter(length(col("BRAND_CHARS")) > 0)

tx = session.table(TX_TBL).select(
        col(TX_COL).alias("MERCH_RAW")
    ).with_columns({
        "MERCH_CHARS": char_only(col("MERCH_RAW")),
    }).with_column("PFX", prefix(col("MERCH_CHARS"), 3)) \
     .filter(length(col("MERCH_CHARS")) > 0)

if TX_FILTER:
    tx = tx.filter(TX_FILTER)

# ---- candidate generation (by 3-letter character prefix; fallback to 1-2 if short) ----
candidates = vd.join(tx, vd["PFX"] == tx["PFX"], how="inner")
# optional: widen for short names
short_v = vd.filter(length(col("BRAND_CHARS")) < 3) \
            .join(tx, prefix(col("BRAND_CHARS"), 1) == prefix(col("MERCH_CHARS"), 1), how="inner")
candidates = candidates.union_all(short_v).distinct()

# ---- character-based similarities ----
jw = call_function("JARO_WINKLER_SIMILARITY", col("BRAND_CHARS"), col("MERCH_CHARS"))
lev = call_function("EDITDISTANCE", col("BRAND_CHARS"), col("MERCH_CHARS"))
den = greatest(length(col("BRAND_CHARS")), length(col("MERCH_CHARS")))
lev_sim = (lit(1.0) - (lev / call_function("NULLIF", den, lit(0))))

scored = candidates.with_columns({
    "JW": jw,
    "LEV_SIM": lev_sim,
    "SCORE": JW_WT * jw + LEV_WT * lev_sim
})

# ---- pick best merchant per brand ----
w = Window.partition_by("BRAND_RAW").order_by(col("SCORE").desc())
best = scored.with_column("RN", call_function("ROW_NUMBER").over(w)) \
             .filter(col("RN") == 1) \
             .drop("RN")

# ---- confidence bands and exists flag ----
best = best.with_column(
    "CONFIDENCE",
    when(col("SCORE") >= lit(HIGH), lit("HIGH"))
     .when(col("SCORE") >= lit(MED),  lit("MEDIUM"))
     .otherwise(lit("LOW"))
).with_column(
    "EXISTS_IN_TX", (col("SCORE") >= lit(MED)).cast("int")
)

result = best.select(
    col("BRAND_RAW").alias("BRAND"),
    col("MERCH_RAW").alias("BEST_MATCH_MERCHANT"),
    col("JW").alias("jw_char_sim"),
    col("LEV_SIM").alias("lev_char_sim"),
    col("SCORE").alias("combined_score"),
    col("CONFIDENCE"),
    col("EXISTS_IN_TX")
)

# coverage metric
metrics = result.agg(
    call_function("COUNT_IF", col("EXISTS_IN_TX") == lit(1)).alias("matched_brands"),
    call_function("COUNT", lit(1)).alias("total_brands")
).with_column("coverage_pct", (col("matched_brands")*100.0 / col("total_brands")))

result.show()     # per-brand best match with scores and confidence
metrics.show()    # overall coverage
# result.write.save_as_table('ANALYTICS.BRAND_MATCHES_CHAR', mode='overwrite')
