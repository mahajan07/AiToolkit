```
/* =======================================================================================
   TYSONS: Brands ↔ Merchants (Transactions) fuzzy mapping with embeddings
   - Uses cosine SIMILARITY (higher is better)
   - Leaves brand NULL when score below thresholds
   - Produces merchant-level trx_count and total_amount
   ======================================================================================= */

/* -------------------------------------------
   STEP 0 (optional) — cleaning UDF.
   If you already created clean_for_embedding(), you can skip this block.
-------------------------------------------- */
CREATE OR REPLACE FUNCTION clean_for_embedding(s STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
TRIM(
  REGEXP_REPLACE(                                         -- 6) collapse spaces
    REGEXP_REPLACE(                                       -- 5) keep only A–Z, space, &
      REGEXP_REPLACE(                                     -- 4) processor/site tokens
        REGEXP_REPLACE(                                   -- 3) remove US states / cities (tune as needed)
          REGEXP_REPLACE(                                 -- 2) remove trailing numbers / units at END
            REGEXP_REPLACE(                               -- 1) mall/location style stop-words
              UPPER(s),
              '\\b(MALL|YSI|TST|SQ|CVENT|WWW|WWP|GAITHERSBURG|CENTER|CENTRE|KIOSK|INC|SINCE|STORE|OF|CORNER|CRNR)\\b',
              ' '
            ),
            '\\s*(NO\\.?|#)?\\s*\\d[\\w\\-]*\\s*$',
            ' '
          ),
          '\\b(AL|AK|AZ|AR|CA|CO|CT|DE|DC|FL|GA|HI|IA|ID|IL|IN|KS|KY|LA|MA|MD|ME|MI|MN|MO|MS|MT|NC|ND|NE|NH|NJ|NM|NV|NY|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VA|VT|WA|WI|WV|VIRGINIA|MARYLAND|MCLEAN)\\b\\.?\\s*',
          ' '
        ),
        '\\b(WWW|HTTP|HTTPS|SQ|SQUARE|CVENT|PAYPAL|PYPL|VENMO|ZELLE|CASHAPP|AMZ|AMZN)\\b',
        ' '
      ),
      '[^A-Z\\s&]+',
      ' '
    ),
    '\\s+',
    ' '
  )
)
$$;

/* -------------------------------------------
   STEP 1 — parameters (no session variables needed)
-------------------------------------------- */
WITH params AS (
  SELECT
    'snowflake-arctic-embed-m-v1.5'::STRING AS model_name,       -- embedding model
    0.06::FLOAT  AS substring_bonus,                              -- bonus when substring hit
    0.02::FLOAT  AS geo_bonus_per_level,                          -- multiplied by 0..3
    0.80::FLOAT  AS accept_hi,                                    -- strict accept
    0.74::FLOAT  AS accept_lo,                                    -- relaxed accept when substring hit
    2025::INTEGER AS period_year                                  -- time window (adjust if needed)
),

/* -------------------------------------------
   STEP 2 — build BRAND embeddings
   Source: your Tysons store list (one row per brand/location)
-------------------------------------------- */
brands AS (
  SELECT
      b.BRANDS,
      b.LOCATION,
      clean_for_embedding(b.BRANDS)                         AS brand_name_clean,
      REGEXP_REPLACE(clean_for_embedding(b.BRANDS), '\\s+','') AS brand_name_nospace,
      /* embed both spaced and nospace forms */
      AI_EMBED(p.model_name, clean_for_embedding(b.BRANDS))      AS brand_vector,
      AI_EMBED(p.model_name, REGEXP_REPLACE(clean_for_embedding(b.BRANDS), '\\s+','')) AS brand_vector_nospace
  FROM SBOX_CONSUMERBANKING_BI.DEPOSITS_CARDS_CHNL_PROD.TYSONS_STORE_DIR_DM b
  CROSS JOIN params p
),

/* -------------------------------------------
   STEP 3 — build unique MERCHANT embeddings from raw transactions
   - ZIP inconsistency handled by deriving ZIP5
   - geo_confidence_level gives a tiny scoring bonus (0..3)
-------------------------------------------- */
tx_base AS (   -- filter transactions in scope (VA + Tysons area + year)
  SELECT *
  FROM PTX_DB.L0_RAW_SCH.RAW_AUTHFILE
  WHERE DAF_MERCH_STATE_COUNTRY = 'VA'
    AND (
      LEFT(COALESCE(DAF_MERCH_ZIP_CODE,''), 5) IN ('22102','22182')
      OR UPPER(COALESCE(DAF_MERCH_CITY,'')) IN ('MCLEAN','VIENNA','TYSONS','TYSONS CORNER')
    )
    /* If you store year separately (e.g. PERIODDATE='2025'), keep this form.
       Otherwise change to YEAR(txn_ts_column)=p.period_year. */
    AND PERIODDATE = '2025'
),
merchants AS (
  SELECT
      /* raw */
      UPPER(TRIM(DAF_MERCH_NAME))                             AS merch_name_raw,
      UPPER(COALESCE(DAF_MERCH_CITY,''))                      AS DAF_MERCH_CITY,
      COALESCE(LEFT(DAF_MERCH_ZIP_CODE,5),'')                 AS ZIP5,
      DAF_MERCH_STATE_COUNTRY,

      /* cleaned for text similarity */
      clean_for_embedding(DAF_MERCH_NAME)                     AS merch_name_clean,
      REGEXP_REPLACE(clean_for_embedding(DAF_MERCH_NAME), '\\s+','') AS merch_name_nospace,

      /* tiny geo signal: 3=zip5 in 22102/22182, 2=prefix match, 1=city hit, else 0 */
      CASE
        WHEN LEFT(COALESCE(DAF_MERCH_ZIP_CODE,''),5) IN ('22102','22182') THEN 3
        WHEN SUBSTR(COALESCE(DAF_MERCH_ZIP_CODE,''),1,3) IN ('221')       THEN 2
        WHEN UPPER(COALESCE(DAF_MERCH_CITY,'')) IN ('MCLEAN','VIENNA','TYSONS','TYSONS CORNER') THEN 1
        ELSE 0
      END AS geo_confidence_level
  FROM tx_base
  GROUP BY 1,2,3,4,5,6,7
),
merchants_with_vec AS (
  SELECT
      m.*,
      AI_EMBED(p.model_name, m.merch_name_clean)                AS merchant_vector,
      AI_EMBED(p.model_name, m.merch_name_nospace)              AS merchant_vector_nospace
  FROM merchants m
  CROSS JOIN params p
),

/* -------------------------------------------
   STEP 4 — candidate pairs and features
   Light blocking: first letter match OR quick substring overlap
-------------------------------------------- */
candidates AS (
  SELECT
      m.merch_name_raw,
      m.merch_name_clean,
      m.merch_name_nospace,
      m.geo_confidence_level,
      b.BRANDS,
      b.LOCATION,
      b.brand_name_clean,
      b.brand_name_nospace,

      /* base similarities (higher is better) */
      VECTOR_COSINE_SIMILARITY(m.merchant_vector,         b.brand_vector)         AS sim,
      VECTOR_COSINE_SIMILARITY(m.merchant_vector_nospace, b.brand_vector_nospace) AS sim_ns,

      /* quick string hints (not decisive, just bonuses) */
      CASE
        WHEN m.merch_name_clean    LIKE '%' || b.brand_name_clean    || '%'
          OR b.brand_name_clean    LIKE '%' || m.merch_name_clean    || '%'
          OR m.merch_name_nospace  LIKE '%' || b.brand_name_nospace  || '%'
          OR b.brand_name_nospace  LIKE '%' || m.merch_name_nospace  || '%'
        THEN TRUE ELSE FALSE
      END AS substring_hit,

      /* normalized edit distance hint: 0..1 (1 = identical) */
      CASE
        WHEN GREATEST(LENGTH(m.merch_name_clean), LENGTH(b.brand_name_clean)) = 0 THEN 0.0
        ELSE 1.0 - ( EDITDISTANCE(m.merch_name_clean, b.brand_name_clean)
                    / GREATEST(LENGTH(m.merch_name_clean), LENGTH(b.brand_name_clean)) )
      END AS ed_ratio
  FROM merchants_with_vec m
  JOIN brands b
    ON SUBSTR(m.merch_name_clean,1,1) = SUBSTR(b.brand_name_clean,1,1)  -- simple block
       OR m.merch_name_clean LIKE '%' || b.brand_name_clean || '%'
       OR b.brand_name_clean LIKE '%' || m.merch_name_clean || '%'
),

/* -------------------------------------------
   STEP 5 — score & choose the best brand per merchant
   - ba

```
