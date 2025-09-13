```
/* =============================================================================
   TYSONS — Map merchants (from trx) to brand list using *existing* embedding tables
   - Uses VECTOR_COSINE_SIMILARITY (higher = better)
   - Strict thresholds → weak matches stay NULL
   - Outputs merchant-level trx_count & total_amount
   - No session variables; tune knobs in the params CTE
   ============================================================================= */

WITH
/* -------------------------------
   Parameters (tune here)
-------------------------------- */
params AS (
  SELECT
    0.06::FLOAT AS substring_bonus,        -- add if substring hit
    0.02::FLOAT AS geo_bonus_per_level,    -- * geo_confidence_level (0..3)
    0.20::FLOAT AS ed_cap,                 -- cap for editdistance contribution (0..1 scale)
    0.80::FLOAT AS accept_hi,              -- strict accept
    0.74::FLOAT AS accept_lo,              -- relaxed accept when substring hits
    '2025'::STRING AS period_yr            -- RAW_AUTHFILE period filter (adjust if needed)
),

/* -------------------------------
   Brands — reference your embeddings table
   Expected columns (adapt if different):
     BRANDS, LOCATION,
     cleaned_brand_name,
     brand_vector, brand_vector_nospace
-------------------------------- */
brands AS (
  SELECT
    BRANDS,
    LOCATION,
    cleaned_brand_name                                         AS brand_name_clean,
    REGEXP_REPLACE(cleaned_brand_name, '\\s+', '')             AS brand_name_nospace,
    brand_vector,
    brand_vector_nospace
  FROM SBOX_CONSUMERBANKING_BI.DEPOSITS_CARDS_CHNL_PROD.tysons_brands_embeddings
),

/* -------------------------------
   Merchants — reference your embeddings table
   Expected columns (adapt if different):
     DAF_MERCH_NAME,
     cleaned_merchant_name,
     merchant_vector, merchant_vector_nospace,
     DAF_MERCH_CITY, DAF_MERCH_ZIP_CODE, DAF_MERCH_STATE_COUNTRY,
     geo_confidence_level (0..3)
-------------------------------- */
merchants AS (
  SELECT
    DAF_MERCH_NAME,
    cleaned_merchant_name                                      AS merch_name_clean,
    REGEXP_REPLACE(cleaned_merchant_name, '\\s+', '')          AS merch_name_nospace,
    merchant_vector,
    merchant_vector_nospace,
    UPPER(COALESCE(DAF_MERCH_CITY, ''))                        AS DAF_MERCH_CITY,
    COALESCE(LEFT(DAF_MERCH_ZIP_CODE, 5), '')                  AS ZIP5,
    DAF_MERCH_STATE_COUNTRY,
    COALESCE(geo_confidence_level, 0)                          AS geo_confidence_level
  FROM SBOX_CONSUMERBANKING_BI.DEPOSITS_CARDS_CHNL_PROD.tysons_merchants_embeddings
),

/* -------------------------------
   Candidate pairs (light blocking)
   Avoid full cross join; keep recall high.
-------------------------------- */
candidates AS (
  SELECT
    m.DAF_MERCH_NAME,
    m.merch_name_clean,
    m.merch_name_nospace,
    m.geo_confidence_level,

    b.BRANDS,
    b.LOCATION,
    b.brand_name_clean,
    b.brand_name_nospace,

    /* base cosine similarities (higher = better) */
    VECTOR_COSINE_SIMILARITY(m.merchant_vector,         b.brand_vector)         AS sim,
    VECTOR_COSINE_SIMILARITY(m.merchant_vector_nospace, b.brand_vector_nospace) AS sim_ns,

    /* fast string hints (bonuses only) */
    CASE
      WHEN m.merch_name_clean   LIKE '%' || b.brand_name_clean   || '%'
        OR b.brand_name_clean   LIKE '%' || m.merch_name_clean   || '%'
        OR m.merch_name_nospace LIKE '%' || b.brand_name_nospace || '%'
        OR b.brand_name_nospace LIKE '%' || m.merch_name_nospace || '%'
      THEN TRUE ELSE FALSE
    END AS substring_hit,

    /* normalized edit-distance similarity: 0..1 */
    CASE
      WHEN GREATEST(LENGTH(m.merch_name_clean), LENGTH(b.brand_name_clean)) = 0 THEN 0.0
      ELSE 1.0 - (
        EDITDISTANCE(m.merch_name_clean, b.brand_name_clean)
        / GREATEST(LENGTH(m.merch_name_clean), LENGTH(b.brand_name_clean))
      )
    END AS ed_ratio
  FROM merchants m
  JOIN brands b
    ON  SUBSTR(m.merch_name_clean,1,1) = SUBSTR(b.brand_name_clean,1,1)  -- cheap block
     OR m.merch_name_clean LIKE '%' || b.brand_name_clean || '%'
     OR b.brand_name_clean LIKE '%' || m.merch_name_clean || '%'
),

/* -------------------------------
   Score & choose best brand per merchant
   - base = max(sim, sim_ns) clamped to 0..1
   - + bonuses: substring + tiny geo + tiny ed_ratio (capped)
-------------------------------- */
scored AS (
  SELECT
    c.*,

    /* clamp negatives and take the stronger of the two similarities */
    GREATEST(0.0, GREATEST(sim, sim_ns)) AS base_sim,

    /* composite score: base sim + small bonuses */
    GREATEST(0.0, GREATEST(sim, sim_ns))
      + CASE WHEN c.substring_hit THEN p.substring_bonus ELSE 0 END
      + c.geo_confidence_level * p.geo_bonus_per_level
      + LEAST(GREATEST(c.ed_ratio, 0.0), p.ed_cap) * 0.20      -- small cap on editdistance influence
      AS match_score,

    ROW_NUMBER() OVER (
      PARTITION BY c.merch_name_clean
      ORDER BY
        GREATEST(0.0, GREATEST(sim, sim_ns)) DESC,
        c.substring_hit DESC,
        c.geo_confidence_level DESC,
        c.ed_ratio DESC
    ) AS rn
  FROM candidates c
  CROSS JOIN params p
),

best_brand_per_merchant AS (
  SELECT
    merch_name_clean,
    /* keep brand only if it's strong; else NULL */
    CASE
      WHEN match_score >= p.accept_hi
        OR (match_score >= p.accept_lo AND substring_hit)
      THEN brand_name_clean
      ELSE NULL
    END AS mapped_brand,
    match_score,
    substring_hit,
    geo_confidence_level
  FROM scored
  CROSS JOIN params p
  WHERE rn = 1
),

/* -------------------------------
   Aggregate transactions for the same area/time window
   - Clean the merchant name the same way so the join aligns
   - Keep your Tysons/VA filters and 5+4 → ZIP5 handling
-------------------------------- */
tx_base AS (
  SELECT *
  FROM PTX_DB.L0_RAW_SCH.RAW_AUTHFILE
  WHERE DAF_MERCH_STATE_COUNTRY = 'VA'
    AND (
      LEFT(COALESCE(DAF_MERCH_ZIP_CODE,''), 5) IN ('22102','22182')
      OR UPPER(COALESCE(DAF_MERCH_CITY,'')) IN ('MCLEAN','VIENNA','TYSONS','TYSONS CORNER')
    )
    AND PERIODDATE = (SELECT period_yr FROM params)
),
trx_agg AS (
  SELECT
    clean_for_embedding(DAF_MERCH_NAME) AS merch_name_clean,
    COUNT(*)                            AS trx_count,
    SUM(DAF_AMOUNT)                     AS total_amount
  FROM tx_base
  GROUP BY 1
)

/* -------------------------------
   FINAL — merchant level with nullable mapped brand
-------------------------------- */
SELECT
  a.merch_name_clean,
  m.mapped_brand,                 -- NULL when not strong enough
  m.match_score,
  m.substring_hit,
  COALESCE(m.geo_confidence_level, 0) AS geo_confidence_level,
  a.trx_count,
  a.total_amount
FROM trx_agg a
LEFT JOIN best_brand_per_merchant m
  ON a.merch_name_clean = m.merch_name_clean
ORDER BY a.trx_count DESC, a.total_amount DESC;
```
