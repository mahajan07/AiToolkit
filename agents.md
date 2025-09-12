```
-- ============================
-- PARAMETERS (tune as needed)
-- ============================
SET year_filter           = '2025';
SET min_sim_strict        = 0.78;  -- accept outright if sim >= this
SET min_sim_with_hint     = 0.72;  -- accept if sim >= this AND substring hint true
SET geo_bonus_per_level   = 0.02;  -- geo_confidence_level 0..3 => up to +0.06
SET substring_bonus       = 0.05;  -- small bump when substring hit
SET editdistance_cap      = 0.02;  -- max bonus from editdistance ratio
SET topk_per_merchant     = 5;     -- keep top-K candidates before final pick (perf guard)

-- ============================
-- INPUTS
-- ============================
WITH brands AS (
  SELECT
      BRANDS                                AS brand_name_raw,
      clean_for_embedding(BRANDS)           AS brand_name_clean,
      brand_vector,
      brand_vector_nospace
  FROM SBOX_CONSUMERBANKING_BI.DEPOSITS_CARDS_CHNL_PROD.tysons_brands_embeddings
),
merchants AS (
  SELECT
      DAF_MERCH_NAME                         AS merch_name_raw,
      clean_for_embedding(DAF_MERCH_NAME)    AS merch_name_clean,
      DAF_MERCH_ZIP_CODE,
      DAF_MERCH_CITY,
      DAF_MERCH_STATE_COUNTRY,
      geo_confidence_level,
      merchant_vector,
      merchant_vector_nospace
  FROM SBOX_CONSUMERBANKING_BI.DEPOSITS_CARDS_CHNL_PROD.tysons_merchants_embeddings
),

-- ============================
-- TRANSACTION AGGREGATION (merchant-level)
-- Keep the same geography & time scope as your embeddings build.
-- ============================
tx AS (
  SELECT
      clean_for_embedding(DAF_MERCH_NAME) AS merch_name_clean,
      COUNT(*)                            AS trx_count,
      SUM(DAF_AMOUNT)                     AS total_amount
  FROM PTX_DB.L0_RAW_SCH.RAW_AUTHFILE
  WHERE PERIODDATE = $year_filter
    AND DAF_MERCH_STATE_COUNTRY = 'VA'
    AND (
          LEFT(DAF_MERCH_ZIP_CODE,5) IN ('22102','22182')
          OR UPPER(COALESCE(DAF_MERCH_CITY,'')) IN ('MCLEAN','VIENNA','TYSONS','TYSONS CORNER')
        )
  GROUP BY 1
),

-- ============================
-- CANDIDATE PAIRS (light blocking to avoid full cross join)
-- ============================
candidates AS (
  SELECT
      m.*,
      b.brand_name_raw,
      b.brand_name_clean,
      b.brand_vector,
      b.brand_vector_nospace,

      -- Vector sims
      (1 - VECTOR_COSINE_DISTANCE(m.merchant_vector,        b.brand_vector       )) AS sim,
      (1 - VECTOR_COSINE_DISTANCE(m.merchant_vector_nospace,b.brand_vector_nospace)) AS sim_ns,

      -- String hints
      (CASE WHEN m.merch_name_clean LIKE '%' || b.brand_name_clean || '%'
              OR b.brand_name_clean LIKE '%' || m.merch_name_clean || '%'
            THEN TRUE ELSE FALSE END) AS substring_hit,

      -- Light edit-distance ratio (bounded)
      LEAST(
        CASE
          WHEN GREATEST(LENGTH(m.merch_name_clean), LENGTH(b.brand_name_clean)) = 0 THEN 0
          ELSE 1.0
             - (EDITDISTANCE(m.merch_name_clean, b.brand_name_clean)
                / GREATEST(LENGTH(m.merch_name_clean), LENGTH(b.brand_name_clean)))
        END,
        1.0
      ) AS ed_ratio

  FROM merchants m
  JOIN brands b
    ON  -- cheap blocks (any true keeps the pair)
       LEFT(m.merch_name_clean,1) = LEFT(b.brand_name_clean,1)
    OR ABS(LENGTH(m.merch_name_clean) - LENGTH(b.brand_name_clean)) <= 8
    OR m.merch_name_clean LIKE '%' || b.brand_name_clean || '%'
    OR b.brand_name_clean LIKE '%' || m.merch_name_clean || '%'
),

-- ============================
-- SCORE + TOP-K per merchant (perf guard)
-- ============================
scored AS (
  SELECT
      merch_name_raw,
      merch_name_clean,
      DAF_MERCH_ZIP_CODE,
      DAF_MERCH_CITY,
      DAF_MERCH_STATE_COUNTRY,
      geo_confidence_level,

      brand_name_raw,
      brand_name_clean,

      GREATEST(sim, sim_ns)                                              AS sim_max,
      substring_hit,
      ed_ratio,

      /* Composite score:
         base similarity + substring bonus + tiny editdistance bonus + geo bonus
      */
      GREATEST(sim, sim_ns)
        + (CASE WHEN substring_hit THEN $substring_bonus ELSE 0 END)
        + LEAST(GREATEST(ed_ratio - 0.80, 0) * $editdistance_cap / 0.20, $editdistance_cap)
        + (COALESCE(geo_confidence_level,0) * $geo_bonus_per_level)      AS match_score,

      ROW_NUMBER() OVER (
        PARTITION BY merch_name_clean
        ORDER BY
          GREATEST(sim, sim_ns) DESC,
          substring_hit DESC,
          ed_ratio DESC
      ) AS rn_sim_rank

  FROM candidates
),
topk AS (
  SELECT *
  FROM scored
  WHERE rn_sim_rank <= $topk_per_merchant
),

-- ============================
-- CHOOSE TOP-1 + APPLY ACCEPTANCE RULES
-- ============================
picked AS (
  SELECT
      merch_name_raw,
      merch_name_clean,
      DAF_MERCH_ZIP_CODE,
      DAF_MERCH_CITY,
      DAF_MERCH_STATE_COUNTRY,
      geo_confidence_level,

      brand_name_raw,
      brand_name_clean,
      sim_max,
      substring_hit,
      ed_ratio,
      match_score,

      ROW_NUMBER() OVER (
        PARTITION BY merch_name_clean
        ORDER BY match_score DESC, sim_max DESC
      ) AS rn
  FROM topk
),
final_map AS (
  SELECT
      merch_name_clean,
      /* keep brand NULL unless rules pass */
      CASE
        WHEN sim_max >= $min_sim_strict THEN brand_name_raw
        WHEN sim_max >= $min_sim_with_hint AND substring_hit THEN brand_name_raw
        WHEN match_score >= ($min_sim_strict + 0.02) THEN brand_name_raw
        ELSE NULL
      END AS mapped_brand,
      sim_max,
      match_score,
      substring_hit,
      ed_ratio,
      geo_confidence_level
  FROM picked
  WHERE rn = 1
),

-- ============================
-- MERCHANT-LEVEL AGGREGATION + LEFT JOIN TO MAP
-- (Unmatched merchants keep mapped_brand = NULL)
-- ============================
merchant_rollup AS (
  SELECT
      m.merch_name_clean,
      ANY_VALUE(m.merch_name_raw)              AS sample_merchant_name,
      ANY_VALUE(m.DAF_MERCH_CITY)              AS sample_city,
      ANY_VALUE(m.DAF_MERCH_ZIP_CODE)          AS sample_zip,
      ANY_VALUE(m.DAF_MERCH_STATE_COUNTRY)     AS sample_state,

      COALESCE(f.mapped_brand, NULL)           AS mapped_brand,
      f.sim_max,
      f.match_score,
      f.substring_hit,
      f.ed_ratio,
      f.geo_confidence_level,

      COALESCE(t.trx_count, 0)                 AS trx_count,
      COALESCE(t.total_amount, 0)              AS total_amount
  FROM merchants m
  LEFT JOIN final_map f
    ON f.merch_name_clean = m.merch_name_clean
  LEFT JOIN tx t
    ON t.merch_name_clean = m.merch_name_clean
  GROUP BY 1, 6, 7, 8, 9, 10, 11
)

-- ============================
-- MATERIALIZE RESULT
-- ============================
CREATE OR REPLACE TABLE
  SBOX_CONSUMERBANKING_BI.DEPOSITS_CARDS_CHNL_PROD.tysons_merchant_brand_map_2025
AS
SELECT
  merch_name_clean                         AS merchant_key,
  sample_merchant_name                     AS merchant_name_example,
  sample_city,
  sample_zip,
  sample_state,

  mapped_brand,                            -- NULL when weak/unclear
  sim_max,
  match_score,
  substring_hit,
  ed_ratio,
  geo_confidence_level,

  trx_count,
  total_amount
FROM merchant_rollup
;

```
