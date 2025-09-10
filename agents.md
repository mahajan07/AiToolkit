```
-- =====================================================
-- ENHANCED VECTOR EMBEDDING APPROACH
-- Improved accuracy with better preprocessing and thresholds
-- =====================================================

-- Step 1: Enhanced cleaning function for better embeddings
CREATE OR REPLACE FUNCTION clean_for_embedding(s STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
  TRIM(
    REGEXP_REPLACE(
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(
              UPPER(COALESCE(s,'')),
              '(\\+?1[\\-\\.\\s]?)?\\(?\\d{3}\\)?[\\-\\.\\s]?\\d{3}[\\-\\.\\s]?\\d{4}', ' '), -- phone
            '(#\\d+|[0-9]{3,})', ' '),                                                    -- store numbers / long numbers (3+ digits)
          '\\b(TYSONS|TYSON\\'?S|MCLEAN|VIENNA|CORNER|CENTER|CTR|MALL|UNITED STATES|USA|VA|VIRGINIA)\\b', ' '), -- expanded stopwords
        '\\b(STORE|SHOP|LOCATION|BRANCH|OUTLET|PLAZA|SQUARE)\\b', ' '),                 -- additional retail stopwords
      '[^A-Z& ]', ' '),                                                                  -- keep letters, space, &
    '\\s+', ' ')                                                                         -- squeeze spaces
  )
$$;

-- Step 2: Choose better embedding model and create brand embeddings
SET MODEL = 'snowflake-arctic-embed-m-v1.5';  -- Better than the basic model

CREATE OR REPLACE TABLE SBOX_CONSUMERBANKING_BI.DEPOSITS_CARDS_CHNL_PROD.tysons_brands_embeddings AS
SELECT 
  BRAND_RAW,
  clean_for_embedding(BRAND_RAW) as cleaned_brand_name,
  LENGTH(BRAND_RAW) as brand_length,
  SNOWFLAKE.ML.EMBED_TEXT_768($MODEL, cleaned_brand_name) AS brand_vector,
  -- Create variations for better matching
  SNOWFLAKE.ML.EMBED_TEXT_768($MODEL, REGEXP_REPLACE(cleaned_brand_name, '\\s+', '')) AS brand_vector_nospace,
  CURRENT_TIMESTAMP() as created_at
FROM SBOX_CONSUMERBANKING_BI.DEPOSITS_CARDS_CHNL_PROD.tysons_brand_directory
WHERE clean_for_embedding(BRAND_RAW) IS NOT NULL 
  AND LENGTH(clean_for_embedding(BRAND_RAW)) > 2;

-- Step 3: Create merchant embeddings with geographic filtering
CREATE OR REPLACE TABLE SBOX_CONSUMERBANKING_BI.DEPOSITS_CARDS_CHNL_PROD.tysons_merchants_embeddings AS
SELECT 
  DAF_MERCH_NAME,
  clean_for_embedding(DAF_MERCH_NAME) as cleaned_merchant_name,
  SNOWFLAKE.ML.EMBED_TEXT_768($MODEL, clean_for_embedding(DAF_MERCH_NAME)) AS merchant_vector,
  SNOWFLAKE.ML.EMBED_TEXT_768($MODEL, REGEXP_REPLACE(clean_for_embedding(DAF_MERCH_NAME), '\\s+', '')) AS merchant_vector_nospace,
  
  -- Pre-compute geographic confidence
  CASE 
    WHEN DAF_MERCH_ZIP_CODE IN ('22102-4501', '22102-4601', '22102-4698', '22102-4603', '22102-5953') THEN 3
    WHEN LEFT(DAF_MERCH_ZIP_CODE, 5) IN ('22102', '22182') THEN 2
    WHEN DAF_MERCH_STATE_COUNTRY = 'VA' 
         AND DAF_MERCH_CITY IN ('MCLEAN', 'VIENNA', 'TYSONS', 'TYSONS CORNER') THEN 1
    ELSE 0
  END AS geo_confidence_level,
  
  COUNT(*) as transaction_frequency,
  SUM(DAF_AMOUNT) as total_amount,
  CURRENT_TIMESTAMP() as created_at
  
FROM PTX_DB.L0_RAW_SCH.RAW_AUTHFILE
WHERE DAF_MERCH_STATE_COUNTRY = 'VA'
  AND PERIODDATE >= '2024-01-01'
  AND (
    LEFT(DAF_MERCH_ZIP_CODE, 5) IN ('22102', '22182') 
    OR DAF_MERCH_CITY IN ('MCLEAN', 'VIENNA', 'TYSONS', 'TYSONS CORNER')
  )
  AND clean_for_embedding(DAF_MERCH_NAME) IS NOT NULL
  AND LENGTH(clean_for_embedding(DAF_MERCH_NAME)) > 2
GROUP BY 1,2,3,4,5;

-- Step 4: Enhanced matching with multiple similarity metrics
WITH enhanced_similarity AS (
  SELECT 
    b.BRAND_RAW,
    b.cleaned_brand_name,
    b.brand_length,
    m.DAF_MERCH_NAME,
    m.cleaned_merchant_name,
    m.geo_confidence_level,
    m.transaction_frequency,
    m.total_amount,
    
    -- Multiple similarity calculations
    VECTOR_COSINE_SIMILARITY(b.brand_vector, m.merchant_vector) as cosine_similarity,
    VECTOR_COSINE_SIMILARITY(b.brand_vector_nospace, m.merchant_vector_nospace) as cosine_similarity_nospace,
    
    -- Hybrid similarity (embedding + string)
    GREATEST(
      VECTOR_COSINE_SIMILARITY(b.brand_vector, m.merchant_vector),
      VECTOR_COSINE_SIMILARITY(b.brand_vector_nospace, m.merchant_vector_nospace),
      CASE WHEN m.cleaned_merchant_name = b.cleaned_brand_name THEN 1.0 ELSE 0.0 END,
      CASE WHEN CONTAINS(m.cleaned_merchant_name, b.cleaned_brand_name) 
                OR CONTAINS(b.cleaned_brand_name, m.cleaned_merchant_name) THEN 0.95 ELSE 0.0 END
    ) as hybrid_similarity,
    
    -- Brand specificity score (longer brands = more specific)
    CASE 
      WHEN b.brand_length >= 15 THEN 1.0
      WHEN b.brand_length >= 10 THEN 0.9
      WHEN b.brand_length >= 6 THEN 0.8
      ELSE 0.7
    END as brand_specificity_score
    
  FROM SBOX_CONSUMERBANKING_BI.DEPOSITS_CARDS_CHNL_PROD.tysons_brands_embeddings b
  CROSS JOIN SBOX_CONSUMERBANKING_BI.DEPOSITS_CARDS_CHNL_PROD.tysons_merchants_embeddings m
  WHERE VECTOR_COSINE_SIMILARITY(b.brand_vector, m.merchant_vector) > 0.7  -- Pre-filter weak matches
),

best_matches_per_merchant AS (
  SELECT *,
    -- Weighted final score
    (hybrid_similarity * 0.7 + brand_specificity_score * 0.3) as final_similarity_score,
    
    ROW_NUMBER() OVER (
      PARTITION BY DAF_MERCH_NAME 
      ORDER BY 
        (hybrid_similarity * 0.7 + brand_specificity_score * 0.3) DESC,
        brand_length DESC,
        BRAND_RAW ASC
    ) as match_rank
  FROM enhanced_similarity
),

classified_matches AS (
  SELECT *,
    -- Convert similarity to confidence percentage
    ROUND(final_similarity_score * 100, 1) as similarity_confidence,
    
    -- Overall confidence combining similarity + geography
    CASE 
      WHEN geo_confidence_level = 3 AND final_similarity_score >= 0.85 THEN 95
      WHEN geo_confidence_level = 3 AND final_similarity_score >= 0.75 THEN 90
      WHEN geo_confidence_level = 2 AND final_similarity_score >= 0.90 THEN 90
      WHEN geo_confidence_level = 2 AND final_similarity_score >= 0.80 THEN 85
      WHEN geo_confidence_level = 2 AND final_similarity_score >= 0.75 THEN 80
      WHEN geo_confidence_level = 1 AND final_similarity_score >= 0.95 THEN 75
      WHEN final_similarity_score >= 0.95 THEN 70  -- Very high similarity, any geography
      ELSE GREATEST(50, ROUND(final_similarity_score * 65, 0))
    END as overall_confidence_score
    
  FROM best_matches_per_merchant
  WHERE match_rank = 1  -- Only best match per merchant
    AND final_similarity_score >= 0.75  -- Minimum similarity threshold
)

-- Final results
SELECT 
  DAF_MERCH_NAME,
  cleaned_merchant_name,
  BRAND_RAW as matched_brand,
  cleaned_brand_name,
  
  -- Classification
  CASE 
    WHEN (geo_confidence_level >= 2 AND final_similarity_score >= 0.75)
      OR (geo_confidence_level = 1 AND final_similarity_score >= 0.90)
      OR final_similarity_score >= 0.95 THEN 'In TysonsCC'
    ELSE 'Outside TysonsCC'
  END as tysons_classification,
  
  -- Confidence metrics
  overall_confidence_score,
  similarity_confidence,
  ROUND(cosine_similarity * 100, 1) as cosine_similarity_pct,
  ROUND(hybrid_similarity * 100, 1) as hybrid_similarity_pct,
  
  -- Geographic info
  CASE geo_confidence_level 
    WHEN 3 THEN 'HIGH_GEO' 
    WHEN 2 THEN 'MEDIUM_GEO' 
    WHEN 1 THEN 'LOW_GEO' 
    ELSE 'NO_GEO' 
  END as geo_level,
  
  -- Transaction metrics for validation
  transaction_frequency,
  total_amount

FROM classified_matches
ORDER BY overall_confidence_score DESC, final_similarity_score DESC, total_amount DESC
LIMIT 200;

-- =====================================================
-- VALIDATION AND TUNING QUERIES
-- =====================================================

-- Check distribution of similarity scores
SELECT 
  CASE 
    WHEN final_similarity_score >= 0.95 THEN '0.95+'
    WHEN final_similarity_score >= 0.90 THEN '0.90-0.95'
    WHEN final_similarity_score >= 0.85 THEN '0.85-0.90'
    WHEN final_similarity_score >= 0.80 THEN '0.80-0.85'
    WHEN final_similarity_score >= 0.75 THEN '0.75-0.80'
    ELSE '<0.75'
  END as similarity_range,
  COUNT(*) as merchant_count,
  AVG(transaction_frequency) as avg_transaction_freq,
  SUM(total_amount) as total_dollar_amount
FROM best_matches_per_merchant
WHERE match_rank = 1
GROUP BY 1
ORDER BY similarity_range DESC;

-- Review borderline matches for manual validation
SELECT 
  DAF_MERCH_NAME,
  BRAND_RAW,
  ROUND(final_similarity_score * 100, 1) as similarity_pct,
  ROUND(cosine_similarity * 100, 1) as cosine_pct,
  geo_confidence_level,
  transaction_frequency
FROM best_matches_per_merchant
WHERE match_rank = 1
  AND final_similarity_score BETWEEN 0.75 AND 0.85
ORDER BY final_similarity_score DESC, transaction_frequency DESC
LIMIT 50;

-- Find high-frequency merchants with no good matches
SELECT 
  m.DAF_MERCH_NAME,
  m.cleaned_merchant_name,
  m.geo_confidence_level,
  m.transaction_frequency,
  m.total_amount
FROM SBOX_CONSUMERBANKING_BI.DEPOSITS_CARDS_CHNL_PROD.tysons_merchants_embeddings m
LEFT JOIN best_matches_per_merchant bm 
  ON m.DAF_MERCH_NAME = bm.DAF_MERCH_NAME 
  AND bm.match_rank = 1 
  AND bm.final_similarity_score >= 0.75
WHERE bm.DAF_MERCH_NAME IS NULL
  AND m.transaction_frequency >= 10  -- Focus on frequent merchants
ORDER BY m.transaction_frequency DESC, m.total_amount DESC
LIMIT 25;

-- =====================================================
-- PRODUCTION IMPROVEMENTS
-- =====================================================

-- Create materialized view for daily refresh
CREATE OR REPLACE SECURE VIEW TYSONS_MERCHANT_CLASSIFICATION AS
SELECT 
  t.PERIODID,
  t.DAF_MERCH_CAT,
  t.DAF_MERCH_ZIP_CODE,
  t.DAF_MERCH_NAME,
  t.DAF_MERCH_CITY,
  t.DAF_MERCH_STATE_COUNTRY,
  t.DAF_AMOUNT,
  t.PERIODDATE,
  
  COALESCE(c.matched_brand, 'UNMATCHED') as matched_brand,
  COALESCE(c.tysons_classification, 'Outside TysonsCC') as tysons_classification,
  COALESCE(c.overall_confidence_score, 0) as confidence_score

FROM PTX_DB.L0_RAW_SCH.RAW_AUTHFILE t
LEFT JOIN classified_matches c ON t.DAF_MERCH_NAME = c.DAF_MERCH_NAME
WHERE t.DAF_MERCH_STATE_COUNTRY = 'VA'
  AND t.PERIODDATE >= '2024-01-01';

-- Performance optimization: Create indexes on embedding tables
CREATE INDEX IF NOT EXISTS idx_merchant_name ON 
  SBOX_CONSUMERBANKING_BI.DEPOSITS_CARDS_CHNL_PROD.tysons_merchants_embeddings(DAF_MERCH_NAME);

CREATE INDEX IF NOT EXISTS idx_brand_raw ON 
  SBOX_CONSUMERBANKING_BI.DEPOSITS_CARDS_CHNL_PROD.tysons_brands_embeddings(BRAND_RAW);
```
