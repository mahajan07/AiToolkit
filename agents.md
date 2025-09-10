```bash
-- =====================================================
-- OPTIMIZED TYSONS CORNER CLASSIFICATION
-- Using sandbox tables and improved cleaning function
-- =====================================================

-- Step 1: Create improved cleaning function
CREATE OR REPLACE FUNCTION clean_merchant_name_v2(s STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
  TRIM(
    REGEXP_REPLACE(
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            UPPER(COALESCE(s,'')),
            '(\\+?1[\\-\\.\\s]?)?\\(?\\d{3}\\)?[\\-\\.\\s]?\\d{3}[\\-\\.\\s]?\\d{4}', ' '),  -- phone
          '(#\\d+|[0-9]{2,})', ' '),                                                     -- store numbers / long numbers
        '\\b(TYSONS|TYSON\\'?S|MCLEAN|VIENNA|CORNER|CENTER|CTR|MALL|UNITED STATES|USA|VA)\\b', ' '), -- stopwords
      '[^A-Z& ]', ' '),                                                                  -- keep letters, space, &
    '\\s+', ' ')                                                                          -- squeeze spaces
  )
$$;

-- Step 2: Create sandbox table for cleaned brands (run once, then reuse)
CREATE OR REPLACE TABLE SBOX_CONSUMERBANKING_BI.DEPOSITS_CARDS_CHNL_PROD.tysons_brands_cleaned AS
SELECT 
  BRAND_RAW,
  clean_merchant_name_v2(BRAND_RAW) as cleaned_brand_name,
  LENGTH(BRAND_RAW) as brand_length,
  CURRENT_TIMESTAMP() as created_at
FROM SBOX_CONSUMERBANKING_BI.DEPOSITS_CARDS_CHNL_PROD.tysons_brand_directory
WHERE clean_merchant_name_v2(BRAND_RAW) IS NOT NULL 
  AND LENGTH(clean_merchant_name_v2(BRAND_RAW)) > 2;  -- Avoid very short brand names

-- Step 3: Create sandbox table for cleaned transactions (run daily/weekly)
CREATE OR REPLACE TABLE SBOX_CONSUMERBANKING_BI.DEPOSITS_CARDS_CHNL_PROD.tysons_transactions_cleaned AS
SELECT 
  PERIODID,
  DAF_MERCH_CAT,
  DAF_MERCH_ZIP_CODE,
  DAF_MERCH_NAME,
  DAF_MERCH_CITY,
  DAF_MERCH_STATE_COUNTRY,
  DAF_AMOUNT,
  PERIODDATE,
  
  -- Cleaned merchant name
  clean_merchant_name_v2(DAF_MERCH_NAME) as cleaned_merchant_name,
  
  -- Pre-computed geographic confidence (numeric for performance)
  CASE 
    WHEN DAF_MERCH_ZIP_CODE IN ('22102-4501', '22102-4601', '22102-4698', '22102-4603', '22102-5953') THEN 3
    WHEN LEFT(DAF_MERCH_ZIP_CODE, 5) IN ('22102', '22182') THEN 2
    WHEN DAF_MERCH_STATE_COUNTRY = 'VA' 
         AND DAF_MERCH_CITY IN ('MCLEAN', 'VIENNA', 'TYSONS', 'TYSONS CORNER') THEN 1
    ELSE 0
  END AS geo_confidence_level,
  
  CURRENT_TIMESTAMP() as processed_at
  
FROM PTX_DB.L0_RAW_SCH.RAW_AUTHFILE
WHERE DAF_MERCH_STATE_COUNTRY = 'VA'
  AND PERIODDATE >= '2024-01-01'  -- Adjust date range as needed
  AND (
    LEFT(DAF_MERCH_ZIP_CODE, 5) IN ('22102', '22182') 
    OR DAF_MERCH_CITY IN ('MCLEAN', 'VIENNA', 'TYSONS', 'TYSONS CORNER')
  )
  AND clean_merchant_name_v2(DAF_MERCH_NAME) IS NOT NULL
  AND LENGTH(clean_merchant_name_v2(DAF_MERCH_NAME)) > 2;

-- Step 4: Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_cleaned_merchant ON 
  SBOX_CONSUMERBANKING_BI.DEPOSITS_CARDS_CHNL_PROD.tysons_transactions_cleaned(cleaned_merchant_name);

CREATE INDEX IF NOT EXISTS idx_geo_level ON 
  SBOX_CONSUMERBANKING_BI.DEPOSITS_CARDS_CHNL_PROD.tysons_transactions_cleaned(geo_confidence_level);

CREATE INDEX IF NOT EXISTS idx_cleaned_brand ON 
  SBOX_CONSUMERBANKING_BI.DEPOSITS_CARDS_CHNL_PROD.tysons_brands_cleaned(cleaned_brand_name);

-- =====================================================
-- FAST MATCHING QUERY (using pre-computed tables)
-- =====================================================

-- Step 5: Ultra-fast matching using sandbox tables
WITH potential_matches AS (
  SELECT 
    t.PERIODID,
    t.DAF_MERCH_CAT,
    t.DAF_MERCH_ZIP_CODE,
    t.DAF_MERCH_NAME,
    t.DAF_MERCH_CITY,
    t.DAF_MERCH_STATE_COUNTRY,
    t.DAF_AMOUNT,
    t.cleaned_merchant_name,
    t.geo_confidence_level,
    
    b.BRAND_RAW,
    b.cleaned_brand_name,
    
    -- Calculate match score (prioritize exact > contains > fuzzy)
    CASE 
      WHEN t.cleaned_merchant_name = b.cleaned_brand_name THEN 100
      WHEN CONTAINS(t.cleaned_merchant_name, b.cleaned_brand_name) THEN 90
      WHEN CONTAINS(b.cleaned_brand_name, t.cleaned_merchant_name) THEN 85
      ELSE GREATEST(0, ROUND(JAROWINKLER_SIMILARITY(t.cleaned_merchant_name, b.cleaned_brand_name) * 100, 0))
    END as name_match_score,
    
    -- Add brand length for tie-breaking (prefer longer, more specific brands)
    b.brand_length,
    
    -- Row number to get best match per transaction
    ROW_NUMBER() OVER (
      PARTITION BY t.PERIODID, t.DAF_MERCH_NAME
      ORDER BY 
        CASE 
          WHEN t.cleaned_merchant_name = b.cleaned_brand_name THEN 100
          WHEN CONTAINS(t.cleaned_merchant_name, b.cleaned_brand_name) THEN 90
          WHEN CONTAINS(b.cleaned_brand_name, t.cleaned_merchant_name) THEN 85
          ELSE GREATEST(0, ROUND(JAROWINKLER_SIMILARITY(t.cleaned_merchant_name, b.cleaned_brand_name) * 100, 0))
        END DESC,
        b.brand_length DESC,
        b.BRAND_RAW ASC  -- Consistent ordering for ties
    ) as match_rank
    
  FROM SBOX_CONSUMERBANKING_BI.DEPOSITS_CARDS_CHNL_PROD.tysons_transactions_cleaned t
  INNER JOIN SBOX_CONSUMERBANKING_BI.DEPOSITS_CARDS_CHNL_PROD.tysons_brands_cleaned b
    ON (
      -- Efficient join conditions to avoid cartesian product
      t.cleaned_merchant_name = b.cleaned_brand_name
      OR CONTAINS(t.cleaned_merchant_name, b.cleaned_brand_name)
      OR CONTAINS(b.cleaned_brand_name, t.cleaned_merchant_name)
      OR (LENGTH(b.cleaned_brand_name) > 3 
          AND JAROWINKLER_SIMILARITY(t.cleaned_merchant_name, b.cleaned_brand_name) > 0.8)
    )
  WHERE t.PERIODDATE >= '2025-01-01'  -- Recent data only for testing
),

best_matches AS (
  SELECT *
  FROM potential_matches
  WHERE match_rank = 1  -- Only the best match per transaction
    AND name_match_score >= 75  -- Minimum confidence threshold
)

-- Final classification
SELECT 
  PERIODID,
  DAF_MERCH_CAT,
  DAF_MERCH_ZIP_CODE,
  DAF_MERCH_NAME,
  DAF_MERCH_CITY,
  DAF_MERCH_STATE_COUNTRY,
  DAF_AMOUNT,
  cleaned_merchant_name,
  BRAND_RAW as matched_brand,
  
  -- Overall confidence score
  CASE 
    WHEN geo_confidence_level = 3 AND name_match_score >= 90 THEN 95
    WHEN geo_confidence_level = 3 AND name_match_score >= 75 THEN 90
    WHEN geo_confidence_level = 2 AND name_match_score >= 95 THEN 90
    WHEN geo_confidence_level = 2 AND name_match_score >= 85 THEN 85
    WHEN geo_confidence_level = 2 AND name_match_score >= 75 THEN 80
    WHEN geo_confidence_level = 1 AND name_match_score >= 95 THEN 75
    WHEN name_match_score = 100 THEN 70  -- Perfect name match but no geo
    ELSE GREATEST(50, name_match_score - 10)  -- Minimum viable confidence
  END as overall_confidence_score,
  
  -- Classification
  CASE 
    WHEN (geo_confidence_level >= 2 AND name_match_score >= 75)
      OR (geo_confidence_level = 1 AND name_match_score >= 95)
      OR name_match_score = 100 THEN 'In TysonsCC'
    ELSE 'Outside TysonsCC'
  END as tysons_classification,
  
  -- Debug info
  CASE geo_confidence_level 
    WHEN 3 THEN 'HIGH_GEO' 
    WHEN 2 THEN 'MEDIUM_GEO' 
    WHEN 1 THEN 'LOW_GEO' 
    ELSE 'NO_GEO' 
  END as geo_level,
  name_match_score

FROM best_matches
ORDER BY overall_confidence_score DESC, DAF_AMOUNT DESC
LIMIT 100;

-- =====================================================
-- QUICK VALIDATION QUERIES
-- =====================================================

-- Check data quality after cleaning
SELECT 
  'Brands' as table_type,
  COUNT(*) as total_records,
  COUNT(DISTINCT cleaned_brand_name) as unique_cleaned_names,
  AVG(LENGTH(cleaned_brand_name)) as avg_cleaned_length
FROM SBOX_CONSUMERBANKING_BI.DEPOSITS_CARDS_CHNL_PROD.tysons_brands_cleaned

UNION ALL

SELECT 
  'Transactions' as table_type,
  COUNT(*) as total_records,
  COUNT(DISTINCT cleaned_merchant_name) as unique_cleaned_names,
  AVG(LENGTH(cleaned_merchant_name)) as avg_cleaned_length
FROM SBOX_CONSUMERBANKING_BI.DEPOSITS_CARDS_CHNL_PROD.tysons_transactions_cleaned
WHERE PERIODDATE >= '2025-01-01';

-- Sample of cleaned vs original names
SELECT 
  DAF_MERCH_NAME as original_name,
  cleaned_merchant_name as cleaned_name,
  COUNT(*) as frequency
FROM SBOX_CONSUMERBANKING_BI.DEPOSITS_CARDS_CHNL_PROD.tysons_transactions_cleaned
WHERE PERIODDATE >= '2025-01-01'
GROUP BY 1, 2
ORDER BY frequency DESC
LIMIT 20;

-- =====================================================
-- INCREMENTAL UPDATE PROCEDURE (for production)
-- =====================================================

-- For daily updates, create this procedure
CREATE OR REPLACE PROCEDURE update_tysons_transactions_incremental(start_date DATE)
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
  -- Delete existing data for the date range
  DELETE FROM SBOX_CONSUMERBANKING_BI.DEPOSITS_CARDS_CHNL_PROD.tysons_transactions_cleaned
  WHERE PERIODDATE >= start_date;
  
  -- Insert new cleaned data
  INSERT INTO SBOX_CONSUMERBANKING_BI.DEPOSITS_CARDS_CHNL_PROD.tysons_transactions_cleaned
  SELECT 
    PERIODID, DAF_MERCH_CAT, DAF_MERCH_ZIP_CODE, DAF_MERCH_NAME,
    DAF_MERCH_CITY, DAF_MERCH_STATE_COUNTRY, DAF_AMOUNT, PERIODDATE,
    clean_merchant_name_v2(DAF_MERCH_NAME) as cleaned_merchant_name,
    CASE 
      WHEN DAF_MERCH_ZIP_CODE IN ('22102-4501', '22102-4601', '22102-4698', '22102-4603', '22102-5953') THEN 3
      WHEN LEFT(DAF_MERCH_ZIP_CODE, 5) IN ('22102', '22182') THEN 2
      WHEN DAF_MERCH_STATE_COUNTRY = 'VA' 
           AND DAF_MERCH_CITY IN ('MCLEAN', 'VIENNA', 'TYSONS', 'TYSONS CORNER') THEN 1
      ELSE 0
    END AS geo_confidence_level,
    CURRENT_TIMESTAMP() as processed_at
  FROM PTX_DB.L0_RAW_SCH.RAW_AUTHFILE
  WHERE DAF_MERCH_STATE_COUNTRY = 'VA'
    AND PERIODDATE >= start_date
    AND (LEFT(DAF_MERCH_ZIP_CODE, 5) IN ('22102', '22182') 
         OR DAF_MERCH_CITY IN ('MCLEAN', 'VIENNA', 'TYSONS', 'TYSONS CORNER'))
    AND clean_merchant_name_v2(DAF_MERCH_NAME) IS NOT NULL
    AND LENGTH(clean_merchant_name_v2(DAF_MERCH_NAME)) > 2;
    
  RETURN 'Successfully updated transactions from ' || start_date;
END;
$$;

-- Usage: CALL update_tysons_transactions_incremental('2025-09-01');
```
