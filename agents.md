```bash
-- =====================================================
-- TYSONS CORNER CENTER TRANSACTION CLASSIFICATION
-- Multi-stage approach: Geographic Filter → Name Matching
-- =====================================================

-- Step 1: Create helper function for cleaning merchant names
CREATE OR REPLACE FUNCTION CLEAN_MERCHANT_NAME(merchant_name STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
  UPPER(
    REGEXP_REPLACE(
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(merchant_name, '\\s*#\\d+.*$', ''), -- Remove store numbers and everything after
            '\\s+(VA|VIRGINIA|MD|MARYLAND|DC)\\s*$', ''), -- Remove state suffixes
          '\\s+(MCLEAN|VIENNA|TYSONS|FALLS CHURCH).*$', ''), -- Remove city suffixes
        '[^A-Z0-9\\s&]', ''), -- Remove special characters except & and spaces
      '\\s+', ' ') -- Normalize whitespace
    )
$$;

-- Step 2: Main classification query with confidence scoring
WITH geographic_filter AS (
  SELECT 
    *,
    CASE 
      -- High confidence geographic indicators
      WHEN DAF_MERCH_ZIP_CODE IN ('22102-4501', '22102-4601', '22102-4698', '22102-4603', '22102-5953') THEN 'HIGH_GEO_CONFIDENCE'
      -- Medium confidence (primary Tysons ZIP codes)
      WHEN LEFT(DAF_MERCH_ZIP_CODE, 5) IN ('22102', '22182') THEN 'MEDIUM_GEO_CONFIDENCE'
      -- Low confidence (broader area)
      WHEN DAF_MERCH_STATE_COUNTRY = 'VA' 
           AND DAF_MERCH_CITY IN ('MCLEAN', 'VIENNA', 'TYSONS', 'TYSONS CORNER') THEN 'LOW_GEO_CONFIDENCE'
      ELSE 'NO_GEO_MATCH'
    END AS geo_confidence_level
  FROM PTX_DB.L0_RAW_SCH.RAW_AUTHFILE
  WHERE DAF_MERCH_STATE_COUNTRY = 'VA'
),

brand_matching AS (
  SELECT 
    t.*,
    b.BRAND_RAW as matched_brand,
    CLEAN_MERCHANT_NAME(t.DAF_MERCH_NAME) as cleaned_merchant_name,
    
    -- Fuzzy matching with different algorithms
    EDITDISTANCE(CLEAN_MERCHANT_NAME(t.DAF_MERCH_NAME), UPPER(b.BRAND_RAW)) as edit_distance,
    JAROWINKLER_SIMILARITY(CLEAN_MERCHANT_NAME(t.DAF_MERCH_NAME), UPPER(b.BRAND_RAW)) as jaro_similarity,
    
    -- Contains matching (for partial matches)
    CASE 
      WHEN CLEAN_MERCHANT_NAME(t.DAF_MERCH_NAME) = UPPER(b.BRAND_RAW) THEN 100
      WHEN CONTAINS(CLEAN_MERCHANT_NAME(t.DAF_MERCH_NAME), UPPER(b.BRAND_RAW)) 
           OR CONTAINS(UPPER(b.BRAND_RAW), CLEAN_MERCHANT_NAME(t.DAF_MERCH_NAME)) THEN 85
      ELSE 0
    END as contains_match_score,
    
    -- Calculate overall name matching confidence
    GREATEST(
      CASE WHEN CLEAN_MERCHANT_NAME(t.DAF_MERCH_NAME) = UPPER(b.BRAND_RAW) THEN 100 ELSE 0 END,
      CASE WHEN CONTAINS(CLEAN_MERCHANT_NAME(t.DAF_MERCH_NAME), UPPER(b.BRAND_RAW)) 
                OR CONTAINS(UPPER(b.BRAND_RAW), CLEAN_MERCHANT_NAME(t.DAF_MERCH_NAME)) THEN 85 ELSE 0 END,
      ROUND(JAROWINKLER_SIMILARITY(CLEAN_MERCHANT_NAME(t.DAF_MERCH_NAME), UPPER(b.BRAND_RAW)) * 100, 2),
      CASE WHEN EDITDISTANCE(CLEAN_MERCHANT_NAME(t.DAF_MERCH_NAME), UPPER(b.BRAND_RAW)) <= 2 THEN 75 ELSE 0 END
    ) as name_confidence_score
    
  FROM geographic_filter t
  LEFT JOIN SBOX_CONSUMERBANKING_BI.DEPOSITS_CARDS_CHNL_PROD.tysons_brand_directory b
    ON (
      -- Direct match
      CLEAN_MERCHANT_NAME(t.DAF_MERCH_NAME) = UPPER(b.BRAND_RAW)
      -- Contains match
      OR CONTAINS(CLEAN_MERCHANT_NAME(t.DAF_MERCH_NAME), UPPER(b.BRAND_RAW))
      OR CONTAINS(UPPER(b.BRAND_RAW), CLEAN_MERCHANT_NAME(t.DAF_MERCH_NAME))
      -- Fuzzy match (Jaro-Winkler similarity > 0.8)
      OR JAROWINKLER_SIMILARITY(CLEAN_MERCHANT_NAME(t.DAF_MERCH_NAME), UPPER(b.BRAND_RAW)) > 0.8
      -- Edit distance <= 3 for reasonable fuzzy matching
      OR (EDITDISTANCE(CLEAN_MERCHANT_NAME(t.DAF_MERCH_NAME), UPPER(b.BRAND_RAW)) <= 3 
          AND LENGTH(UPPER(b.BRAND_RAW)) > 4)
    )
),

final_classification AS (
  SELECT 
    *,
    -- Calculate combined confidence score
    CASE 
      WHEN geo_confidence_level = 'HIGH_GEO_CONFIDENCE' AND name_confidence_score >= 75 THEN 95
      WHEN geo_confidence_level = 'HIGH_GEO_CONFIDENCE' AND name_confidence_score >= 50 THEN 85
      WHEN geo_confidence_level = 'MEDIUM_GEO_CONFIDENCE' AND name_confidence_score >= 85 THEN 90
      WHEN geo_confidence_level = 'MEDIUM_GEO_CONFIDENCE' AND name_confidence_score >= 75 THEN 80
      WHEN geo_confidence_level = 'LOW_GEO_CONFIDENCE' AND name_confidence_score >= 90 THEN 75
      WHEN name_confidence_score = 100 THEN 70 -- Perfect name match but no geo confirmation
      ELSE 0
    END as overall_confidence_score,
    
    -- Final classification
    CASE 
      WHEN (geo_confidence_level = 'HIGH_GEO_CONFIDENCE' AND name_confidence_score >= 50)
        OR (geo_confidence_level = 'MEDIUM_GEO_CONFIDENCE' AND name_confidence_score >= 75)
        OR (geo_confidence_level = 'LOW_GEO_CONFIDENCE' AND name_confidence_score >= 90) THEN 'In TysonsCC'
      ELSE 'Outside TysonsCC'
    END as tysons_classification
  FROM brand_matching
)

SELECT 
  PERIODID,
  DAF_MERCH_CAT,
  DAF_MERCH_ZIP_CODE,
  DAF_MERCH_NAME,
  DAF_MERCH_CITY,
  DAF_MERCH_STATE_COUNTRY,
  DAF_AMOUNT,
  
  -- Classification results
  tysons_classification,
  overall_confidence_score,
  
  -- Debug information
  cleaned_merchant_name,
  matched_brand,
  geo_confidence_level,
  name_confidence_score,
  edit_distance,
  jaro_similarity,
  contains_match_score
  
FROM final_classification
WHERE overall_confidence_score > 0  -- Only show potential matches
ORDER BY overall_confidence_score DESC, DAF_AMOUNT DESC;

-- =====================================================
-- ALTERNATIVE: SIMPLIFIED VERSION FOR PRODUCTION
-- =====================================================

-- If you want a simpler, faster query for production use:
CREATE OR REPLACE VIEW TYSONS_TRANSACTIONS_CLASSIFIED AS
WITH base_filter AS (
  SELECT *,
    UPPER(REGEXP_REPLACE(REGEXP_REPLACE(DAF_MERCH_NAME, '\\s*#\\d+.*$', ''), '[^A-Z0-9\\s&]', '')) as clean_name
  FROM PTX_DB.L0_RAW_SCH.RAW_AUTHFILE
  WHERE DAF_MERCH_STATE_COUNTRY = 'VA'
    AND (LEFT(DAF_MERCH_ZIP_CODE, 5) IN ('22102', '22182')
         OR DAF_MERCH_CITY IN ('MCLEAN', 'VIENNA', 'TYSONS', 'TYSONS CORNER'))
)

SELECT 
  t.*,
  CASE 
    WHEN b.BRAND_RAW IS NOT NULL 
         AND (DAF_MERCH_ZIP_CODE LIKE '22102-%' OR LEFT(DAF_MERCH_ZIP_CODE, 5) = '22102') 
    THEN 'In TysonsCC'
    ELSE 'Outside TysonsCC'
  END as tysons_classification,
  
  CASE 
    WHEN b.BRAND_RAW IS NOT NULL AND DAF_MERCH_ZIP_CODE LIKE '22102-%' THEN 90
    WHEN b.BRAND_RAW IS NOT NULL AND LEFT(DAF_MERCH_ZIP_CODE, 5) = '22102' THEN 75
    ELSE 0
  END as confidence_score

FROM base_filter t
LEFT JOIN SBOX_CONSUMERBANKING_BI.DEPOSITS_CARDS_CHNL_PROD.tysons_brand_directory b
  ON (t.clean_name LIKE '%' || UPPER(b.BRAND_RAW) || '%' 
      OR UPPER(b.BRAND_RAW) LIKE '%' || t.clean_name || '%'
      OR JAROWINKLER_SIMILARITY(t.clean_name, UPPER(b.BRAND_RAW)) > 0.8);

-- =====================================================
-- VALIDATION QUERIES
-- =====================================================

-- Check matching effectiveness
SELECT 
  tysons_classification,
  COUNT(*) as transaction_count,
  COUNT(DISTINCT matched_brand) as unique_brands_matched,
  AVG(overall_confidence_score) as avg_confidence,
  SUM(DAF_AMOUNT) as total_amount
FROM final_classification
GROUP BY tysons_classification;

-- Review low confidence matches for tuning
SELECT 
  DAF_MERCH_NAME,
  cleaned_merchant_name,
  matched_brand,
  geo_confidence_level,
  name_confidence_score,
  overall_confidence_score,
  COUNT(*) as frequency
FROM final_classification
WHERE overall_confidence_score BETWEEN 50 AND 75
GROUP BY 1,2,3,4,5,6
ORDER BY frequency DESC, overall_confidence_score DESC
LIMIT 50
```
