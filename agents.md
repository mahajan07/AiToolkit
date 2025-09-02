Totally—Cortex can do this two solid ways. Below are copy-paste Snowflake SQL snippets that use Cortex the right way (no city/ZIP). They fix the syntax issues in your screenshot and give you “Abercrombie & Fitch ↔ A&F ↔ Fitch” style matches.


---

1) Embedding match (recommended)

Use Cortex embeddings to vectorize every brand and every merchant name, then take the nearest merchant per brand by cosine similarity. This naturally catches abbreviations like A&F.

-- Choose a Snowflake-hosted embedding model
SET MODEL := 'snowflake-arctic-embed-m-v1.5';

-- Brands → embeddings
CREATE OR REPLACE TEMP TABLE BRAND_EMB AS
SELECT
  BRANDS                                AS BRAND_RAW,
  AI_EMBED($MODEL, BRANDS)              AS BRAND_VEC
FROM "SBOX_CONSUMERBANKING_DB"."DEPOSITS_CARDS_CHNL_PROD"."TYSONS_STORE_DIR_DM"
WHERE BRANDS IS NOT NULL;

-- Merchants → embeddings
CREATE OR REPLACE TEMP TABLE MERCH_EMB AS
SELECT
  DAF_MERCH_NAME                        AS MERCH_RAW,
  AI_EMBED($MODEL, DAF_MERCH_NAME)      AS MERCH_VEC
FROM PTX_DB.DBO.RAW_SCH.AUTHFILE
WHERE DAF_MERCH_NAME IS NOT NULL;

-- (Optional) cheap candidate reducer so we don’t do a full cartesian
WITH B AS (
  SELECT BRAND_RAW, BRAND_VEC, SUBSTR(UPPER(BRAND_RAW),1,1) AS K FROM BRAND_EMB
),
M AS (
  SELECT MERCH_RAW, MERCH_VEC, SUBSTR(UPPER(MERCH_RAW),1,1) AS K FROM MERCH_EMB
)
SELECT
  B.BRAND_RAW,
  M.MERCH_RAW,
  VECTOR_COSINE_SIMILARITY(B.BRAND_VEC, M.MERCH_VEC) AS COS_SIM
FROM B JOIN M USING (K)
QUALIFY ROW_NUMBER() OVER (PARTITION BY B.BRAND_RAW ORDER BY COS_SIM DESC) = 1
ORDER BY COS_SIM DESC;

Confidence bands (tweak after a spot-check):

SELECT
  BRAND_RAW AS BRAND,
  MERCH_RAW AS BEST_MATCH_MERCHANT,
  COS_SIM,
  CASE WHEN COS_SIM >= 0.90 THEN 'HIGH'
       WHEN COS_SIM >= 0.85 THEN 'MEDIUM'
       ELSE 'LOW' END AS CONFIDENCE
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));


---

2) LLM canonicalization + similarity (nice complement)

Have Cortex extract just the brand from each messy merchant string (drop store numbers, symbols), then fuzzy-match to your directory. This fixes cases like “APPLE STORE #R010” → “APPLE” before matching.

> In SQL, call the scalar function SNOWFLAKE.CORTEX.COMPLETE with (model, prompt_text)
or SNOWFLAKE.CORTEX.EXTRACT_ANSWER(question, context).
Your earlier syntax error happened because WHERE cannot contain window functions and because of minor punctuation around the function call.



-- 2a) Canonicalize merchant → brand-ish name with LLM
CREATE OR REPLACE TEMP TABLE MERCH_CANON AS
SELECT
  DAF_MERCH_NAME AS MERCH_RAW,
  SNOWFLAKE.CORTEX.EXTRACT_ANSWER(
    'Give only the merchant brand name (no store #, location, or descriptors). Examples: "A&F"→"ABERCROMBIE & FITCH", "APPLE STORE #R010"→"APPLE", "CMX CINEMAS"→"CMX".',
    DAF_MERCH_NAME
  ) AS MERCH_BRAND
FROM PTX_DB.DBO.RAW_SCH.AUTHFILE
WHERE DAF_MERCH_NAME IS NOT NULL;

-- 2b) Best match from your brand list using Jaro–Winkler
WITH DIR AS (
  SELECT UPPER(BRANDS) AS BRAND FROM "SBOX_CONSUMERBANKING_DB"."DEPOSITS_CARDS_CHNL_PROD"."TYSONS_STORE_DIR_DM"
),
C AS (
  SELECT UPPER(MERCH_BRAND) AS MERCH_BRAND, MERCH_RAW FROM MERCH_CANON WHERE MERCH_BRAND IS NOT NULL
)
SELECT
  C.MERCH_RAW,
  C.MERCH_BRAND,
  D.BRAND,
  JARO_WINKLER_SIMILARITY(C.MERCH_BRAND, D.BRAND) AS SCORE
FROM C JOIN D ON TRUE
QUALIFY ROW_NUMBER() OVER (PARTITION BY C.MERCH_RAW ORDER BY SCORE DESC) = 1
ORDER BY SCORE DESC;

Add a confidence:

SELECT *,
  CASE WHEN SCORE >= 0.92 THEN 'HIGH'
       WHEN SCORE >= 0.86 THEN 'MEDIUM'
       ELSE 'LOW' END AS CONFIDENCE
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));


---

Why these work for “Abercrombie & Fitch ↔ A&F ↔ Fitch”

Embeddings capture semantics and abbreviations (A&F, Fitch) → high cosine to “Abercrombie & Fitch”.

Canonicalization strips noise (“STORE #10647”, “*TYSONS PLAYGROUND”) before a simpler character similarity.



---

Quick fixes for the errors you saw

Use the fully-qualified function: SNOWFLAKE.CORTEX.COMPLETE('model','prompt') or SNOWFLAKE.CORTEX.EXTRACT_ANSWER(q, context).

Don’t put window functions in a WHERE; use QUALIFY.

If you SET prompt='...', reference it as $prompt (no brackets) in SQL strings.

End statements with semicolons in Worksheets.



---

If you want top-k candidates per brand (for review), change ROW_NUMBER() = 1 to <= 3. I can also add a tiny “brand variants” expansion (A&F / AF / Fitch) on the query side for an even stronger embedding match if your directory has many short names.

