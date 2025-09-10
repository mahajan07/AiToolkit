```bash
-- ================================
-- Tysons: Merchant → Brand matcher
-- One script, low complexity
-- ================================

-- Helper UDFs (temporary to your session)
create or replace temporary function ZIP5(s string)
returns string
language sql
as $$
  regexp_substr(coalesce(s,''), '(\\d{5})')
$$;

create or replace temporary function CANON(s string)
returns string
language sql
as $$
  -- Uppercase, strip phones / numbers / mall-location words, keep letters, spaces, and '&'
  regexp_replace(
    regexp_replace(
      regexp_replace(
        regexp_replace(
          upper(coalesce(s,'')),
          '(\\+?1[\\-\\.\\s]?)?\\(?\\d{3}\\)?[\\-\\.\\s]?\\d{3}[\\-\\.\\s]?\\d{4}', ' '),  -- phone
        '(#\\d+|[0-9]{2,})', ' '),                                                       -- store numbers / long nums
      '\\b(TYSONS|TYSON\\'?S|MCLEAN|VIENNA|CORNER|CENTER|CTR|MALL|UNITED STATES|USA|VA)\\b', ' '),
    '[^A-Z& ]', ' '                                                                    -- keep A-Z, space, &
  )
$$;

with
-- 1) Brand directory (normalize once)
BRANDS as (
  select
    brands as brand_raw,
    trim(regexp_replace(CANON(brands), '\\s+', ' ')) as brand_norm,
    length(trim(regexp_replace(CANON(brands), '\\s+', ' '))) as brand_len
  from "SBOX_CONSUMERBANKING_BI"."DEPOSITS_CARDS_CHNL_PROD"."<TYSONS_BRAND_TABLE>"
  where brands is not null
),

-- Minimal alias map for common edge cases (kept tiny on purpose)
ALIASES as (
  select * from values
    ('AMC',                   'AMC THEATRES'),
    ('ARC TERYX',             'ARCTERYX'),
    ('AMERICAN EAGLE',        'AMERICAN EAGLE OUTFITTERS'),
    ('APPLE STORE',           'APPLE')
  as T(alias_norm_raw, brand_canonical_raw)
),
BRANDS_EXPANDED as (
  select brand_raw, brand_norm, brand_len from BRANDS
  union all
  select
    brand_canonical_raw as brand_raw,
    trim(regexp_replace(CANON(alias_norm_raw), '\\s+', ' ')) as brand_norm,
    length(trim(regexp_replace(CANON(alias_norm_raw), '\\s+', ' '))) as brand_len
  from ALIASES
),

-- 2) Transactions (ZIP gate + normalization)
TX as (
  select
    t.*,
    ZIP5(t.DAF_MERCH_ZIP_CODE) as zip5,
    trim(regexp_replace(CANON(t.DAF_MERCH_NAME), '\\s+', ' ')) as merch_norm
  from "PTX_DB1"."O_RAW"."SCH_RAW_AUTHFILE" t
  where upper(coalesce(t.DAF_MERCH_STATE_COUNTRY,'')) = 'VA'
    and (
      ZIP5(t.DAF_MERCH_ZIP_CODE) in ('22102','22182')
      or upper(coalesce(t.DAF_MERCH_CITY,'')) like '%TYSON%'
      or upper(coalesce(t.DAF_MERCH_NAME,'')) like '%TYSON%'
    )
),

-- 3) Single fuzzy pass (plus short-name safeguard)
CANDIDATES as (
  select
    t.*,
    b.brand_raw,
    b.brand_norm,
    JAROWINKLER_SIMILARITY(t.merch_norm, b.brand_norm) as jw,
    EDITDISTANCE(t.merch_norm, b.brand_norm)           as ed
  from TX t
  join BRANDS_EXPANDED b
    on (
         JAROWINKLER_SIMILARITY(t.merch_norm, b.brand_norm) >= 0.92
         or (b.brand_len <= 5 and EDITDISTANCE(t.merch_norm, b.brand_norm) <= 1)
       )
),

-- 4) Pick the single best brand per transaction (adjust partition key if you have a TXN_ID)
TOP1 as (
  select
    *,
    row_number() over (
      partition by merch_norm, zip5, coalesce(PERIODID,0), coalesce(DAF_AMOUNT,0)
      order by jw desc, ed asc, length(brand_norm) desc
    ) as rn
  from CANDIDATES
)

-- 5) Final output: top-1 match + confidence; safe UNKNOWN fallback
select
  t.PERIODID,
  t.DAF_MERCH_NAME,
  t.DAF_MERCH_CITY,
  t.DAF_MERCH_STATE_COUNTRY,
  t.DAF_MERCH_ZIP_CODE,
  t.zip5,
  t.DAF_AMOUNT,
  t.merch_norm,

  coalesce(x.brand_raw, 'UNKNOWN') as matched_brand,
  x.jw, x.ed,
  case
    when x.jw >= 0.96 then 'HIGH'
    when x.jw >= 0.92 then 'MEDIUM'
    when x.brand_raw is not null then 'LOW'
    else 'NONE'
  end as match_confidence,
  case when x.brand_raw is null then 1 else 0 end as is_unmatched
from TX t
left join TOP1 x
  on t.merch_norm = x.merch_norm
```
 and t.zip5       = x.zip5
 and x.rn = 1
;
