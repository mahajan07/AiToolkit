-- 1) Tokenize each name into distinct word sets
with vd_tokens as (
  select
    VENDOR,
    CITY,
    array_agg(distinct value) as tokens
  from VENDOR_DIRECTORY,
       lateral split_to_table(VENDOR, ' ')
  group by 1,2
),
tx_tokens as (
  select
    DAF_MERCH_NAME,
    DAF_MERCH_CITY,
    array_agg(distinct value) as tokens
  from TRANSACTIONS,
       lateral split_to_table(DAF_MERCH_NAME, ' ')
  group by 1,2
),

-- 2) Build candidate pairs by location (city match here; switch to ZIP if better)
pairs as (
  select
    v.VENDOR, v.CITY as vendor_city, v.tokens as v_tokens,
    t.DAF_MERCH_NAME, t.DAF_MERCH_CITY as tx_city, t.tokens as t_tokens
  from vd_tokens v
  join tx_tokens t on v.CITY = t.DAF_MERCH_CITY
),

-- 3) Compute Jaccard = |intersection| / |union|
--    We do this by exploding arrays, counting overlaps, and recombining.
intersections as (
  select
    p.VENDOR, p.vendor_city, p.DAF_MERCH_NAME, p.tx_city,
    count(distinct vt.value) as inter_cnt
  from pairs p,
       lateral flatten(input => p.v_tokens) vt,
       lateral flatten(input => p.t_tokens) tt
  where vt.value = tt.value
  group by 1,2,3,4
),
sizes as (
  select
    VENDOR, vendor_city, DAF_MERCH_NAME, tx_city,
    -- sizes of sets
    (select count(distinct value) from lateral flatten(input => v_tokens)) as v_size,
    (select count(distinct value) from lateral flatten(input => t_tokens)) as t_size
  from pairs
)
select
  s.VENDOR, s.vendor_city, s.DAF_MERCH_NAME, s.tx_city,
  i.inter_cnt,
  (s.v_size + s.t_size - i.inter_cnt) as union_cnt,
  (i.inter_cnt * 1.0) / nullif((s.v_size + s.t_size - i.inter_cnt),0) as jaccard
from sizes s
left join intersections i
  on s.VENDOR = i.VENDOR and s.DAF_MERCH_NAME = i.DAF_MERCH_NAME
qualify row_number() over (partition by s.VENDOR order by jaccard desc) = 1
-- Where jaccard >= 0.5 is a good starting point
order by jaccard desc;
