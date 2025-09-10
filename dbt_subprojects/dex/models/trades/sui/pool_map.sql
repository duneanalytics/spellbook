{{ config(
  materialized='table',
  file_format='delta',
  tags=['sui','dex'],
  schema='dex_sui'
) }}

-- 1) limit to pools actually referenced by your decoded trades
with used_pools as (
  select distinct pool_id
  from {{ ref('dex_sui_raw_base_trades') }}
  where pool_id is not null
),

-- 2) grab the latest row per pool_id from objects
latest as (
  select
      o.object_id,
      o.type_,
      o.checkpoint,
      row_number() over (partition by o.object_id order by o.checkpoint desc) as rn
  from {{ source('sui','objects') }} o
  join used_pools u
    on o.object_id = u.pool_id
  -- only types that even *could* have generics
  where strpos(o.type_, '<') > 0
),

-- 3) extract the full generic string inside <…>
parsed as (
  select
      object_id as pool_id,
      type_,
      regexp_extract(type_, '<(.*)>', 1) as generics
  from latest
  where rn = 1
),

-- 4) split the generics by comma -> first two tokens are our A/B coin types
split as (
  select
      pool_id,
      type_,
      element_at(split(generics, ','), 1) as coin_type_a_raw,
      element_at(split(generics, ','), 2) as coin_type_b_raw
  from parsed
),

-- 5) keep only pool-ish structs to avoid false positives
filtered as (
  select
      pool_id,
      lower(trim(coin_type_a_raw)) as coin_type_a,
      lower(trim(coin_type_b_raw)) as coin_type_b
  from split
  where coin_type_a_raw is not null
    and coin_type_b_raw is not null
    and (
      type_ like '%::pool::%' or
      type_ like '%::clpool::%' or
      type_ like '%::clmm::%'  or
      type_ like '%::amm::%'   or
      type_ like '%::pair::%'  or
      type_ like '%::obric::%'
    )
)

select distinct pool_id, coin_type_a, coin_type_b
from filtered
