{{ config(
  materialized='table',
  file_format='delta',
  tags=['sui','dex'],
  schema='dex_sui'
) }}

-- 1) Limit to pools we actually see in swaps
with used_pools as (
  select distinct lower(pool_id) as pool_id
  from {{ ref('dex_sui_raw_base_trades') }}
  where pool_id is not null
),

-- 2) Latest row per object_id, join on normalized hex id
latest as (
  select
      -- normalize VARBINARY id -> '0x' + lowercase hex string
      ('0x' || lower(to_hex(o.object_id))) as object_id_hex,
      cast(o.type_ as varchar)             as type_str,
      o.checkpoint,
      row_number() over (
        partition by ('0x' || lower(to_hex(o.object_id)))
        order by o.checkpoint desc
      ) as rn
  from {{ source('sui','objects') }} o
  join used_pools u
    on ('0x' || lower(to_hex(o.object_id))) = u.pool_id
  where strpos(cast(o.type_ as varchar), '<') > 0  -- only types with generics
),

-- 3) Extract the full generics string between <…>
parsed as (
  select
      object_id_hex as pool_id,
      type_str,
      regexp_extract(type_str, '<(.*)>', 1) as generics
  from latest
  where rn = 1
),

-- 4) Split "A,B,..." and take first two as coin types
split as (
  select
      pool_id,
      type_str,
      element_at(split(generics, ','), 1) as coin_type_a_raw,
      element_at(split(generics, ','), 2) as coin_type_b_raw
  from parsed
),

-- 5) Keep pool-ish structs to avoid false positives
filtered as (
  select
      pool_id,
      cast(lower(trim(coin_type_a_raw)) as varbinary) as coin_type_a,
      cast(lower(trim(coin_type_b_raw)) as varbinary) as coin_type_b
  from split
  where coin_type_a_raw is not null
    and coin_type_b_raw is not null
    and (
      type_str like '%::pool::%' or
      type_str like '%::clpool::%' or
      type_str like '%::clmm::%'  or
      type_str like '%::amm::%'   or
      type_str like '%::pair::%'  or
      type_str like '%::obric::%'
    )
)

select distinct pool_id, coin_type_a, coin_type_b
from filtered
