{{ config(
    schema='dex_sui',
    alias='pool_map',
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    unique_key=['pool_id'],
    tags=['sui','dex']
) }}

-- 1) Pools to resolve
with used_pools as (
  select distinct
      case when starts_with(lower(pool_id), '0x') then lower(pool_id)
           else concat('0x', lower(pool_id)) end as pool_id
  from {{ ref('dex_sui_base_trades') }}
  where pool_id is not null
  {% if is_incremental() %}
    and {{ incremental_predicate('block_time') }}
  {% endif %}
)

-- 2) Latest object row per pool_id
, latest as (
  select
      ('0x' || lower(to_hex(o.object_id))) as object_id_hex
      , cast(o.type_ as varchar)             as type_str
      , o.checkpoint
      , row_number() over (
        partition by ('0x' || lower(to_hex(o.object_id)))
        order by o.checkpoint desc
      ) as rn
  from {{ source('sui','objects') }} o
  join used_pools u
    on ('0x' || lower(to_hex(o.object_id))) = u.pool_id
  where strpos(cast(o.type_ as varchar), '<') > 0
)

-- 3) Extract generic type parameters
, parsed as (
  select
      object_id_hex as pool_id
      , type_str
      , regexp_extract(type_str, '<(.*)>', 1) as generics
  from latest
  where rn = 1
)

-- 4) Split first two generics (A,B)
, split as (
  select
      pool_id
      , type_str
      , trim(element_at(split(generics, ','), 1)) as coin_type_a_raw
      , trim(element_at(split(generics, ','), 2)) as coin_type_b_raw
  from parsed
)

-- 5) Unwrap 0x2::coin::Coin<…> → …
, unwrapped as (
  select
      pool_id
      , type_str
      , coalesce(regexp_extract(lower(coin_type_a_raw), '^0x2::coin::coin<(.*)>$', 1), lower(coin_type_a_raw)) as coin_type_a_u
      , coalesce(regexp_extract(lower(coin_type_b_raw), '^0x2::coin::coin<(.*)>$', 1), lower(coin_type_b_raw)) as coin_type_b_u
  from split
  where coin_type_a_raw is not null
    and coin_type_b_raw is not null
)

-- 6) Keep pool-ish structs and canonicalize addresses (strip leading zeros)
, filtered as (
  select
      pool_id
      , regexp_replace(coin_type_a_u, '^0x0*([0-9a-f]+)(::.*)$', '0x$1$2') as coin_type_a
      , regexp_replace(coin_type_b_u, '^0x0*([0-9a-f]+)(::.*)$', '0x$1$2') as coin_type_b
  from unwrapped
  where
        lower(type_str) like '%::pool::%'
     or lower(type_str) like '%::clpool::%'
     or lower(type_str) like '%::clmm::%'
     or lower(type_str) like '%::amm::%'
     or lower(type_str) like '%::pair::%'
     or lower(type_str) like '%::obric::%'
)

select distinct
    pool_id
    , coin_type_a
    , coin_type_b
from filtered