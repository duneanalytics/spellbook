{{ config(
  materialized='table',
  file_format='delta',
  tags=['sui','dex'],
  schema='dex_sui'
) }}

-- latest object row per pool (object_id)
with latest as (
  select
      object_id
      , type_
      , checkpoint
      , row_number() over (partition by object_id order by checkpoint desc) as rn
  from {{ source('sui','objects') }}
  where type_ like '%<%,%>%'
),
parsed as (
  select
      object_id                                        as pool_id
      , lower(regexp_extract(type_, '<([^,>]+),', 1))    as coin_type_a
      , lower(regexp_extract(type_, ',\\s*([^>]+)>', 1)) as coin_type_b
  from latest
  where rn = 1
    and coin_type_a is not null
    and coin_type_b is not null
    and (
      type_ like '%::pool::%' or
      type_ like '%::clpool::%' or
      type_ like '%::clmm::%'  or
      type_ like '%::amm::%'   or
      type_ like '%::obric::%' or
      type_ like '%::pair::%'
    )
)
select distinct pool_id, coin_type_a, coin_type_b
from parsed
