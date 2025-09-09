{{ config(
  materialized='table',
  file_format='delta',
  tags=['sui','dex'],
  schema='dex_sui'
) }}

with meta as (
  select
      lower(regexp_extract(type_, '<(.*)>', 1)) as coin_type,
      coalesce(
        json_extract_scalar(object_json, '$.symbol'),
        json_extract_scalar(bcs,         '$.symbol')
      )                                         as coin_symbol,
      cast(coalesce(
        json_extract_scalar(object_json, '$.decimals'),
        json_extract_scalar(bcs,         '$.decimals')
      ) as integer)                             as coin_decimals,
      checkpoint,
      version,
      row_number() over (
        partition by lower(regexp_extract(type_, '<(.*)>', 1))
        order by checkpoint desc, version desc
      ) as rn
  from {{ source('sui','objects') }}
  where type_ like '0x2::coin::CoinMetadata<%'
)
select coin_type, coin_symbol, coin_decimals
from meta
where rn = 1;
