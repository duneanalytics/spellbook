{{ config(
  schema='dex_sui',
  alias='coin_info',
  materialized='table',
  file_format='delta',
  tags=['sui','dex']
) }}

with meta as (
  select
      lower(regexp_extract(cast(type_ as varchar), '<(.*)>', 1)) as coin_type
    , cast(json_extract_scalar(object_json, '$.symbol')   as varchar) as coin_symbol
    , cast(json_extract_scalar(object_json, '$.decimals') as integer) as coin_decimals
    , checkpoint
    , version
    , row_number() over (
        partition by lower(regexp_extract(cast(type_ as varchar), '<(.*)>', 1))
        order by checkpoint desc, version desc
      ) as rn
  from {{ source('sui','objects') }}
  where cast(type_ as varchar) like '0x2::coin::CoinMetadata<%'
)
, latest as (
  select
      coin_type
    , coin_symbol
    , coin_decimals
  from meta
  where rn = 1
)
, manual as (
  select * from (
    values
        ( lower('0x2::sui::SUI')                                                                 , 'SUI' , 9 )
      , ( lower('0x27792d9fed7f9844eb4839566001bb6f6cb4804f66aa2da6fe1ee242d896881::coin::COIN') , 'BTC' , 8 )
  ) as t(coin_type, coin_symbol, coin_decimals)
)

select
    l.coin_type
  , l.coin_symbol
  , l.coin_decimals
from latest l

union all

select
    m.coin_type
  , m.coin_symbol
  , m.coin_decimals
from manual m
left join latest l
  on l.coin_type = m.coin_type
where l.coin_type is null