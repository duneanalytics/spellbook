{{ config(
  schema='dex_sui',
  alias='coin_info',
  materialized='incremental',
  file_format='delta',
  incremental_strategy='merge',
  unique_key=['coin_type'],
  tags=['sui','dex']
) }}

-- 1) Pull only new CoinMetadata objects (incremental on checkpoint)
with src as (
  select
      lower(regexp_extract(cast(type_ as varchar), '<(.*)>', 1)) as coin_type
    , cast(json_extract_scalar(object_json, '$.symbol')   as varchar) as coin_symbol
    , cast(json_extract_scalar(object_json, '$.decimals') as integer) as coin_decimals
    , checkpoint                                              as checkpoint_latest
    , version                                                 as version_latest
  from {{ source('sui','objects') }}
  where cast(type_ as varchar) like '0x2::coin::CoinMetadata<%'
  {% if is_incremental() %}
    and checkpoint >
        coalesce((select max(checkpoint_latest) from {{ this }}), 0)
  {% endif %}
)

-- 2) Combine new rows with existing to find the latest per coin_type
, unioned as (
  select * from src
  {% if is_incremental() %}
  union all
  select coin_type, coin_symbol, coin_decimals, checkpoint_latest, version_latest
  from {{ this }}
  {% endif %}
)

, ranked as (
  select
      u.*
    , row_number() over (
        partition by u.coin_type
        order by u.checkpoint_latest desc, u.version_latest desc
      ) as rn
  from unioned u
)

, latest as (
  select
      coin_type
    , coin_symbol
    , coin_decimals
    , checkpoint_latest
    , version_latest
  from ranked
  where rn = 1
)

-- 3) Manual fill-ins for coins that don't have metadata on-chain
, manual as (
  select * from (
    values
        ( lower('0x2::sui::SUI')                                                                 , 'SUI' , 9 )
      , ( lower('0x27792d9fed7f9844eb4839566001bb6f6cb4804f66aa2da6fe1ee242d896881::coin::COIN') , 'BTC' , 8 )
  ) as t(coin_type, coin_symbol, coin_decimals)
)

-- 4) Final: latest on-chain + manual for missing types only
select
    l.coin_type
  , l.coin_symbol
  , l.coin_decimals
  , l.checkpoint_latest
  , l.version_latest
from latest l

union all

select
    m.coin_type
  , m.coin_symbol
  , m.coin_decimals
  , cast(null as bigint)  as checkpoint_latest
  , cast(null as bigint)  as version_latest
from manual m
left join latest l
  on l.coin_type = m.coin_type
where l.coin_type is null
