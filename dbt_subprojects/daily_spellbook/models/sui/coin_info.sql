{{ config(
  materialized='table',
  file_format='delta',
  schema='dex_sui',
  tags=['sui','dex']
) }}

-- Take the latest metadata per coin_type from staging
with latest as (
  select
      coin_type,
      coin_symbol,
      coin_decimals,
      checkpoint,
      version,
      row_number() over (
        partition by coin_type
        order by checkpoint desc, version desc
      ) as rn
  from {{ ref('coin_metadata_staging') }}
),

canonical as (
  select coin_type, coin_symbol, coin_decimals, 0 as is_manual
  from latest
  where rn = 1
),

-- Optional manual backfills for types without CoinMetadata
manual as (
  select * from (
    values
      ( lower('0x2::sui::SUI'), 'SUI', 9, 1 ),
      ( lower('0x27792d9fed7f9844eb4839566001bb6f6cb4804f66aa2da6fe1ee242d896881::coin::COIN'), 'BTC', 8, 1 )
  ) as t(coin_type, coin_symbol, coin_decimals, is_manual)
),

unioned as (
  -- prefer chain metadata when present; else manual
  select * from canonical
  union all
  select coin_type, coin_symbol, coin_decimals, is_manual from manual
),

ranked as (
  select
    coin_type,
    coin_symbol,
    coin_decimals,
    row_number() over (
      partition by coin_type
      order by is_manual asc
    ) as rn
  from unioned
)

select coin_type, coin_symbol, coin_decimals
from ranked
where rn = 1