{{ config(
  schema = 'deepbook_sui',
  alias  = 'pool_daily_metrics',
  materialized = 'incremental',
  file_format = 'delta',
  incremental_strategy = 'merge',
  partition_by = ['metric_month'],
  unique_key = ['metric_date','pool_id'],
  -- manual incremental window below uses metric_month; no time-based predicate column on target
) }}

with base as (
  select *
  from {{ ref('dex_sui_trades') }}
  where project = 'deepbook'
  {% if is_incremental() %}
    and {{ incremental_predicate('block_time') }}
  {% endif %}
)

, pm as (
  select pool_id, coin_type_a as base_asset_coin_type, coin_type_b as quote_asset_coin_type
  from {{ ref('dex_sui_pool_map') }}
)

, enriched as (
  select
      date(b.block_time) as metric_date
    , cast(date_trunc('month', b.block_time) as date) as metric_month
    , case
        when starts_with(lower(b.pool_id), '0x') then lower(b.pool_id)
        else concat('0x', lower(b.pool_id))
      end as pool_id
    , p.base_asset_coin_type
    , p.quote_asset_coin_type
    , b.token_sold_address
    , b.token_bought_address
    , b.token_sold_amount
    , b.token_bought_amount
    , b.amount_usd
  from base b
  left join pm p on (
    case when b.pool_id is null then null
         when starts_with(lower(b.pool_id), '0x') then lower(b.pool_id)
         else concat('0x', lower(b.pool_id)) end
  ) = p.pool_id
)

, aggregated as (
  select
      metric_date
    , metric_month
    , pool_id
    , base_asset_coin_type
    , quote_asset_coin_type
    , cast(sum(
        case
          when lower(token_bought_address) = lower(base_asset_coin_type) then cast(token_bought_amount as double)
          when lower(token_sold_address)  = lower(base_asset_coin_type) then cast(token_sold_amount  as double)
          else 0.0 end
      ) as decimal(38,18)) as total_base_asset_volume
    , cast(sum(
        case
          when lower(token_bought_address) = lower(quote_asset_coin_type) then cast(token_bought_amount as double)
          when lower(token_sold_address)  = lower(quote_asset_coin_type) then cast(token_sold_amount  as double)
          else 0.0 end
      ) as decimal(38,18)) as total_quote_asset_volume
    , cast(sum(cast(amount_usd as double)) as decimal(38,8)) as total_volume_usd
  from enriched
  group by 1,2,3,4,5
)

select
    metric_date
  , metric_month
  , pool_id
  , base_asset_coin_type
  , quote_asset_coin_type
  , total_base_asset_volume
  , total_quote_asset_volume
  , total_volume_usd
from aggregated
{% if is_incremental() %}
where metric_month >= date_trunc('month', (current_timestamp - interval '{{ var('DBT_ENV_INCREMENTAL_TIME', '3') }}' {{ var('DBT_ENV_INCREMENTAL_TIME_UNIT', 'day') }}))
{% endif %}
