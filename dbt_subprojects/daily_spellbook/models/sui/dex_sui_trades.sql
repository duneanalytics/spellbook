{{ config(
  schema = 'dex_sui',
  alias  = 'trades',
  materialized = 'incremental',
  file_format = 'delta',
  incremental_strategy = 'merge',
  unique_key = ['project','transaction_digest','event_index'],
  partition_by = ['block_month'],
  incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
) }}

-- 0) Base (pure pass-through union)
with base as (
  select *
  from {{ ref('dex_sui_base_trades') }}
  {% if is_incremental() %} 
  where {{ incremental_predicate('block_time') }} 
  {% endif %}
)

-- 1) Resolve coin types from event or pool_map (normalization only in ON)
, resolved as (
  select
      b.blockchain
    , b.project
    , b.timestamp_ms
    , b.block_time
    , b.block_date
    , b.block_month
    , b.transaction_digest
    , b.event_index
    , b.epoch
    , b.checkpoint
    , b.pool_id
    , b.sender
    , b.amount_in
    , b.amount_out
    , b.a_to_b
    , b.fee_amount
    , coalesce(
        b.coin_type_in
      , case when b.a_to_b then pm.coin_type_a else pm.coin_type_b end
      ) as coin_type_in
    , coalesce(
        b.coin_type_out
      , case when b.a_to_b then pm.coin_type_b else pm.coin_type_a end
      ) as coin_type_out
  from base b
  left join {{ ref('dex_sui_pool_map') }} pm
    on case
         when b.pool_id is null then null
         when starts_with(lower(b.pool_id), '0x') then lower(b.pool_id)
         else concat('0x', lower(b.pool_id))
       end = pm.pool_id
)

-- 2) Coin metadata (decimals) from coin_info
, ci_in as (
  select coin_type
       , coin_decimals as coin_in_decimals
       , coin_symbol as coin_in_symbol
  from {{ ref('dex_sui_coin_info') }}
)
, ci_out as (
  select coin_type
       , coin_decimals as coin_out_decimals
       , coin_symbol as coin_out_symbol
  from {{ ref('dex_sui_coin_info') }}
)

, with_decimals as (
  select
      r.*
    , ci_in.coin_in_symbol
    , ci_in.coin_in_decimals
    , ci_out.coin_out_symbol
    , ci_out.coin_out_decimals
    , case when ci_in.coin_in_decimals is not null
        then cast(r.amount_in as decimal(38,0)) / cast(pow(10, ci_in.coin_in_decimals) as decimal(38,0))
        else cast(null as decimal(38,18)) end as amount_in_decimal
    , case when ci_out.coin_out_decimals is not null
        then cast(r.amount_out as decimal(38,0)) / cast(pow(10, ci_out.coin_out_decimals) as decimal(38,0))
        else cast(null as decimal(38,18)) end as amount_out_decimal
  from resolved r
  left join ci_in
    on case
         when r.coin_type_in is null then null
         when starts_with(lower(r.coin_type_in), '0x') then lower(r.coin_type_in)
         else concat('0x', lower(r.coin_type_in))
       end = ci_in.coin_type
  left join ci_out
    on case
         when r.coin_type_out is null then null
         when starts_with(lower(r.coin_type_out), '0x') then lower(r.coin_type_out)
         else concat('0x', lower(r.coin_type_out))
       end = ci_out.coin_type
)

-- 3) Prices join
, priced as (
  select
      d.*
    , pin.price as price_in_usd
    , pout.price as price_out_usd
  from with_decimals d
  left join {{ source('prices','usd') }} pin
    on pin.blockchain = 'sui'
   and pin.minute = cast(date_trunc('minute', at_timezone(d.block_time,'UTC')) as timestamp)
   and pin.contract_address =
       cast(
         split_part(
           case
             when d.coin_type_in is null then null
             when starts_with(lower(d.coin_type_in),'0x') then lower(d.coin_type_in)
             else concat('0x', lower(d.coin_type_in))
           end
         , '::', 1
         ) as varbinary
       )
  left join {{ source('prices','usd') }} pout
    on pout.blockchain = 'sui'
   and pout.minute = cast(date_trunc('minute', at_timezone(d.block_time,'UTC')) as timestamp)
   and pout.contract_address =
       cast(
         split_part(
           case
             when d.coin_type_out is null then null
             when starts_with(lower(d.coin_type_out),'0x') then lower(d.coin_type_out)
             else concat('0x', lower(d.coin_type_out))
           end
         , '::', 1
         ) as varbinary
       )
)

-- 4) Minimal final projection
, final as (
  select
      p.blockchain
    , p.project
    , p.block_month
    , p.block_date
    , p.block_time
    , p.epoch
    , p.checkpoint
    , p.pool_id
    , p.sender
    , p.transaction_digest
    , p.event_index
    , p.coin_type_in  as token_sold_address
    , p.coin_type_out as token_bought_address
    , p.coin_in_symbol  as token_sold_symbol
    , p.coin_out_symbol as token_bought_symbol
    , p.amount_in  as token_sold_amount_raw
    , p.amount_out as token_bought_amount_raw
    , p.amount_in_decimal  as token_sold_amount
    , p.amount_out_decimal as token_bought_amount
    , p.a_to_b
    , p.fee_amount
    , case when p.coin_in_decimals is not null
        then cast(p.fee_amount as decimal(38,0)) / cast(pow(10, p.coin_in_decimals) as decimal(38,0))
        else cast(null as decimal(38,18)) end as fee_amount_decimal
    , p.price_in_usd
    , p.price_out_usd
    , case when p.amount_in_decimal  is not null and p.price_in_usd  is not null
        then p.amount_in_decimal  * p.price_in_usd end as token_sold_usd
    , case when p.amount_out_decimal is not null and p.price_out_usd is not null
        then p.amount_out_decimal * p.price_out_usd end as token_bought_usd
    , case
        when p.fee_amount is not null and p.coin_in_decimals is not null and p.price_in_usd is not null
          then (cast(p.fee_amount as decimal(38,0)) / cast(pow(10, p.coin_in_decimals) as decimal(38,0))) * p.price_in_usd
        when p.fee_amount is not null and p.coin_in_decimals is not null and p.price_out_usd is not null
          then (cast(p.fee_amount as decimal(38,0)) / cast(pow(10, p.coin_in_decimals) as decimal(38,0))) * p.price_out_usd
        else null
      end as fee_usd
  from priced p
)

select *
from final
{% if is_incremental() %}
where {{ incremental_predicate('final.block_time') }}
{% endif %}