{{ config(
    schema = 'dex_sui',
    alias = 'base_trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['project','transaction_digest','event_index'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
) }}

with raw as (
  select * 
  from {{ ref('dex_sui_raw_base_trades') }}
  {% if is_incremental() %} 
  where {{ incremental_predicate('block_time') }} 
  {% endif %}
),

-- resolve coin types: prefer provided ones; fallback to pool_map + a_to_b
resolved as (
  select
      r.*
      , coalesce(
          r.coin_type_in,
          case when r.a_to_b is null then null
               when r.a_to_b then pm.coin_type_a else pm.coin_type_b end
        ) as coin_type_in
      , coalesce(
          r.coin_type_out,
          case when r.a_to_b is null then null
               when r.a_to_b then pm.coin_type_b else pm.coin_type_a end
        ) as coin_type_out
  from raw r
  left join {{ ref('pool_map') }} pm
    on lower(r.pool_id) = lower(pm.pool_id)
),

-- coin metadata
meta as (
  select
      re.*
      , ci_in.coin_symbol    as coin_symbol_in
      , ci_out.coin_symbol   as coin_symbol_out
      , ci_in.coin_decimals  as coin_decimals_in
      , ci_out.coin_decimals as coin_decimals_out

      -- normalized trade amounts
      , case when ci_in.coin_decimals is not null
           then cast(re.amount_in  as decimal(38,0)) / cast(pow(10, ci_in.coin_decimals)  as decimal(38,0))
           else cast(null as decimal(38,18)) end as amount_in_decimal
      , case when ci_out.coin_decimals is not null
           then cast(re.amount_out as decimal(38,0)) / cast(pow(10, ci_out.coin_decimals) as decimal(38,0))
           else cast(null as decimal(38,18)) end as amount_out_decimal
  from resolved re
  left join {{ ref('coin_info') }} ci_in
    on lower(re.coin_type_in) = ci_in.coin_type
  left join {{ ref('coin_info') }} ci_out
    on lower(re.coin_type_out) = ci_out.coin_type
),

-- transactions: gas components (Dune column names)
tx as (
  select
      transaction_digest
      , gas_budget   as gas_budget_mist
      , gas_price    as gas_price_mist_per_unit
      , computation_cost
      , storage_cost
      , storage_rebate
      , non_refundable_storage_fee      -- informative; already inside storage_cost accounting on Dune
      , total_gas_cost as total_gas_mist
  from {{ source('sui','transactions') }}
),

-- join trades to tx; alias to gas_used_* so final select matches
joined as (
  select
      m.*
      , t.gas_budget_mist
      , t.gas_price_mist_per_unit
      , t.computation_cost              as gas_used_computation_cost
      , t.storage_cost                  as gas_used_storage_cost
      , t.storage_rebate                as gas_used_storage_rebate
      , t.non_refundable_storage_fee    as gas_used_non_refundable_storage_fee
      , t.total_gas_mist
      , cast(t.total_gas_mist as decimal(38,0)) / cast(1000000000 as decimal(38,0)) as total_gas_sui
  from meta m
  left join tx t
    on m.transaction_digest = t.transaction_digest
),

-- add price data for USD calculations
priced as (
  select
      j.*
      , pb.price as price_in
      , ps.price as price_out
  from joined j
  left join {{ source('prices','usd') }} pb
    on pb.blockchain = 'sui'
   and pb.minute = date_trunc('minute', j.block_time)
   and pb.contract_address = j.coin_type_in
  left join {{ source('prices','usd') }} ps
    on ps.blockchain = 'sui'
   and ps.minute = date_trunc('minute', j.block_time)
   and ps.contract_address = j.coin_type_out
),

-- DEX fee normalization & rates (fees are in the input coin units)
finalize as (
  select
      -- Standard DEX model columns (required by tests)
      'sui' as blockchain
      , project
      , version
      , timestamp_ms
      , block_time
      , block_date
      , block_month
      , cast(null as bigint) as block_number  -- Sui doesn't have block numbers
      , transaction_digest
      , event_index
      , epoch
      , checkpoint
      , pool_id
      , sender

      -- swap core (raw + normalized)
      , amount_in
      , amount_out
      , amount_in_decimal
      , amount_out_decimal
      , a_to_b

      -- coin identity & metadata
      , lower(coin_type_in)   as coin_type_in
      , lower(coin_type_out)  as coin_type_out
      , coin_symbol_in
      , coin_symbol_out
      , coin_decimals_in
      , coin_decimals_out

      -- DEX fees (raw)
      , fee_amount
      , protocol_fee_amount

      -- DEX fees normalized (in input coin units)
      , case when coin_decimals_in is not null
           then cast(fee_amount as decimal(38,0)) / cast(pow(10, coin_decimals_in) as decimal(38,0))
           else cast(null as decimal(38,18)) end as fee_amount_decimal

      , case when coin_decimals_in is not null
           then cast(protocol_fee_amount as decimal(38,0)) / cast(pow(10, coin_decimals_in) as decimal(38,0))
           else cast(null as decimal(38,18)) end as protocol_fee_amount_decimal

      -- fee rates relative to input size
      , case when amount_in_decimal is not null and amount_in_decimal > 0
           then cast(
                  cast(fee_amount as decimal(38,0)) / cast(pow(10, coin_decimals_in) as decimal(38,0))
                as decimal(38,18)
                ) / amount_in_decimal
           else null end as fee_rate

      , case when amount_in_decimal is not null and amount_in_decimal > 0
           then cast(
                  cast(protocol_fee_amount as decimal(38,0)) / cast(pow(10, coin_decimals_in) as decimal(38,0))
                ) / amount_in_decimal
           else null end as protocol_fee_rate

      -- USD amount calculation
      , coalesce(
          amount_out_decimal * coalesce(price_out, 0),
          amount_in_decimal * coalesce(price_in, 0)
        ) as amount_usd

      -- Standard DEX test columns (mapped to Sui equivalents)
      , amount_out as token_bought_amount_raw
      , amount_in as token_sold_amount_raw
      , lower(coin_type_out) as token_bought_address
      , lower(coin_type_in) as token_sold_address
      , sender as taker
      , pool_id as maker
      , pool_id as project_contract_address
      , transaction_digest as tx_hash
      , event_index as evt_index

      -- pool state & ticks
      , after_sqrt_price
      , before_sqrt_price
      , liquidity
      , reserve_a
      , reserve_b
      , tick_index_bits

      -- gas
      , gas_budget_mist
      , gas_price_mist_per_unit
      , gas_used_computation_cost
      , gas_used_storage_cost
      , gas_used_storage_rebate
      , gas_used_non_refundable_storage_fee
      , total_gas_mist
      , total_gas_sui
  from priced
)

select * from finalize