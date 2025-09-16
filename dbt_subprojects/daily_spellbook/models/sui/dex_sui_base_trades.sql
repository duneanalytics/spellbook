{{ config(
    schema = 'dex_sui',
    alias = 'base_trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['project','transaction_digest','event_index'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
) }}

with raw as (
  select *
  from {{ ref('dex_sui_raw_base_trades') }}
  {% if is_incremental() %}
    where {{ incremental_predicate('block_time') }}
  {% endif %}
),

-- Resolve coin types: prefer provided; else infer from pool_map + a_to_b
resolved as (
  select
      r.blockchain
    , r.project
    , r.version
    , r.timestamp_ms
    , r.block_time
    , r.block_date
    , r.block_month
    , r.transaction_digest
    , r.transaction_digest_b58
    , r.event_index
    , r.epoch
    , r.checkpoint
    , r.pool_id
    , r.sender
    , r.amount_in
    , r.amount_out
    , r.a_to_b
    , r.fee_amount
    , r.protocol_fee_amount
    , r.after_sqrt_price
    , r.before_sqrt_price
    , r.liquidity
    , r.reserve_a
    , r.reserve_b
    , r.tick_index_bits
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

-- Coin metadata & normalized amounts
meta as (
  select
      re.*
    , ci_in.coin_symbol    as coin_symbol_in
    , ci_out.coin_symbol   as coin_symbol_out
    , ci_in.coin_decimals  as coin_decimals_in
    , ci_out.coin_decimals as coin_decimals_out
    , case when ci_in.coin_decimals  is not null
        then cast(re.amount_in  as decimal(38,0)) / cast(pow(10, ci_in.coin_decimals)  as decimal(38,0))
        else cast(null as decimal(38,18)) end as amount_in_decimal
    , case when ci_out.coin_decimals is not null
        then cast(re.amount_out as decimal(38,0)) / cast(pow(10, ci_out.coin_decimals) as decimal(38,0))
        else cast(null as decimal(38,18)) end as amount_out_decimal
  from resolved re
  left join {{ ref('coin_info') }} ci_in
    on lower(re.coin_type_in) = lower(ci_in.coin_type)
  left join {{ ref('coin_info') }} ci_out
    on lower(re.coin_type_out) = lower(ci_out.coin_type)
),

-- Transaction gas components
tx as (
  select
      ('0x' || lower(to_hex(from_base58(t.transaction_digest)))) as transaction_digest,
      t.gas_budget   as gas_budget_mist,
      t.gas_price    as gas_price_mist_per_unit,
      t.computation_cost,
      t.storage_cost,
      t.storage_rebate,
      t.non_refundable_storage_fee,
      t.total_gas_cost as total_gas_mist
  from {{ source('sui','transactions') }} t
  join (select distinct transaction_digest from meta) m
    on from_base58(t.transaction_digest) = from_hex(substr(m.transaction_digest, 3))
)

-- Attach gas usage & total_gas_sui
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

-- Prices (cast ONLY in the ON clause to match prices.usd VARBINARY contract_address)
priced as (
  select
      j.*
    , pb.price as price_in_usd
    , ps.price as price_out_usd
    , pg.price as price_gas_usd
  from joined j
  left join {{ source('prices','usd') }} pb
    on pb.blockchain = 'sui'
   and pb.minute = date_trunc('minute', j.block_time)
   and pb.contract_address = cast(lower(j.coin_type_in) as varbinary)
  left join {{ source('prices','usd') }} ps
    on ps.blockchain = 'sui'
   and ps.minute = date_trunc('minute', j.block_time)
   and ps.contract_address = cast(lower(j.coin_type_out) as varbinary)
  left join {{ source('prices','usd') }} pg
    on pg.blockchain = 'sui'
   and pg.minute = date_trunc('minute', j.block_time)
   and pg.contract_address = cast(lower('0x2::sui::SUI') as varbinary)
),

-- Final projection
finalize as (
  select
      'sui' as blockchain
    , project
    , version
    , timestamp_ms
    , block_time
    , block_date
    , block_month
    , cast(null as bigint) as block_number

    , transaction_digest      -- native VARCHAR
    , transaction_digest_b58
    , cast(event_index as bigint) as event_index

    , epoch
    , checkpoint
    , pool_id                 -- native VARCHAR
    , sender                  -- native VARCHAR

    , amount_in
    , amount_out
    , amount_in_decimal
    , amount_out_decimal
    , a_to_b

    , coin_type_in            -- native VARCHAR
    , coin_type_out           -- native VARCHAR
    , coin_symbol_in
    , coin_symbol_out
    , coin_decimals_in
    , coin_decimals_out

    , price_in_usd
    , price_out_usd
    , price_gas_usd
    , coalesce(
      case when price_out_usd is not null then amount_out_decimal * price_out_usd end,
      case when price_in_usd  is not null then amount_in_decimal  * price_in_usd  end
    ) as amount_usd

    , fee_amount
    , protocol_fee_amount
    , case when coin_decimals_in is not null
        then cast(fee_amount as decimal(38,0)) / cast(pow(10, coin_decimals_in) as decimal(38,0))
        else cast(null as decimal(38,18)) end as fee_amount_decimal
    , case when coin_decimals_in is not null
        then cast(protocol_fee_amount as decimal(38,0)) / cast(pow(10, coin_decimals_in) as decimal(38,0))
        else cast(null as decimal(38,18)) end as protocol_fee_amount_decimal
    , case when amount_in_decimal is not null and amount_in_decimal > 0
        then (cast(fee_amount as decimal(38,0)) / cast(pow(10, coin_decimals_in) as decimal(38,0)))
             / amount_in_decimal
        else null end as fee_rate
    , case when amount_in_decimal is not null and amount_in_decimal > 0
        then (cast(protocol_fee_amount as decimal(38,0)) / cast(pow(10, coin_decimals_in) as decimal(38,0)))
             / amount_in_decimal
        else null end as protocol_fee_rate

    -- Standard DEX mapping
    , amount_out as token_bought_amount_raw
    , amount_in  as token_sold_amount_raw
    , coin_type_out as token_bought_address
    , coin_type_in  as token_sold_address
    , sender    as taker
    , pool_id   as maker
    , pool_id   as project_contract_address
    , transaction_digest as tx_hash
    , event_index        as evt_index

    , after_sqrt_price
    , before_sqrt_price
    , liquidity
    , reserve_a
    , reserve_b
    , tick_index_bits

    , gas_budget_mist
    , gas_price_mist_per_unit
    , gas_used_computation_cost
    , gas_used_storage_cost
    , gas_used_storage_rebate
    , gas_used_non_refundable_storage_fee
    , total_gas_mist
    , total_gas_sui
    , (total_gas_sui * price_gas_usd) as total_gas_usd
  from priced
)

select *
from finalize
where transaction_digest is not null
  and event_index        is not null