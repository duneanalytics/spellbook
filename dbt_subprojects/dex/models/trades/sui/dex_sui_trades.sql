{{
  config(
    schema = 'dex_sui',
    alias = 'trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    partition_by = ['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['project', 'transaction_digest', 'event_index'],
    post_hook='{{ expose_spells(\'["sui"]\', "sector", "dex_sui", \'["krishhh"]\') }}'
  )
}}

with base_trades as (
  select *
  from {{ ref('dex_sui_base_trades') }}
  {% if is_incremental() %}
    where {{ incremental_predicate('block_time') }}
  {% endif %}
)

-- join prices for both legs (same minute as the trade)
, priced as (
  select
      bt.*,

      -- prices keyed by contract
      pb.price as price_in,   -- coin_type_in
      ps.price as price_out,  -- coin_type_out
      pg.price as price_gas   -- SUI gas price

  from base_trades bt
  left join {{ source('prices','usd') }} pb
    on pb.blockchain = 'sui'
   and pb.minute = date_trunc('minute', bt.block_time)
   and pb.contract_address = bt.coin_type_in
  left join {{ source('prices','usd') }} ps
    on ps.blockchain = 'sui'
   and ps.minute = date_trunc('minute', bt.block_time)
   and ps.contract_address = bt.coin_type_out
  left join {{ source('prices','usd') }} pg
    on pg.blockchain = 'sui'
   and pg.minute = date_trunc('minute', bt.block_time)
   and pg.contract_address = cast(lower('0x2::sui::SUI') as varbinary)
  {% if is_incremental() %}
    and {{ incremental_predicate('bt.block_time') }}
  {% endif %}
)

select
    -- standard Dune outputs
    bt.project ,
    bt.version,
    bt.blockchain,
    bt.block_month,
    cast(date_trunc('day', bt.block_time) as date) as block_date,
    bt.block_time,
    bt.epoch,
    bt.checkpoint,

    -- bought = output leg; sold = input leg
    bt.coin_symbol_out   as token_bought_symbol,
    bt.coin_symbol_in    as token_sold_symbol,
    case
      when lower(bt.coin_symbol_out) > lower(bt.coin_symbol_in)
        then concat(bt.coin_symbol_in, '-', bt.coin_symbol_out)
      else concat(bt.coin_symbol_out, '-', bt.coin_symbol_in)
    end as token_pair,

    -- decimals
    bt.amount_out_decimal as token_bought_amount,
    bt.amount_in_decimal  as token_sold_amount,

    -- raw amounts
    bt.amount_out as token_bought_amount_raw,
    bt.amount_in  as token_sold_amount_raw,

    -- USD notional: prefer valuing the bought leg; fall back to sold leg
    coalesce(
      bt.amount_out_decimal * bt.price_out,
      bt.amount_in_decimal  * bt.price_in
    ) as amount_usd,

    -- fees (taken from input leg)
    bt.fee_amount_decimal as fee_amount,
    case
      when bt.fee_amount_decimal is not null and bt.price_in is not null
        then bt.fee_amount_decimal * bt.price_in
      when bt.fee_amount_decimal is not null and bt.price_out is not null
        then bt.fee_amount_decimal * bt.price_out
      else null
    end as fee_usd,

    -- addresses
    bt.coin_type_out as token_bought_address,
    bt.coin_type_in  as token_sold_address,

    -- actors & ids
    bt.sender        as taker,
    bt.pool_id       as maker,
    bt.pool_id       as project_contract_address,
    bt.transaction_digest as tx_hash,
    bt.transaction_digest as tx_id,
    bt.event_index   as evt_index,

    -- extras you surfaced
    bt.a_to_b,
    bt.after_sqrt_price,
    bt.before_sqrt_price,
    bt.liquidity,
    bt.reserve_a,
    bt.reserve_b,
    bt.tick_index_bits,
    bt.fee_rate,
    bt.protocol_fee_rate,
    bt.total_gas_sui,
    (bt.total_gas_sui * bt.price_gas) as total_gas_usd

from priced bt