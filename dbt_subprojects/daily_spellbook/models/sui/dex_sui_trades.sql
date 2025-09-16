{{ config(
    schema = 'dex_sui',
    alias = 'trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    partition_by = ['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['project', 'transaction_digest', 'event_index'],
    post_hook='{{ expose_spells(\'["sui"]\', "sector", "dex_sui", \'["krishhh"]\') }}'
) }}

with base_trades as (
  select *
  from {{ ref('dex_sui_base_trades') }}
  {% if is_incremental() %}
    where {{ incremental_predicate('block_time') }}
  {% endif %}
)

select
    -- identity / time
    bt.project,
    bt.version,
    bt.blockchain,
    bt.block_month,
    cast(date_trunc('day', bt.block_time) as date) as block_date,
    bt.block_time,
    bt.epoch,
    bt.checkpoint,

    -- token labels & pair
    bt.coin_symbol_out as token_bought_symbol,
    bt.coin_symbol_in  as token_sold_symbol,
    case
      when bt.coin_symbol_out is not null and bt.coin_symbol_in is not null
           and lower(bt.coin_symbol_out) > lower(bt.coin_symbol_in)
        then concat(bt.coin_symbol_in, '-', bt.coin_symbol_out)
      when bt.coin_symbol_out is not null and bt.coin_symbol_in is not null
        then concat(bt.coin_symbol_out, '-', bt.coin_symbol_in)
      else null
    end as token_pair,

    -- normalized amounts
    bt.amount_out_decimal as token_bought_amount,
    bt.amount_in_decimal  as token_sold_amount,

    -- raw amounts
    bt.amount_out as token_bought_amount_raw,
    bt.amount_in  as token_sold_amount_raw,

    -- USD notional
    bt.amount_usd,

    -- fees
    bt.fee_amount_decimal as fee_amount,
    case
      when bt.fee_amount_decimal is not null and bt.price_in_usd  is not null
        then bt.fee_amount_decimal * bt.price_in_usd
      when bt.fee_amount_decimal is not null and bt.price_out_usd is not null
        then bt.fee_amount_decimal * bt.price_out_usd
      else null
    end as fee_usd,

    -- addresses (*** stay native VARCHAR ***)
    bt.coin_type_out as token_bought_address,
    bt.coin_type_in  as token_sold_address,

    -- actors & ids
    bt.sender  as taker,
    bt.pool_id as maker,
    bt.pool_id as project_contract_address,

    -- Unique keys
    bt.transaction_digest as transaction_digest,
    bt.transaction_digest_b58 as transaction_digest_b58,
    bt.event_index        as event_index,

    -- pricing columns (passed through)
    bt.price_in_usd,
    bt.price_out_usd,
    bt.price_gas_usd,

    -- gas
    bt.total_gas_sui,
    bt.total_gas_usd,

    -- pool state & extras
    bt.a_to_b,
    bt.after_sqrt_price,
    bt.before_sqrt_price,
    bt.liquidity,
    bt.reserve_a,
    bt.reserve_b,
    bt.tick_index_bits,
    bt.fee_rate,
    bt.protocol_fee_rate
from base_trades bt
where bt.transaction_digest is not null
  and bt.event_index        is not null