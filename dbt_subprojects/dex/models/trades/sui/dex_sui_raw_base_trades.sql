{{ config(
    schema = 'dex_sui_raw',
    alias = 'base_trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['protocol', 'transaction_digest', 'event_index'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
) }}

{% set base_models = [
  ref('momentum_sui_base_trades')
  , ref('cetus_sui_base_trades')
] %}

with all_swaps as (
  select *
  from (
    {% for base in base_models %}
      select
          protocol
          , timestamp_ms
          , block_time
          , block_date
          , block_month
          , transaction_digest
          , event_index
          , epoch
          , checkpoint
          , pool_id
          , sender
          , cast(amount_in  as decimal(38,0)) as amount_in
          , cast(amount_out as decimal(38,0)) as amount_out
          , a_to_b
          , cast(fee_amount           as decimal(38,0)) as fee_amount
          , cast(protocol_fee_amount  as decimal(38,0)) as protocol_fee_amount
          , after_sqrt_price
          , before_sqrt_price
          , liquidity
          , reserve_a
          , reserve_b
          , tick_index_bits
          , row_number() over (
            partition by protocol, transaction_digest, event_index
            order by transaction_digest
          ) as dup_rank
      from {{ base }}
      {% if is_incremental() %}
        where {{ incremental_predicate('block_time') }}
      {% endif %}
      {% if not loop.last %}
        union all
      {% endif %}
    {% endfor %}
  )
  where dup_rank = 1
)

select * from all_swaps
