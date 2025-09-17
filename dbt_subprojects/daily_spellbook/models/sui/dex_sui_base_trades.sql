{{ config(
    schema = 'dex_sui',
    alias = 'base_trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['project', 'transaction_digest', 'event_index'],
    incremental_predicates = [incremental_predicate('block_time')]
) }}

{% set branches = [
  ('aftermath', ref('aftermath_sui_base_trades')),
  ('bluefin',   ref('bluefin_sui_base_trades')),
  ('bluemove',  ref('bluemove_sui_base_trades')),
  ('cetus',     ref('cetus_sui_base_trades')),
  ('flowx',     ref('flowx_sui_base_trades')),
  ('kriya',     ref('kriya_sui_base_trades')),
  ('momentum',  ref('momentum_sui_base_trades')),
  ('obric',     ref('obric_sui_base_trades'))
] %}

with all_swaps as (
  select *
  from (
    {% for name, model in branches %}
      select
          'sui'                 as blockchain
        , '{{ name }}'          as project
        -- event-table columns & time helpers: pass-through (we preformatted these in each base)
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
        , coin_type_in
        , coin_type_out
        , amount_in
        , amount_out
        , a_to_b
        , fee_amount

        , row_number() over (
            partition by '{{ name }}', transaction_digest, event_index
            order by transaction_digest
          ) as dup_rank
      from {{ model }}
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

select
    blockchain
  , project
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
  , coin_type_in
  , coin_type_out
  , amount_in
  , amount_out
  , a_to_b
  , fee_amount
from all_swaps
{% if is_incremental() %}
where {{ incremental_predicate('block_time') }}
{% endif %}