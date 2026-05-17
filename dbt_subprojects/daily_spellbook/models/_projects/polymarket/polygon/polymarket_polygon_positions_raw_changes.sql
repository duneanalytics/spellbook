{{
  config(
    schema = 'polymarket_polygon',
    alias = 'positions_raw_changes',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    partition_by = ['day'],
    unique_key = ['day', 'address', 'token_id'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')],
    post_hook = '{{ hide_spells() }}'
  )
}}

select
  blockchain,
  day,
  address,
  token_address,
  token_id,
  balance_raw
from {{ source('tokens_polygon', 'balances_daily_agg_base') }}
where token_address = 0x4D97DCd97eC945f40cF65F87097ACe5EA0476045
  and day >= cast('2020-09-03' as date)
{% if is_incremental() %}
  and {{ incremental_predicate('day') }}
{% endif %}
