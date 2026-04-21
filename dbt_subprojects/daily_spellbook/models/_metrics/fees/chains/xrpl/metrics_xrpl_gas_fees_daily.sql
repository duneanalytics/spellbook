{{
  config(
    schema = 'metrics_xrpl',
    alias = 'gas_fees_daily',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['blockchain', 'block_date'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
  )
}}

with fees as (
  select
    blockchain,
    block_date,
    sum(tx_fee_usd) as gas_fees_usd
  from {{ source('gas_xrpl', 'fees') }}
  where blockchain = 'xrpl'
    {% if is_incremental() %}
    and {{ incremental_predicate('block_date') }}
    {% endif %}
  group by 1, 2
)

select
  blockchain,
  block_date,
  gas_fees_usd
from fees