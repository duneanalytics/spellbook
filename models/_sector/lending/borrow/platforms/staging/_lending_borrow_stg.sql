{{
  config(
    schema = 'lending',
    alias = 'borrow_stg',
    materialized = 'view'
  )
}}

{%
  set models = [
    ref('aave_ethereum_borrow_stg'),
    ref('moola_celo_borrow_stg'),
    ref('aave_polygon_borrow_stg'),
    ref('aave_optimism_borrow_stg'),
    ref('aave_base_borrow_stg'),
    ref('aave_arbitrum_borrow_stg')
  ]
%}

{% for model in models %}
select
  blockchain,
  project,
  version,
  transaction_type,
  loan_type,
  token_address,
  borrower,
  repayer,
  liquidator,
  amount,
  evt_tx_hash,
  evt_index,
  evt_block_month,
  evt_block_time,
  evt_block_number
from {{ model }}
{% if not loop.last %}
union all
{% endif %}
{% endfor %}
