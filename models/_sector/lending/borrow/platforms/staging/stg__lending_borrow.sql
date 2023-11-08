{{
  config(
    schema = 'lending',
    alias = 'stg_borrow',
    materialized = 'view'
  )
}}

{%
  set models = [
    ref('stg_aave_v1_ethereum_borrow'),
    ref('stg_aave_v2_ethereum_borrow'),
    ref('stg_aave_v3_ethereum_borrow'),
    ref('stg_moola_v1_celo_borrow'),
    ref('stg_aave_v2_polygon_borrow'),
    ref('stg_aave_v3_polygon_borrow'),
    ref('stg_aave_v3_optimism_borrow'),
    ref('stg_aave_v3_base_borrow'),
    ref('stg_aave_v3_arbitrum_borrow')
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
