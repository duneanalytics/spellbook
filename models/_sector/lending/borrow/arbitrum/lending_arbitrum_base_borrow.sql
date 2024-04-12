{{
  config(
    schema = 'lending_arbitrum',
    alias = 'base_borrow',
    materialized = 'view'
  )
}}

{%
  set models = [
    ref('aave_v3_arbitrum_base_borrow'),
    ref('compound_v3_arbitrum_base_borrow'),
    ref('radiant_arbitrum_base_borrow'),
    ref('lodestar_v0_arbitrum_base_borrow'),
    ref('lodestar_v1_arbitrum_base_borrow'),
    ref('granary_arbitrum_base_borrow')
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
  block_month,
  block_time,
  block_number,
  tx_hash,
  evt_index
from {{ model }}
{% if not loop.last %}
union all
{% endif %}
{% endfor %}
