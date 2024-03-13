{{
  config(
    schema = 'lending_ethereum',
    alias = 'base_borrow',
    materialized = 'view'
  )
}}

{%
  set models = [
    ref('aave_v1_ethereum_base_borrow'),
    ref('aave_v2_ethereum_base_borrow'),
    ref('aave_v3_ethereum_base_borrow'),
    ref('compound_v2_ethereum_base_borrow'),
    ref('compound_v3_ethereum_base_borrow'),
    ref('radiant_ethereum_base_borrow'),
    ref('uwulend_ethereum_base_borrow'),
    ref('spark_ethereum_base_borrow'),
    ref('fluxfinance_ethereum_base_borrow'),
    ref('strike_ethereum_base_borrow'),
    ref('granary_ethereum_base_borrow')
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
