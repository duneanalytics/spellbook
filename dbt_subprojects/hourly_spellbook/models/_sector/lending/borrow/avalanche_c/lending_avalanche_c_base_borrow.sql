{{
  config(
    schema = 'lending_avalanche_c',
    alias = 'base_borrow',
    materialized = 'view'
  )
}}

{%
  set models = [
    ref('aave_v2_avalanche_c_base_borrow'),
    ref('aave_v3_avalanche_c_base_borrow'),
    ref('benqi_avalanche_c_base_borrow'),
    ref('granary_avalanche_c_base_borrow')
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
  on_behalf_of,
  repayer,
  liquidator,
  amount,
  block_month,
  block_time,
  block_number,
  project_contract_address,
  tx_hash,
  evt_index
from {{ model }}
{% if not loop.last %}
union all
{% endif %}
{% endfor %}
