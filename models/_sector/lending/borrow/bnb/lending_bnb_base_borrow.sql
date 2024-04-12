{{
  config(
    schema = 'lending_bnb',
    alias = 'base_borrow',
    materialized = 'view'
  )
}}

{%
  set models = [
    ref('radiant_bnb_base_borrow'),
    ref('aave_v3_bnb_base_borrow'),
    ref('granary_bnb_base_borrow')
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
