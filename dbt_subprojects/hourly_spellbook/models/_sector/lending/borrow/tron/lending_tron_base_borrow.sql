{{
  config(
    schema = 'lending_tron',
    alias = 'base_borrow',
    materialized = 'view'
  )
}}

{%
  set models = [
    ref('justlend_tron_base_borrow')
  ]
%}

{% for model in models %}
select
  blockchain,
  project,
  version,
  transaction_type,
  loan_type,
  to_tron_address(token_address) as token_address,
  to_tron_address(borrower) as borrower,
  to_tron_address(on_behalf_of) as on_behalf_of,
  to_tron_address(repayer) as repayer,
  to_tron_address(liquidator) as liquidator,
  amount,
  amount_raw,
  block_month,
  block_time,
  block_number,
  to_tron_address(project_contract_address) as project_contract_address,
  lower(to_hex(tx_hash)) as tx_hash,
  evt_index
from {{ model }}
{% if not loop.last %}
union all
{% endif %}
{% endfor %}
