{%
  macro lending_aave_compatible_borrow_view(
    blockchain = 'multichain',
    project = 'aave',
    version = 'all'
  )
%}

select
  blockchain,
  project,
  version,
  transaction_type,
  loan_type,
  symbol,
  token_address,
  borrower,
  on_behalf_of,
  repayer,
  liquidator,
  amount,
  amount_usd,
  block_month,
  block_time,
  block_number,
  project_contract_address,
  evt_index,
  tx_hash
from {{ ref('lending_borrow') }}
where 1 = 1
  {% if blockchain != 'multichain' %}
  and blockchain = '{{ blockchain }}'
  {% endif %}
  and project = '{{ project }}'
  {% if version != 'all' %}
  and version = '{{ version }}'
  {% endif %}

{% endmacro %}

{# ######################################################################### #}

{%
  macro lending_aave_compatible_flashloans_view(
    blockchain = 'multichain',
    project = 'aave',
    version = 'all'
  )
%}

select
  blockchain,
  project,
  version,
  block_time,
  block_month,
  block_number,
  recipient,
  amount,
  amount_usd,
  fee,
  symbol as currency_symbol,
  token_address as currency_contract,
  project_contract_address as contract_address,
  evt_index,
  tx_hash
from {{ ref('lending_flashloans') }}
where 1 = 1
  {% if blockchain != 'multichain' %}
  and blockchain = '{{ blockchain }}'
  {% endif %}
  and project = '{{ project }}'
  {% if version != 'all' %}
  and version = '{{ version }}'
  {% endif %}

{% endmacro %}

{# ######################################################################### #}

{%
  macro lending_aave_compatible_supply_view(
    blockchain = 'multichain',
    project = 'aave',
    version = 'all'
  )
%}

select
  blockchain,
  project,
  version,
  transaction_type,
  symbol,
  token_address,
  depositor,
  on_behalf_of,
  withdrawn_to,
  liquidator,
  amount,
  amount_usd,
  block_month,
  block_time,
  block_number,
  project_contract_address,
  evt_index,
  tx_hash
from {{ ref('lending_supply') }}
where 1 = 1
  {% if blockchain != 'multichain' %}
  and blockchain = '{{ blockchain }}'
  {% endif %}
  and project = '{{ project }}'
  {% if version != 'all' %}
  and version = '{{ version }}'
  {% endif %}

{% endmacro %}

{# ######################################################################### #}

{%
  macro lending_aave_compatible_market_view(
    blockchain = 'multichain',
    project = 'aave',
    version = 'all'
  )
%}

select
  blockchain,
  project,
  version,
  block_time,
  block_hour,
  block_month,
  block_number,
  token_address,
  symbol,
  liquidity_index,
  variable_borrow_index,
  deposit_rate,
  stable_borrow_rate,
  variable_borrow_rate,
  project_contract_address,
  evt_index,
  tx_hash
from {{ ref('lending_market') }}
where 1 = 1
  {% if blockchain != 'multichain' %}
  and blockchain = '{{ blockchain }}'
  {% endif %}
  and project = '{{ project }}'
  {% if version != 'all' %}
  and version = '{{ version }}'
  {% endif %}

{% endmacro %}

{# ######################################################################### #}

{%
  macro lending_aave_compatible_market_hourly_agg_view(
    blockchain = 'multichain',
    project = 'aave',
    version = 'all'
  )
%}

select
  blockchain,
  project,
  version,
  block_month,
  block_hour,
  token_address,
  symbol,
  liquidity_index,
  variable_borrow_index,
  deposit_rate,
  stable_borrow_rate,
  variable_borrow_rate
from {{ ref('lending_market_hourly_agg') }}
where 1 = 1
  {% if blockchain != 'multichain' %}
  and blockchain = '{{ blockchain }}'
  {% endif %}
  and project = '{{ project }}'
  {% if version != 'all' %}
  and version = '{{ version }}'
  {% endif %}

{% endmacro %}

{# ######################################################################### #}

{%
  macro lending_aave_compatible_interest_rates_view(
    blockchain = 'multichain',
    project = 'aave',
    version = 'all'
  )
%}

select
  blockchain,
  project,
  version,
  block_hour,
  token_address,
  symbol,
  deposit_rate,
  stable_borrow_rate,
  variable_borrow_rate
from {{ ref('lending_market_hourly_agg') }}
where 1 = 1
  {% if blockchain != 'multichain' %}
  and blockchain = '{{ blockchain }}'
  {% endif %}
  and project = '{{ project }}'
  {% if version != 'all' %}
  and version = '{{ version }}'
  {% endif %}

{% endmacro %}
