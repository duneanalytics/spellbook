{{
  config(
    schema = 'lending',
    alias = 'market',
    partition_by = ['blockchain', 'project', 'block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['blockchain', 'project', 'version', 'block_time', 'token_address', 'tx_hash', 'evt_index'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
  )
}}

{%
  set models = [
    ref('lending_arbitrum_base_market'),
    ref('lending_avalanche_c_base_market'),
    ref('lending_base_base_market'),
    ref('lending_bnb_base_market'),
    ref('lending_celo_base_market'),
    ref('lending_ethereum_base_market'),
    ref('lending_fantom_base_market'),
    ref('lending_gnosis_base_market'),
    ref('lending_linea_base_market'),
    ref('lending_optimism_base_market'),
    ref('lending_polygon_base_market'),
    ref('lending_scroll_base_market'),
    ref('lending_sonic_base_market'),
    ref('lending_zksync_base_market')
  ]
%}

{% for model in models %}
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
from {{ model }}
{% if is_incremental() %}
where {{ incremental_predicate('block_time') }}
{% endif %}
{% if not loop.last %}
union all
{% endif %}
{% endfor %}
