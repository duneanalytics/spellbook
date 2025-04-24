{{
  config(
    schema = 'lending_market_hourly_agg',
    alias = 'market',
    partition_by = ['blockchain', 'project', 'block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['blockchain', 'project', 'version', 'block_hour', 'token_address'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_hour')]
  )
}}

{%
  set models = [
    ref('lending_arbitrum_base_market_hourly_agg'),
    ref('lending_avalanche_c_base_market_hourly_agg'),
    ref('lending_base_base_market_hourly_agg'),
    ref('lending_bnb_base_market_hourly_agg'),
    ref('lending_celo_base_market_hourly_agg'),
    ref('lending_ethereum_base_market_hourly_agg'),
    ref('lending_fantom_base_market_hourly_agg'),
    ref('lending_gnosis_base_market_hourly_agg'),
    ref('lending_linea_base_market_hourly_agg'),
    ref('lending_optimism_base_market_hourly_agg'),
    ref('lending_polygon_base_market_hourly_agg'),
    ref('lending_scroll_base_market_hourly_agg'),
    ref('lending_sonic_base_market_hourly_agg'),
    ref('lending_zksync_base_market_hourly_agg')
  ]
%}

{% for model in models %}
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
from {{ model }}
{% if is_incremental() %}
where {{ incremental_predicate('block_hour') }}
{% endif %}
{% if not loop.last %}
union all
{% endif %}
{% endfor %}
