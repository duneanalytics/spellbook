{{
   config(
     schema = 'stablecoins',
     alias = 'base_balances',
     materialized = 'view'
   )
 }}

{%
  set models = [
    ref('stablecoins_arbitrum_base_balances'),
    ref('stablecoins_avalanche_c_base_balances'),
    ref('stablecoins_base_base_balances'),
    ref('stablecoins_bnb_base_balances'),
    ref('stablecoins_ethereum_base_balances'),
    ref('stablecoins_kaia_base_balances'),
    ref('stablecoins_linea_base_balances'),
    ref('stablecoins_optimism_base_balances'),
    ref('stablecoins_polygon_base_balances'),
    ref('stablecoins_scroll_base_balances'),
    ref('stablecoins_worldchain_base_balances'),
    ref('stablecoins_zksync_base_balances')
  ]
%}

{% for model in models %}
select
  blockchain,
  day,
  address,
  token_address,
  token_standard,
  token_id,
  balance_raw,
  last_updated
from {{ model }}
{% if not loop.last %}
union all
{% endif %}
{% endfor %}

