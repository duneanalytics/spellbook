{{
  config(
    schema = 'lending',
    alias = 'supply_scaled',
    materialized = 'view',
    post_hook = '{{ expose_spells(\'["arbitrum", "avalanche_c", "base", "bnb", "celo", "ethereum", "fantom", "gnosis", "linea", "optimism", "polygon", "scroll", "sonic", "zksync"]\',
                                "sector",
                                "lending",
                                \'["tomfutago"]\') }}'
  )
}}

{%
  set models = [
    ref('lending_avalanche_c_base_supply_scaled'),
    ref('lending_bnb_base_supply_scaled'),
    ref('lending_celo_base_supply_scaled'),
    ref('lending_ethereum_base_supply_scaled'),
    ref('lending_fantom_base_supply_scaled'),
    ref('lending_gnosis_base_supply_scaled'),
    ref('lending_linea_base_supply_scaled'),
    ref('lending_sonic_base_supply_scaled'),
    ref('lending_zksync_base_supply_scaled')
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
  user,
  amount
from {{ model }}
{% if not loop.last %}
union all
{% endif %}
{% endfor %}
