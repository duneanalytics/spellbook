{{
    config(
        tags = ['dunesql'],
        schema = 'mento_celo',
        alias = alias('trades'),
        post_hook = '{{ expose_spells(\'["celo"]\',
                                    "project",
                                    "mento",
                                    \'["tomfutago"]\') }}'
    )
}}

{% set dex_models = [
  ref('mento_celo_trades_v1'),
  ref('mento_celo_trades_v2')
] %}

{% for dex_model in dex_models %}
select
  blockchain,
  project,
  version,
  block_month,
  block_date,
  block_time,
  token_bought_symbol,
  token_sold_symbol,
  token_pair,
  token_bought_amount,
  token_sold_amount,
  token_bought_amount_raw,
  token_sold_amount_raw,
  amount_usd,
  token_bought_address,
  token_sold_address,
  taker,
  maker,
  project_contract_address,
  tx_hash,
  tx_from,
  tx_to,
  evt_index
from {{ dex_model }}
{% if not loop.last %}
union all
{% endif %}
{% endfor %}
