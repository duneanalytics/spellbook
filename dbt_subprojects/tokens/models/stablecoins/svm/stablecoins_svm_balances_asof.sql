{% set chains = [
  'solana',
] %}

{{
  config(
    schema = 'stablecoins_svm',
    alias = 'balances_asof',
    materialized = 'view'
  )
}}

-- SVM stablecoin balances using ASOF pattern (benchmark)

select *
from (
  {% for chain in chains %}
  select
    blockchain,
    day,
    address,
    token_symbol,
    token_address,
    token_standard,
    token_id,
    token_backing,
    token_name,
    balance_raw,
    balance,
    balance_usd,
    last_updated
  from {{ ref('stablecoins_' ~ chain ~ '_balances_asof') }}
  {% if not loop.last %}
  union all
  {% endif %}
  {% endfor %}
)
