{% set chains = [
  'solana',
] %}

{{
  config(
    schema = 'stablecoins_svm',
    alias = 'balances',
    materialized = 'view'
  )
}}

{% for chain in chains %}
select
  blockchain,
  day,
  address,
  token_symbol,
  token_address,
  token_standard,
  token_id,
  currency,
  balance_raw,
  balance,
  balance_usd,
  last_updated
from {{ ref('stablecoins_' ~ chain ~ '_balances') }}
{% if not loop.last %}
union all
{% endif %}
{% endfor %}
