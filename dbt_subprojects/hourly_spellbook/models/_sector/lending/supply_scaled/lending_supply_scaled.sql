{{
  config(
    schema = 'lending',
    alias = 'supply_scaled',
    partition_by = ['blockchain', 'project', 'block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['blockchain', 'project', 'version', 'block_hour', 'token_address', 'user'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_hour')],
    post_hook = '{{ expose_spells(\'["arbitrum", "avalanche_c", "base", "bnb", "celo", "ethereum", "fantom", "gnosis", "linea", "optimism", "polygon", "scroll", "sonic", "zksync"]\',
                                "sector",
                                "lending",
                                \'["tomfutago"]\') }}'
  )
}}

{%
  set models = [
    ref('lending_arbitrum_base_supply_scaled')
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
{% if is_incremental() %}
where {{ incremental_predicate('block_hour') }}
{% endif %}
{% if not loop.last %}
union all
{% endif %}
{% endfor %}
