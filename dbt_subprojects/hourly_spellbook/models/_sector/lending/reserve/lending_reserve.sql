{{
  config(
    schema = 'lending',
    alias = 'reserve',
    partition_by = ['blockchain', 'project', 'block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['blockchain', 'project', 'version', 'block_time', 'token_address', 'tx_hash', 'evt_index'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    post_hook = '{{ expose_spells(blockchains = \'["arbitrum", "avalanche_c", "base", "bnb", "celo", "ethereum", "fantom", "gnosis", "linea", "optimism", "polygon", "scroll", "sonic", "zksync"]\',
                                  spell_type = "sector",
                                  spell_name = "lending",
                                  contributors = \'["tomfutago"]\') }}'
  )
}}

{%
  set models = [
    ref('lending_arbitrum_base_reserve')
  ]
%}

{% for model in models %}
select
  blockchain,
  project,
  version,
  block_time,
  block_date,
  block_month,
  block_number,
  token_address,
  token_symbol,
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
