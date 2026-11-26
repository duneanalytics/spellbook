{{ config(
    schema = 'bridges_aero'
    , alias = 'v1_deposits'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['deposit_chain', 'tx_hash', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    , filtering_columns = ['block_date', 'deposit_chain']
) }}

{% set chains = ['arbitrum', 'arc', 'base', 'ethereum', 'ink', 'optimism'] %}

{% for blockchain in chains %}
{{ aero_v1_deposits(blockchain) }}
{% if not loop.last %}
UNION ALL
{% endif %}
{% endfor %}
