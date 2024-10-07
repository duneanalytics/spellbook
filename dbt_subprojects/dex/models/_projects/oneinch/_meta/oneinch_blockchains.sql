{{
    config(
        schema = 'oneinch',
        alias = 'blockchains',
        materialized = 'table',
        unique_key = ['blockchain'],
    )
}}

{% set
    blockchains = [
        'ethereum',
        'bnb',
        'polygon',
        'arbitrum',
        'optimism',
        'avalanche_c',
        'gnosis',
        'fantom',
        'base',
        'zksync',
        'aurora',
        'klaytn',
    ]
%}

{% for blockchain in blockchains %}
    {{ oneinch_blockchain_macro(blockchain) }}
    {% if not loop.last %} union all {% endif %}
{% endfor %}
