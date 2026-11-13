{{ config(
        schema='evms',
        alias = 'transaction_address_metrics'
        , post_hook='{{ hide_spells() }}'
        )
}}

{% set chains = [
    'arbitrum',
    'avalanche_c',
    'base',
    'bnb',
    'celo',
    'ethereum',
    'gnosis',
    'linea',
    'mantle',
    'optimism',
    'polygon',
    'scroll',
    'sei',
    'zksync'
] %}

SELECT *
FROM (
    {% for chain in chains %}
    SELECT
        blockchain
        , chain_id
        , block_hour
        , from_address
        , to_address
        , tx_count
        , tx_success_rate
        , from_is_new_address
        , from_is_contract
        , to_is_new_address
        , to_is_contract
    FROM {{ ref(chain ~ '_transaction_address_metrics') }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
