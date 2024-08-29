{{ config(
        schema='evms',
        alias = 'transaction_address_metrics',
        post_hook='{{ expose_spells(\'["arbitrum", "avalanche_c", "base", "blast", "bnb", "celo", "ethereum", "fantom", "gnosis", "linea", "mantle", "optimism", "polygon", "scroll", "sei", "zkevm", "zksync", "zora"]\',
                                    "sector",
                                    "metrics",
                                    \'["jeff-dude"]\') }}'
        )
}}

{% set chains = [
    'arbitrum',
    'avalanche_c',
    'base',
    'blast',
    'bnb',
    'celo',
    'ethereum',
    'fantom',
    'gnosis',
    'linea',
    'mantle',
    'optimism',
    'polygon',
    'scroll',
    'sei',
    'zkevm',
    'zksync',
    'zora'
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
