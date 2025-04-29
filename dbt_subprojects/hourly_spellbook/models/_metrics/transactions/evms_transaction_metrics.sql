{{ config(
        schema='evms',
        alias = 'transaction_metrics',
        post_hook='{{ expose_spells(\'["arbitrum", "avalanche_c", "base", "blast", "bnb", "celo", "ethereum", "fantom", "gnosis", "linea", "mantle", "optimism", "polygon", "scroll", "sei", "zkevm", "zksync", "zora"]\',
                                    "sector",
                                    "metrics",
                                    \'["0xRob"]\') }}'
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
        ,chain_id
        ,block_hour
        ,tx_count
        ,tx_success_count
        ,tx_success_rate
        ,avg_block_time_seconds
        ,tx_per_second
        ,new_addresses
        ,new_contracts
    FROM {{ ref(chain ~ '_transaction_metrics') }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
