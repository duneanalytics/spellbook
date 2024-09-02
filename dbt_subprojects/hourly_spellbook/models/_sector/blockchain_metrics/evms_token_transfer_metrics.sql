{{ config(
        schema='evms',
        alias = 'token_transfer_metrics',
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
        ,token_address
        ,token_symbol
        ,token_standard
        ,price_usd
        ,transfer_volume_raw
        ,transfer_volume
        ,transfer_volume_usd
        ,transfer_count
    FROM {{ ref(chain ~ '_token_transfer_metrics') }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
