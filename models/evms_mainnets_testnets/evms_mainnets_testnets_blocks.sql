{{ config(
        tags = ['dunesql'],
        alias = alias('blocks'),
        unique_key=['blockchain', 'number'],
        post_hook='{{ expose_spells(\'["goerli","ethereum", "polygon", "bnb", "avalanche_c", "gnosis", "fantom", "optimism", "arbitrum", "celo"]\',
                                    "sector",
                                    "evms_mainnets_testnets",
                                    \'["hildobby", "msilb7"]\') }}'
        )
}}

{% set models = [
     ('mainnet', ref('evms_blocks'))
     ,('testnet', ref('evms_testnets_blocks'))
] %}

SELECT *
FROM (
        {% for model in models %}
        SELECT
        '{{ model[0] }}' AS chain_type
        , *
        FROM {{ model[1] }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
        );