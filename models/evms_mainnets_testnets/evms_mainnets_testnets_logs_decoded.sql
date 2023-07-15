{{ config(
        tags = ['dunesql'],
        alias = alias('logs_decoded'),
        unique_key=['blockchain', 'tx_hash'],
        post_hook='{{ expose_spells(\'["goerli","ethereum", "polygon", "bnb", "avalanche_c", "gnosis", "fantom", "optimism", "arbitrum", "celo"]\',
                                    "sector",
                                    "evms_mainnets_testnets",
                                    \'["hildobby", "msilb7"]\') }}'
        )
}}

{% set models = [
     ('mainnet', ref('evms_logs_decoded'))
     ,('testnet', ref('evms_testnets_logs_decoded'))
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