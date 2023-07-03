{{ config(
        tags = ['dunesql'],
        alias = alias('logs_decoded'),
        unique_key=['blockchain', 'tx_hash'],
        post_hook='{{ expose_spells(\'["goerli"]\',
                                    "sector",
                                    "emvs_mainnets_testnets",
                                    \'["hildobby", "msilb7"]\') }}'
        )
}}

{% set models = [
     ('mainnet', ref('emvs_logs_decoded'))
     ,('testnet', ref('emvs_testnets_logs_decoded'))
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