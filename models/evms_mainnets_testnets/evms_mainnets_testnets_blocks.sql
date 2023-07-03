{{ config(
        tags = ['dunesql'],
        alias = alias('blocks'),
        unique_key=['blockchain', 'number'],
        post_hook='{{ expose_spells(\'["goerli"]\',
                                    "sector",
                                    "evms_mainnets_testnets",
                                    \'["hildobby", "msilb7"]\') }}'
        )
}}

{% set models = [
     ('mainnet', ref('emvs_blocks'))
     ,('testnet', ref('emvs_testnets_blocks'))
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