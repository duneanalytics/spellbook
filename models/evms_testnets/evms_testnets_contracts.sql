{{ config(
        tags = ['dunesql'],
        alias = alias('contracts'),
        unique_key=['blockchain', 'address', 'created_at'],
        post_hook='{{ expose_spells(\'["goerli"]\',
                                    "sector",
                                    "evms",
                                    \'["hildobby", "msilb7"]\') }}'
        )
}}

{% set contracts_models = [
     ('goerli', source('goerli', 'contracts'))
] %}

SELECT *
FROM (
        {% for contracts_model in contracts_models %}
        SELECT
        '{{ contracts_model[0] }}' AS blockchain
        , *
        FROM {{ contracts_model[1] }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
        );