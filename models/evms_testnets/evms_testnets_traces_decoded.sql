{{ config(
        tags = ['dunesql'],
        alias = alias('traces_decoded'),
        unique_key=['blockchain', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["goerli"]\',
                                    "sector",
                                    "evms_testnets",
                                    \'["hildobby", "msilb7"]\') }}'
        )
}}

{% set decodedtraces_models = [
     ('goerli', source('goerli', 'traces_decoded'))
] %}

SELECT *
FROM (
        {% for decodedtraces_model in decodedtraces_models %}
        SELECT
        '{{ decodedtraces_model[0] }}' AS blockchain
        , *
        FROM {{ decodedtraces_model[1] }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
        );