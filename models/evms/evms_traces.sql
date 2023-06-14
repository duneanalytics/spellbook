{{ config(
        alias ='traces',
        unique_key=['blockchain', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["ethereum", "polygon", "bnb", "avalanche_c", "gnosis", "fantom", "optimism", "arbitrum"]\',
                                    "sector",
                                    "evms",
                                    \'["hildobby"]\') }}'
        )
}}

{% set traces_models = [
     ('ethereum', source('ethereum', 'traces'))
     , ('polygon', source('polygon', 'traces'))
     , ('bnb', source('bnb', 'traces'))
     , ('avalanche_c', source('avalanche_c', 'traces'))
     , ('gnosis', source('gnosis', 'traces'))
     , ('fantom', source('fantom', 'traces'))
     , ('optimism', source('optimism', 'traces'))
     , ('arbitrum', source('arbitrum', 'traces'))
] %}

SELECT *
FROM (
        {% for traces_model in traces_models %}
        SELECT
        '{{ traces_model[0] }}' AS blockchain
        , *
        FROM {{ traces_model[1] }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
        );