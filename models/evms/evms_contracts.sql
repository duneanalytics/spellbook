{{ config(
        alias ='contracts',
        unique_key=['blockchain', 'address', 'created_at'],
        post_hook='{{ expose_spells(\'["ethereum", "polygon", "bnb", "avalanche_c", "gnosis", "fantom", "optimism", "arbitrum"]\',
                                    "sector",
                                    "evms",
                                    \'["hildobby"]\') }}'
        )
}}

{% set contracts_models = [
     ('ethereum', source('ethereum', 'contracts'))
     , ('polygon', source('polygon', 'contracts'))
     , ('bnb', source('bnb', 'contracts'))
     , ('avalanche_c', source('avalanche_c', 'contracts'))
     , ('gnosis', source('gnosis', 'contracts'))
     , ('fantom', source('fantom', 'contracts'))
     , ('optimism', source('optimism', 'contracts'))
     , ('arbitrum', source('arbitrum', 'contracts'))
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