{{ config(
        alias ='logs_decoded',
        unique_key=['blockchain', 'tx_hash'],
        post_hook='{{ expose_spells(\'["ethereum", "polygon", "bnb", "avalanche_c", "gnosis", "fantom", "optimism", "arbitrum"]\',
                                    "sector",
                                    "evms",
                                    \'["hildobby"]\') }}'
        )
}}

{% set decodedlogs_models = [
     ('ethereum', source('ethereum', 'logs_decoded'))
     , ('polygon', source('polygon', 'logs_decoded'))
     , ('bnb', source('bnb', 'logs_decoded'))
     , ('avalanche_c', source('avalanche_c', 'logs_decoded'))
     , ('gnosis', source('gnosis', 'logs_decoded'))
     , ('fantom', source('fantom', 'logs_decoded'))
     , ('optimism', source('optimism', 'logs_decoded'))
     , ('arbitrum', source('arbitrum', 'logs_decoded'))
] %}

SELECT *
FROM (
        {% for decodedlogs_model in decodedlogs_models %}
        SELECT
        '{{ decodedlogs_model[0] }}' AS blockchain
        , *
        FROM {{ decodedlogs_model[1] }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
        );