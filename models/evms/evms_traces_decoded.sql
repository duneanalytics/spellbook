{{ config(
        alias ='traces_decoded',
        unique_key=['blockchain', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["ethereum", "polygon", "bnb", "avalanche_c", "gnosis", "fantom", "optimism", "arbitrum"]\',
                                    "sector",
                                    "evms",
                                    \'["hildobby"]\') }}'
        )
}}

{% set decodedtraces_models = [
     ('ethereum', source('ethereum', 'traces_decoded'))
     , ('polygon', source('polygon', 'traces_decoded'))
     , ('bnb', source('bnb', 'traces_decoded'))
     , ('avalanche_c', source('avalanche_c', 'traces_decoded'))
     , ('gnosis', source('gnosis', 'traces_decoded'))
     , ('fantom', source('fantom', 'traces_decoded'))
     , ('optimism', source('optimism', 'traces_decoded'))
     , ('arbitrum', source('arbitrum', 'traces_decoded'))
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