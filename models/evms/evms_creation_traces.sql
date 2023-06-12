{{ config(
        alias ='creation_traces',
        unique_key=['blockchain', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["ethereum", "polygon", "bnb", "avalanche_c", "gnosis", "fantom", "optimism", "arbitrum"]\',
                                    "sector",
                                    "evms",
                                    \'["hildobby"]\') }}'
        )
}}

{% set creation_traces_models = [
     ('ethereum', source('ethereum', 'creation_traces'))
     , ('polygon', source('polygon', 'creation_traces'))
     , ('bnb', source('bnb', 'creation_traces'))
     , ('avalanche_c', source('avalanche_c', 'creation_traces'))
     , ('gnosis', source('gnosis', 'creation_traces'))
     , ('fantom', source('fantom', 'creation_traces'))
     , ('optimism', source('optimism', 'creation_traces'))
     , ('arbitrum', source('arbitrum', 'creation_traces'))
] %}

SELECT *
FROM (
        {% for creation_traces_model in creation_traces_models %}
        SELECT
        '{{ creation_traces_model[0] }}' AS blockchain
        , *
        FROM {{ creation_traces_model[1] }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
        );