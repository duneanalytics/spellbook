{{ config(
        alias ='contracts',
        materialized = 'incremental',
        file_format = 'delta',
        unique_key=['blockchain', 'tx_hash', 'evt_index'],
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
        {% if is_incremental() %}
        WHERE block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
        UNION ALL
        {% endif %}
        {% endfor %}
        );