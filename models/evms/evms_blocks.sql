{{ config(
        alias ='blocks',
        materialized = 'incremental',
        file_format = 'delta',
        unique_key=['blockchain', 'number'],
        post_hook='{{ expose_spells(\'["ethereum", "polygon", "bnb", "avalanche_c", "gnosis", "fantom", "optimism", "arbitrum"]\',
                                    "sector",
                                    "evms",
                                    \'["hildobby"]\') }}'
        )
}}

{% set blocks_models = [
     ('ethereum', source('ethereum', 'transactions'), source('ethereum', 'blocks'))
     , ('polygon', source('polygon', 'transactions'), source('polygon', 'blocks'))
     , ('bnb', source('bnb', 'transactions'), source('bnb', 'blocks'))
     , ('avalanche_c', source('avalanche_c', 'transactions'), source('avalanche_c', 'blocks'))
     , ('gnosis', source('gnosis', 'transactions'), source('gnosis', 'blocks'))
     , ('fantom', source('fantom', 'transactions'), source('fantom', 'blocks'))
     , ('optimism', source('optimism', 'transactions'), source('optimism', 'blocks'))
     , ('arbitrum', source('arbitrum', 'transactions'), source('arbitrum', 'blocks'))
] %}

SELECT *
FROM (
        {% for blocks_model in blocks_models %}
        SELECT
        '{{ blocks_model[0] }}' AS blockchain
        , hash
        , miner
        , nonce
        , parent_hash
        , size
        , time
        , CAST(total_difficulty AS double) AS total_difficulty
        , number
        , base_fee_per_gas
        , CAST(difficulty AS double) AS difficulty
        , gas_limit
        , gas_used
        FROM {{ blocks_model[2] }}
        {% if not loop.last %}
        {% if is_incremental() %}
        WHERE block_time >= date_trunc("day", now() - interval '1 week')
        AND number = (
                SELECT MAX(number) FROM {{ blocks_model[1] }}
                WHERE block_time BETWEEN NOW() - interval '7 days' AND NOW() - interval '164 hours'
                )
        {% endif %}
        UNION ALL
        {% endif %}
        {% endfor %}
        );