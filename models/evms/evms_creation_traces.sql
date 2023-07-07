{{ config(
        tags = ['dunesql'],
        alias = alias('creation_traces'),
        unique_key=['blockchain', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["ethereum", "polygon", "bnb", "avalanche_c", "gnosis", "fantom", "optimism", "arbitrum", "celo"]\',
                                    "sector",
                                    "evms",
                                    \'["hildobby"]\') }}'
        )
}}

{% set creation_traces_models = [
     ('ethereum', source('ethereum', 'creation_traces'), source('ethereum', 'transactions'))
     , ('polygon', source('polygon', 'creation_traces'), source('polygon', 'transactions'))
     , ('bnb', source('bnb', 'creation_traces'), source('bnb', 'transactions'))
     , ('avalanche_c', source('avalanche_c', 'creation_traces'), source('avalanche_c', 'transactions'))
     , ('gnosis', source('gnosis', 'creation_traces'), source('gnosis', 'transactions'))
     , ('fantom', source('fantom', 'creation_traces'), source('fantom', 'transactions'))
     , ('optimism', source('optimism', 'creation_traces'), source('optimism', 'transactions'))
     , ('arbitrum', source('arbitrum', 'creation_traces'), source('arbitrum', 'transactions'))
] %}

SELECT *
FROM (
        {% for creation_traces_model in creation_traces_models %}
        SELECT
        '{{ creation_traces_model[0] }}' AS blockchain
        , ct.block_time
        , ct.block_number
        , ct.tx_hash
        , ct.address
        , ct."from"
        , ct.code
        , txs."from" AS tx_from
        , txs.to AS tx_to
        FROM {{ creation_traces_model[1] }} ct
        INNER JOIN {{ creation_traces_model[2] }} txs 
                ON ct.block_number = txs.block_number
                AND ct.tx_hash = txs.hash
                
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}

        UNION ALL

        SELECT 'celo' AS blockchain
        , ct.block_time
        , ct.block_number
        , ct.tx_hash
        , ct.address
        , ct."from"
        , ct.code
        , txs."from" AS tx_from
        , txs.to AS tx_to
        FROM {{ source('celo', 'creation_traces') }} ct
        INNER JOIN {{ source('celo', 'transactions') }} txs
                ON ct.block_number = txs.block_number
                AND ct.tx_hash = txs.hash
        );