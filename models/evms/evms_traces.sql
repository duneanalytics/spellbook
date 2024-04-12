{{ config(
        
        alias = 'traces',
        unique_key=['blockchain', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["ethereum", "polygon", "bnb", "avalanche_c", "gnosis", "fantom", "optimism", "arbitrum", "celo", "base", "goerli", "zksync", "zora", "scroll"]\',
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
     , ('celo', source('celo', 'traces'))
     , ('base', source('base', 'traces'))
     , ('goerli', source('goerli', 'traces'))
     , ('zksync', source('zksync', 'traces'))
     , ('zora', source('zora', 'traces'))
     , ('scroll', source('scroll', 'traces'))
] %}

SELECT *
FROM (
        {% for traces_model in traces_models %}
        SELECT
        '{{ traces_model[0] }}' AS blockchain
        , block_time
        , block_number
        , value
        , gas
        , gas_used
        , block_hash
        , success
        , tx_index
        , error
        , tx_success
        , tx_hash
        , "from"
        , to
        , trace_address
        , type
        , address
        , code
        , call_type
        , input
        , output
        , refund_address
        FROM {{ traces_model[1] }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
        );