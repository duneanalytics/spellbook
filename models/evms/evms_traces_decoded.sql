{{ config(
        
        alias = 'traces_decoded',
        unique_key=['blockchain', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["ethereum", "polygon", "bnb", "avalanche_c", "gnosis", "fantom", "optimism", "arbitrum", "celo", "base", "goerli", "zksync", "zora", "scroll"]\',
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
     , ('celo', source('celo', 'traces_decoded'))
     , ('base', source('base', 'traces_decoded'))
     , ('goerli', source('goerli', 'traces_decoded'))
     , ('zksync', source('zksync', 'traces_decoded'))
     , ('zora', source('zora', 'traces_decoded'))
     , ('scroll', source('scroll', 'traces_decoded'))
] %}

SELECT *
FROM (
        {% for decodedtraces_model in decodedtraces_models %}
        SELECT
        '{{ decodedtraces_model[0] }}' AS blockchain
        , block_date
        , block_time
        , block_number
        , contract_name
        , function_name
        , namespace
        , signature
        , to
        , trace_address
        , tx_hash
        FROM {{ decodedtraces_model[1] }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
        );