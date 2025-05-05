{{ config(
        schema='evms',
        alias = 'traces',
        unique_key=['blockchain', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'[
                                        "abstract",
                                        "apechain",
                                        "arbitrum",
                                        "avalanche_c",
                                        "b3",
                                        "base",
                                        "berachain",
                                        "blast",
                                        "bnb",
                                        "boba",
                                        "celo",
                                        "ethereum",
                                        "fantom",
                                        "gnosis",
                                        "kaia",
                                        "linea",
                                        "mantle",
                                        "optimism",
                                        "polygon",
                                        "ronin",
                                        "scroll",
                                        "sonic",
                                        "zkevm",
                                        "zksync",
                                        "zora",
                                        "ink"
                                        ]\',
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
     , ('zksync', source('zksync', 'traces'))
     , ('zora', source('zora', 'traces'))
     , ('scroll', source('scroll', 'traces'))
     , ('linea', source('linea', 'traces'))
     , ('zkevm', source('zkevm', 'traces'))
     , ('blast', source('blast', 'traces'))
     , ('mantle', source('mantle', 'traces'))
     , ('mode', source('mode', 'traces'))
     , ('sei', source('sei', 'traces'))
     , ('ronin', source('ronin', 'traces'))
     , ('abstract', source('abstract', 'traces'))
     , ('boba', source('boba', 'traces'))
     , ('sonic', source('sonic', 'traces'))
     , ('kaia', source('kaia', 'traces'))
     , ('apechain', source('apechain', 'traces'))
     , ('berachain', source('berachain', 'traces'))
     , ('b3', source('b3', 'traces'))
     , ('ink', source('ink', 'traces'))
     , ('unichain', source('unichain', 'traces'))
     , ('lens', source('lens', 'traces'))
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