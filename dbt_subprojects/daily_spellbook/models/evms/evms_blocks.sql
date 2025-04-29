{{ config(
        schema='evms',
        alias = 'blocks',
        unique_key=['blockchain', 'number'],
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
                                    \'["hildobby", "shogun"]\') }}'
        )
}}

{% set blocks_models = [
     ('ethereum', source('ethereum', 'blocks'))
     , ('polygon', source('polygon', 'blocks'))
     , ('bnb', source('bnb', 'blocks'))
     , ('avalanche_c', source('avalanche_c', 'blocks'))
     , ('gnosis', source('gnosis', 'blocks'))
     , ('fantom', source('fantom', 'blocks'))
     , ('optimism', source('optimism', 'blocks'))
     , ('arbitrum', source('arbitrum', 'blocks'))
     , ('celo', source('celo', 'blocks'))
     , ('base', source('base', 'blocks'))
     , ('zksync', source('zksync', 'blocks'))
     , ('zora', source('zora', 'blocks'))
     , ('scroll', source('scroll', 'blocks'))
     , ('linea', source('linea', 'blocks'))
     , ('zkevm', source('zkevm', 'blocks'))
     , ('blast', source('blast', 'blocks'))
     , ('mantle', source('mantle', 'blocks'))
     , ('mode', source('mode', 'blocks'))
     , ('sei', source('sei', 'blocks'))
     , ('ronin', source('ronin', 'blocks'))
     , ('abstract', source('abstract', 'blocks'))
     , ('boba', source('boba', 'blocks'))
     , ('sonic', source('sonic', 'blocks'))
     , ('kaia', source('kaia', 'blocks'))
     , ('apechain', source('apechain', 'blocks'))
     , ('berachain', source('berachain', 'blocks'))
     , ('b3', source('b3', 'blocks'))
     , ('ink', source('ink', 'blocks'))
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
        FROM {{ blocks_model[1] }}
        {% if not loop.last %}
        {% if is_incremental() %}
        {% endif %}
        UNION ALL
        {% endif %}
        {% endfor %}
        );