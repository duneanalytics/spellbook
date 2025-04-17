{{ config(
        schema='evms',
        alias = 'blocks',
        unique_key=['blockchain', 'number'],
        post_hook='{{ expose_spells(\'[
                                        "arbitrum"
                                        , "avalanche_c"
                                        , "base"
                                        , "blast"
                                        , "bnb"
                                        , "boba"
                                        , "celo"
                                        , "corn"
                                        , "ethereum"
                                        , "fantom"
                                        , "flare"
                                        , "gnosis"
                                        , "ink"
                                        , "kaia"
                                        , "linea"
                                        , "nova"
                                        , "optimism"
                                        , "polygon"
                                        , "ronin"
                                        , "scroll"
                                        , "sei"
                                        , "shape"
                                        , "sonic"
                                        , "opbnb"
                                        , "unichain"
                                        , "viction"
                                        , "worldchain"
                                        , "zksync"
                                        , "zkevm"
                                        , "zora"
                                        ]\',
                                    "sector",
                                    "evms",
                                    \'["hildobby", "shogun", "allelosi" ]\') }}'
        )
}}

{% set blocks_models = [
     ('arbitrum', source('arbitrum', 'blocks'))
     , ('avalanche_c', source('avalanche_c', 'blocks'))
     , ('base', source('base', 'blocks'))
     , ('blast', source('blast', 'blocks'))
     , ('bnb', source('bnb', 'blocks'))
     , ('boba', source('boba', 'blocks'))
     , ('celo', source('celo', 'blocks'))
     , ('corn', source('corn', 'blocks'))
     , ('ethereum', source('ethereum', 'blocks'))
     , ('fantom', source('fantom', 'blocks'))
     , ('flare', source('flare', 'blocks'))
     , ('gnosis', source('gnosis', 'blocks'))
     , ('ink', source('ink', 'blocks'))
     , ('kaia', source('kaia', 'blocks'))
     , ('linea', source('linea', 'blocks'))
     , ('nova', source('nova', 'blocks'))
     , ('optimism', source('optimism', 'blocks'))
     , ('opbnb', source('opbnb', 'blocks'))
     , ('polygon', source('polygon', 'blocks'))
     , ('ronin', source('ronin', 'blocks'))
     , ('scroll', source('scroll', 'blocks'))
     , ('sei', source('sei', 'blocks'))
     , ('shape', source('shape', 'blocks'))
     , ('sonic', source('sonic', 'blocks'))
     , ('unichain', source('unichain', 'blocks'))
     , ('viction', source('viction', 'blocks'))
     , ('worldchain', source('worldchain', 'blocks'))
     , ('zkevm', source('zkevm', 'blocks'))
     , ('zksync', source('zksync', 'blocks'))
     , ('zora', source('zora', 'blocks'))
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