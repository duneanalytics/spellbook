{{ config(
        schema='evms',
        alias = 'blocks',
        unique_key=['blockchain', 'number'],
        post_hook='{{ expose_spells(\'[
                                        "abstract"
                                        , "apechain"
                                        , "arbitrum"
                                        , "avalanche_c"
                                        , "b3"
                                        , "base"
                                        , "berachain"
                                        , "blast"
                                        , "bnb"
                                        , "bob"
                                        , "boba"
                                        , "celo"
                                        , "corn"
                                        , "degen"
                                        , "ethereum"
                                        , "fantom"
                                        , "flare"
                                        , "gnosis"
                                        , "ink"
                                        , "kaia"
                                        , "lens"
                                        , "linea"
                                        , "mantle"
                                        , "nova"
                                        , "opbnb"
                                        , "optimism"
                                        , "polygon"
                                        , "ronin"
                                        , "scroll"
                                        , "sei"
                                        , "shape"
                                        , "sonic"
                                        , "sophon"
                                        , "unichain"
                                        , "viction"
                                        , "worldchain"
                                        , "zkevm"
                                        , "zksync"
                                        , "zora"
                                        ]\',
                                    "sector",
                                    "evms",
                                    \'["hildobby", "shogun", "allelosi"]\') }}'
        )
}}

{% set blocks_models = [
     ('abstract', source('abstract', 'blocks'))
     , ('apechain', source('apechain', 'blocks'))
     , ('arbitrum', source('arbitrum', 'blocks'))
     , ('avalanche_c', source('avalanche_c', 'blocks'))
     , ('b3', source('b3', 'blocks'))
     , ('base', source('base', 'blocks'))
     , ('berachain', source('berachain', 'blocks'))
     , ('blast', source('blast', 'blocks'))
     , ('bnb', source('bnb', 'blocks'))
     , ('bob', source('bob', 'blocks'))
     , ('boba', source('boba', 'blocks'))
     , ('celo', source('celo', 'blocks'))
     , ('corn', source('corn', 'blocks'))
     , ('degen', source('degen', 'blocks'))
     , ('ethereum', source('ethereum', 'blocks'))
     , ('fantom', source('fantom', 'blocks'))
     , ('flare', source('flare', 'blocks'))
     , ('gnosis', source('gnosis', 'blocks'))
     , ('ink', source('ink', 'blocks'))
     , ('kaia', source('kaia', 'blocks'))
     , ('lens', source('lens', 'blocks'))
     , ('linea', source('linea', 'blocks'))
     , ('mantle', source('mantle', 'blocks'))
     , ('nova', source('nova', 'blocks'))
     , ('opbnb', source('opbnb', 'blocks'))
     , ('optimism', source('optimism', 'blocks'))
     , ('polygon', source('polygon', 'blocks'))
     , ('ronin', source('ronin', 'blocks'))
     , ('scroll', source('scroll', 'blocks'))
     , ('sei', source('sei', 'blocks'))
     , ('shape', source('shape', 'blocks'))
     , ('sonic', source('sonic', 'blocks'))
     , ('sophon', source('sophon', 'blocks'))
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