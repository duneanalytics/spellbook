{{ config(
        schema='evms',
        alias = 'logs',
        unique_key=['blockchain', 'tx_hash'],
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

{% set logs_models = [
     ('abstract', source('abstract', 'logs'))
     , ('apechain', source('apechain', 'logs'))
     , ('arbitrum', source('arbitrum', 'logs'))
     , ('avalanche_c', source('avalanche_c', 'logs'))
     , ('b3', source('b3', 'logs'))
     , ('base', source('base', 'logs'))
     , ('berachain', source('berachain', 'logs'))
     , ('blast', source('blast', 'logs'))
     , ('bnb', source('bnb', 'logs'))
     , ('bob', source('bob', 'logs'))
     , ('boba', source('boba', 'logs'))
     , ('celo', source('celo', 'logs'))
     , ('corn', source('corn', 'logs'))
     , ('degen', source('degen', 'logs'))
     , ('ethereum', source('ethereum', 'logs'))
     , ('fantom', source('fantom', 'logs'))
     , ('flare', source('flare', 'logs'))
     , ('gnosis', source('gnosis', 'logs'))
     , ('ink', source('ink', 'logs'))
     , ('kaia', source('kaia', 'logs'))
     , ('lens', source('lens', 'logs'))
     , ('linea', source('linea', 'logs'))
     , ('mantle', source('mantle', 'logs'))
     , ('nova', source('nova', 'logs'))
     , ('opbnb', source('opbnb', 'logs'))
     , ('optimism', source('optimism', 'logs'))
     , ('polygon', source('polygon', 'logs'))
     , ('ronin', source('ronin', 'logs'))
     , ('scroll', source('scroll', 'logs'))
     , ('sei', source('sei', 'logs'))
     , ('shape', source('shape', 'logs'))
     , ('sonic', source('sonic', 'logs'))
     , ('sophon', source('sophon', 'logs'))
     , ('unichain', source('unichain', 'logs'))
     , ('viction', source('viction', 'logs'))
     , ('worldchain', source('worldchain', 'logs'))
     , ('zkevm', source('zkevm', 'logs'))
     , ('zksync', source('zksync', 'logs'))
     , ('zora', source('zora', 'logs'))
] %}

SELECT *
FROM (
        {% for logs_model in logs_models %}
        SELECT
        '{{ logs_model[0] }}' AS blockchain
        , block_time
        , block_number
        , block_hash
        , contract_address
        , topic0
        , topic1
        , topic2
        , topic3
        , data
        , tx_hash
        , index
        , tx_index
        , block_date
        , tx_from
        , tx_to
        FROM {{ logs_model[1] }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
        );