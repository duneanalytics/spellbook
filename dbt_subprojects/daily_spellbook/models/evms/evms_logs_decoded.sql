{{ config(
        schema='evms',
        alias = 'logs_decoded',
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
                                    \'["hildobby", "allelosi"]\') }}'
        )
}}

{% set decodedlogs_models = [
     ('abstract', source('abstract', 'logs_decoded'))
     , ('apechain', source('apechain', 'logs_decoded'))
     , ('arbitrum', source('arbitrum', 'logs_decoded'))
     , ('avalanche_c', source('avalanche_c', 'logs_decoded'))
     , ('b3', source('b3', 'logs_decoded'))
     , ('base', source('base', 'logs_decoded'))
     , ('berachain', source('berachain', 'logs_decoded'))
     , ('blast', source('blast', 'logs_decoded'))
     , ('bnb', source('bnb', 'logs_decoded'))
     , ('bob', source('bob', 'logs_decoded'))
     , ('boba', source('boba', 'logs_decoded'))
     , ('celo', source('celo', 'logs_decoded'))
     , ('corn', source('corn', 'logs_decoded'))
     , ('degen', source('degen', 'logs_decoded'))
     , ('ethereum', source('ethereum', 'logs_decoded'))
     , ('fantom', source('fantom', 'logs_decoded'))
     , ('flare', source('flare', 'logs_decoded'))
     , ('gnosis', source('gnosis', 'logs_decoded'))
     , ('ink', source('ink', 'logs_decoded'))
     , ('kaia', source('kaia', 'logs_decoded'))
     , ('lens', source('lens', 'logs_decoded'))
     , ('linea', source('linea', 'logs_decoded'))
     , ('mantle', source('mantle', 'logs_decoded'))
     , ('nova', source('nova', 'logs_decoded'))
     , ('opbnb', source('opbnb', 'logs_decoded'))
     , ('optimism', source('optimism', 'logs_decoded'))
     , ('polygon', source('polygon', 'logs_decoded'))
     , ('ronin', source('ronin', 'logs_decoded'))
     , ('scroll', source('scroll', 'logs_decoded'))
     , ('sei', source('sei', 'logs_decoded'))
     , ('shape', source('shape', 'logs_decoded'))
     , ('sonic', source('sonic', 'logs_decoded'))
     , ('sophon', source('sophon', 'logs_decoded'))
     , ('unichain', source('unichain', 'logs_decoded'))
     , ('viction', source('viction', 'logs_decoded'))
     , ('worldchain', source('worldchain', 'logs_decoded'))
     , ('zkevm', source('zkevm', 'logs_decoded'))
     , ('zksync', source('zksync', 'logs_decoded'))
     , ('zora', source('zora', 'logs_decoded'))
] %}

SELECT *
FROM (
        {% for decodedlogs_model in decodedlogs_models %}
        SELECT
        '{{ decodedlogs_model[0] }}' AS blockchain
        , block_time
        , block_number
        , index
        , contract_address
        , event_name
        , namespace
        , signature
        , tx_hash
        FROM {{ decodedlogs_model[1] }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
        );