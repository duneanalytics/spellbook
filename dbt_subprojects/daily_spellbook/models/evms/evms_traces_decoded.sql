{{ config(
        schema='evms',
        alias = 'traces_decoded',
        unique_key=['blockchain', 'tx_hash', 'evt_index'],
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

{% set decodedtraces_models = [
     ('abstract', source('abstract', 'traces_decoded'))
     , ('apechain', source('apechain', 'traces_decoded'))
     , ('arbitrum', source('arbitrum', 'traces_decoded'))
     , ('avalanche_c', source('avalanche_c', 'traces_decoded'))
     , ('b3', source('b3', 'traces_decoded'))
     , ('base', source('base', 'traces_decoded'))
     , ('berachain', source('berachain', 'traces_decoded'))
     , ('blast', source('blast', 'traces_decoded'))
     , ('bnb', source('bnb', 'traces_decoded'))
     , ('bob', source('bob', 'traces_decoded'))
     , ('boba', source('boba', 'traces_decoded'))
     , ('celo', source('celo', 'traces_decoded'))
     , ('corn', source('corn', 'traces_decoded'))
     , ('degen', source('degen', 'traces_decoded'))
     , ('ethereum', source('ethereum', 'traces_decoded'))
     , ('fantom', source('fantom', 'traces_decoded'))
     , ('flare', source('flare', 'traces_decoded'))
     , ('gnosis', source('gnosis', 'traces_decoded'))
     , ('ink', source('ink', 'traces_decoded'))
     , ('kaia', source('kaia', 'traces_decoded'))
     , ('lens', source('lens', 'traces_decoded'))
     , ('linea', source('linea', 'traces_decoded'))
     , ('mantle', source('mantle', 'traces_decoded'))
     , ('nova', source('nova', 'traces_decoded'))
     , ('opbnb', source('opbnb', 'traces_decoded'))
     , ('optimism', source('optimism', 'traces_decoded'))
     , ('polygon', source('polygon', 'traces_decoded'))
     , ('ronin', source('ronin', 'traces_decoded'))
     , ('scroll', source('scroll', 'traces_decoded'))
     , ('sei', source('sei', 'traces_decoded'))
     , ('shape', source('shape', 'traces_decoded'))
     , ('sonic', source('sonic', 'traces_decoded'))
     , ('sophon', source('sophon', 'traces_decoded'))
     , ('unichain', source('unichain', 'traces_decoded'))
     , ('viction', source('viction', 'traces_decoded'))
     , ('worldchain', source('worldchain', 'traces_decoded'))
     , ('zkevm', source('zkevm', 'traces_decoded'))
     , ('zksync', source('zksync', 'traces_decoded'))
     , ('zora', source('zora', 'traces_decoded'))
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