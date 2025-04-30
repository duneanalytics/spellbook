{{ config(
        schema='evms',
        alias = 'traces',
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

{% set traces_models = [
     ('abstract', source('abstract', 'traces'))
     , ('apechain', source('apechain', 'traces'))
     , ('arbitrum', source('arbitrum', 'traces'))
     , ('avalanche_c', source('avalanche_c', 'traces'))
     , ('b3', source('b3', 'traces'))
     , ('base', source('base', 'traces'))
     , ('berachain', source('berachain', 'traces'))
     , ('blast', source('blast', 'traces'))
     , ('bnb', source('bnb', 'traces'))
     , ('bob', source('bob', 'traces'))
     , ('boba', source('boba', 'traces'))
     , ('celo', source('celo', 'traces'))
     , ('corn', source('corn', 'traces'))
     , ('degen', source('degen', 'traces'))
     , ('ethereum', source('ethereum', 'traces'))
     , ('fantom', source('fantom', 'traces'))
     , ('flare', source('flare', 'traces'))
     , ('gnosis', source('gnosis', 'traces'))
     , ('ink', source('ink', 'traces'))
     , ('kaia', source('kaia', 'traces'))
     , ('lens', source('lens', 'traces'))
     , ('linea', source('linea', 'traces'))
     , ('mantle', source('mantle', 'traces'))
     , ('nova', source('nova', 'traces'))
     , ('opbnb', source('opbnb', 'traces'))
     , ('optimism', source('optimism', 'traces'))
     , ('polygon', source('polygon', 'traces'))
     , ('ronin', source('ronin', 'traces'))
     , ('scroll', source('scroll', 'traces'))
     , ('sei', source('sei', 'traces'))
     , ('shape', source('shape', 'traces'))
     , ('sonic', source('sonic', 'traces'))
     , ('sophon', source('sophon', 'traces'))
     , ('unichain', source('unichain', 'traces'))
     , ('viction', source('viction', 'traces'))
     , ('worldchain', source('worldchain', 'traces'))
     , ('zkevm', source('zkevm', 'traces'))
     , ('zksync', source('zksync', 'traces'))
     , ('zora', source('zora', 'traces'))
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