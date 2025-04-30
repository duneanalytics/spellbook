{{ config(
        schema='evms',
        alias = 'contracts',
        unique_key=['blockchain', 'address'],
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

{% set contracts_models = [
     ('abstract', source('abstract', 'contracts'))
     , ('apechain', source('apechain', 'contracts'))
     , ('arbitrum', source('arbitrum', 'contracts'))
     , ('avalanche_c', source('avalanche_c', 'contracts'))
     , ('b3', source('b3', 'contracts'))
     , ('base', source('base', 'contracts'))
     , ('berachain', source('berachain', 'contracts'))
     , ('blast', source('blast', 'contracts'))
     , ('bnb', source('bnb', 'contracts'))
     , ('bob', source('bob', 'contracts'))
     , ('boba', source('boba', 'contracts'))
     , ('celo', source('celo', 'contracts'))
     , ('corn', source('corn', 'contracts'))
     , ('degen', source('degen', 'contracts'))
     , ('ethereum', source('ethereum', 'contracts'))
     , ('fantom', source('fantom', 'contracts'))
     , ('flare', source('flare', 'contracts'))
     , ('gnosis', source('gnosis', 'contracts'))
     , ('ink', source('ink', 'contracts'))
     , ('kaia', source('kaia', 'contracts'))
     , ('lens', source('lens', 'contracts'))
     , ('linea', source('linea', 'contracts'))
     , ('mantle', source('mantle', 'contracts'))
     , ('nova', source('nova', 'contracts'))
     , ('opbnb', source('opbnb', 'contracts'))
     , ('optimism', source('optimism', 'contracts'))
     , ('polygon', source('polygon', 'contracts'))
     , ('ronin', source('ronin', 'contracts'))
     , ('scroll', source('scroll', 'contracts'))
     , ('sei', source('sei', 'contracts'))
     , ('shape', source('shape', 'contracts'))
     , ('sonic', source('sonic', 'contracts'))
     , ('sophon', source('sophon', 'contracts'))
     , ('unichain', source('unichain', 'contracts'))
     , ('viction', source('viction', 'contracts'))
     , ('worldchain', source('worldchain', 'contracts'))
     , ('zkevm', source('zkevm', 'contracts'))
     , ('zksync', source('zksync', 'contracts'))
     , ('zora', source('zora', 'contracts'))
] %}

SELECT *
FROM (
        {% for contracts_model in contracts_models %}
        SELECT
        '{{ contracts_model[0] }}' AS blockchain
        , abi_id
        , abi
        , address
        , "from"
        , code
        , name
        , namespace
        , dynamic
        , base
        , factory
        , detection_source
        , created_at
        , row_number() over (partition by address order by created_at desc) as duplicates_rank
        FROM {{ contracts_model[1] }}
        {% if not loop.last %}
        {% if is_incremental() %}
        {% endif %}
        UNION ALL
        {% endif %}
        {% endfor %}
        )
WHERE duplicates_rank = 1
