{{ config(
        schema='evms',
        alias = 'creation_traces',
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

{% set blockchains = [
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
] %}

{% set creation_traces_models = [] %}
{% for blockchain in blockchains %}
    {% do creation_traces_models.append((blockchain, source(blockchain, 'creation_traces'))) %}
{% endfor %}

SELECT
        *
FROM 
(
        {% for creation_traces_model in creation_traces_models %}
        SELECT
                '{{ creation_traces_model[0] }}' AS blockchain
                , block_time
                , block_number
                , tx_hash
                , address
                , "from"
                , code
                --, tx_from
                --, tx_to
        FROM {{ creation_traces_model[1] }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
)