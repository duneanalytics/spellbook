{{ config(
        schema='evms',
        alias = 'creation_traces',
        unique_key=['blockchain', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["ethereum", "polygon", "bnb", "avalanche_c", "gnosis", "fantom", "optimism", "arbitrum", "celo", "base", "zksync", "zora", "scroll", "linea", "zkevm", "blast", "mantle", "ronin"]\',
                                    "sector",
                                    "evms",
                                    \'["hildobby"]\') }}'
        )
}}

{% set blockchains = [
       "avalanche_c"
       ,"arbitrum"
       , "base"
       , "blast"
       , "bnb"
       , "bob"
       , "boba"
       , "celo"
       , "ethereum"
       , "fantom"
       , "flare"
       , "gnosis"
       , "kaia"
       , "linea"
       , "mantle"
       , "nova"
       , "optimism"
       , "polygon"
       , "ronin"
       , "scroll"
       , "sei"
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