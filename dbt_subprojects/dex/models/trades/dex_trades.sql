{% set chains = [
    'abstract'
    , 'apechain'
    , 'arbitrum'
    , 'avalanche_c'
    , 'base'
    , 'berachain'
    , 'blast'
    , 'bnb'
    , 'boba'
    , 'celo'
    , 'corn'
    , 'ethereum'
    , 'fantom'
    , 'flare'
    , 'flow'
    , 'gnosis'
    , 'hemi'
    , 'hyperevm'
    , 'ink'
    , 'kaia'
    , 'katana'
    , 'linea'
    , 'mantle'
    , 'mezo'
    , 'monad'
    , 'nova'
    , 'opbnb'
    , 'optimism'
    , 'peaq'
    , 'plasma'
    , 'plume'
    , 'polygon'
    , 'ronin'
    , 'scroll'
    , 'sei'
    , 'shape'
    , 'somnia'
    , 'sonic'
    , 'sophon'
    , 'story'
    , 'superseed'
    , 'tac'
    , 'taiko'
    , 'unichain'
    , 'worldchain'
    , 'zkevm'
    , 'zksync'
    , 'zora'
] %}

{% set chains_to_exclude = ['mezo', 'monad', 'story', 'tac'] %}
{% set exposed_chains = [] %}
{% for chain in chains %}
    {% if chain not in chains_to_exclude %}
        {% set _ = exposed_chains.append(chain) %}
    {% endif %}
{% endfor %}

{{ config(
    schema = 'dex'
    , alias = 'trades'
    , materialized = 'view'
    , post_hook='{{ expose_spells(blockchains = \'["' + exposed_chains | join('","') + '"]\',
                                    spell_type = "sector",
                                    spell_name = "dex",
                                    contributors = \'["hosuke", "0xrob", "jeff-dude", "tomfutago", "viniabussafi", "krishhh"]\') }}'
    )
}}

-- keep existing dbt lineages for the following projects, as the team built themselves and use the spells throughout the entire lineage.
{% set as_is_models = [
    ref('oneinch_lop_own_trades')
    , ref('zeroex_native_trades')
] %}

WITH chain_trades AS (
    {% for chain in chains %}
    SELECT
         blockchain
        , project
        , version
        , block_month
        , block_date
        , block_time
        , block_number
        , token_bought_symbol
        , token_sold_symbol
        , token_pair
        , token_bought_amount
        , token_sold_amount
        , token_bought_amount_raw
        , token_sold_amount_raw
        , amount_usd
        , token_bought_address
        , token_sold_address
        , taker
        , maker
        , project_contract_address
        , tx_hash
        , tx_from
        , tx_to
        , evt_index
    FROM
        {{ ref('dex_'~chain~'_trades') }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
, as_is_dexs AS (
    {% for model in as_is_models %}
    SELECT
        blockchain
        , project
        , version
        , block_month
        , block_date
        , block_time
        , block_number
        , token_bought_symbol
        , token_sold_symbol
        , token_pair
        , token_bought_amount
        , token_sold_amount
        , token_bought_amount_raw
        , token_sold_amount_raw
        , amount_usd
        , token_bought_address
        , token_sold_address
        , taker
        , maker
        , project_contract_address
        , tx_hash
        , tx_from
        , tx_to
        , evt_index
    FROM
        {{ model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)

{% set cte_to_union = [
    'chain_trades'
    , 'as_is_dexs'
    ]
%}

{% for cte in cte_to_union %}
    SELECT
        blockchain
        , project
        , version
        , block_month
        , block_date
        , block_time
        , block_number
        , token_bought_symbol
        , token_sold_symbol
        , token_pair
        , token_bought_amount
        , token_sold_amount
        , token_bought_amount_raw
        , token_sold_amount_raw
        , amount_usd
        , token_bought_address
        , token_sold_address
        , taker
        , maker
        , project_contract_address
        , tx_hash
        , tx_from
        , tx_to
        , evt_index
    FROM
        {{ cte }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
{% endfor %}
