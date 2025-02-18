{{ config(
    schema = 'dex'
    , alias = 'trades'
    , partition_by = ['block_month', 'blockchain', 'project']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['blockchain', 'project', 'version', 'tx_hash', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    , post_hook='{{ expose_spells(\'[
                                        "arbitrum"
                                        , "avalanche_c"
                                        , "base"
                                        , "blast"
                                        , "bnb"
                                        , "boba"
                                        , "celo"
                                        , "ethereum"
                                        , "fantom"
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
                                        , "zkevm"
                                        , "zksync"
                                        , "unichain"
                                        , "zora"
                                    ]\',
                                    "sector",
                                    "dex",
                                    \'["hosuke", "0xrob", "jeff-dude", "tomfutago", "viniabussafi"]\') }}')
}}

-- keep existing dbt lineages for the following projects, as the team built themselves and use the spells throughout the entire lineage
{% set as_is_models = [
    ref('oneinch_lop_own_trades')
    , ref('zeroex_native_trades')
] %}

WITH curve AS (
    -- due to curve having increased complexity to determine token_bought_amount / token_sold_amount, enrich separately
    {{
        enrich_curve_dex_trades(
            base_trades = ref('dex_base_trades')
            , filter = "project = 'curve'"
            , curve_ethereum = ref('curve_ethereum_base_trades')
            , curve_optimism = ref('curve_optimism_base_trades')
            , tokens_erc20_model = source('tokens', 'erc20')
        )
    }}
)
, balancer_v3 AS (
    -- due to Balancer V3 having trades between ERC4626 tokens, which won't be priced on prices.usd, enrich separately
    {{
        enrich_balancer_v3_dex_trades(
            base_trades = ref('dex_base_trades')
            , filter = "(project = 'balancer' AND version = '3')"
            , tokens_erc20_model = source('tokens', 'erc20')
        )
    }}
)
, dexs AS (
    {{
        enrich_dex_trades(
            base_trades = ref('dex_base_trades')
            , filter = "project != 'curve' AND NOT (project = 'balancer' AND version = '3')"
            , tokens_erc20_model = source('tokens', 'erc20')
        )
    }}
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
    {% if is_incremental() %}
    WHERE
        {{ incremental_predicate('block_time') }}
    {% endif %}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)


{% set cte_to_union = [
    'curve'
    , 'as_is_dexs'
    , 'dexs'
    , 'balancer_v3'
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