{{ config(
    schema = 'dex'
    , alias = 'trades_beta'
    , partition_by = ['block_month', 'blockchain', 'project']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['blockchain', 'project', 'version', 'tx_hash', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

WITH curve AS (
    {{
        enrich_curve_dex_trades(
            base_trades = ref('dex_base_trades')
            , filter = "project = 'curve'"
            , curve_ethereum = ref('curvefi_ethereum_base_trades')
            , curve_optimism = ref('curvefi_optimism_base_trades')
            , tokens_erc20_model = source('tokens', 'erc20')
            , prices_model = source('prices', 'usd')
        )
    }}
)
, dexs AS (
    {{
        enrich_dex_trades(
            base_trades = ref('dex_base_trades')
            , filter = "project != 'curve'"
            , tokens_erc20_model = source('tokens', 'erc20')
        )
    }}
)
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
    , token_bought_address
    , token_sold_address
    , taker
    , maker
    , project_contract_address
    , tx_hash
    , tx_from
    , tx_to
    , evt_index
    , amount_usd
FROM
    curve
UNION ALL
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
    , token_bought_address
    , token_sold_address
    , taker
    , maker
    , project_contract_address
    , tx_hash
    , tx_from
    , tx_to
    , evt_index
    , amount_usd
FROM
    dexs