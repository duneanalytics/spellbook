{{ config(
    schema = 'pharaoh_avalanche_c'
    , alias = 'base_trades'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['tx_hash', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

WITH dex_raw AS (
    {{
        uniswap_compatible_v3_trades(
            blockchain = 'avalanche_c'
            , project = 'pharaoh'
            , version = '1'
            , Pair_evt_Swap = source('pharaoh_avalanche_c', 'ClPool_evt_Swap')
            , Factory_evt_PoolCreated = source('pharaoh_avalanche_c', 'ClPoolFactory_evt_PoolCreated')
        )
    }}
)

SELECT
    dex_raw.blockchain,
    dex_raw.project,
    dex_raw.version,
    dex_raw.block_month,
    dex_raw.block_date,
    dex_raw.block_time,
    dex_raw.block_number,
    CASE
        WHEN router.evt_tx_hash IS NULL
            THEN dex_raw.token_bought_amount_raw
            ELSE router.amountOut
        END AS token_bought_amount_raw,
    CASE
        WHEN router.evt_tx_hash IS NULL
            THEN dex_raw.token_sold_amount_raw
            ELSE router.inputAmount
        END AS token_sold_amount_raw,
    dex_raw.token_bought_address,
    dex_raw.token_sold_address,
    dex_raw.taker,
    dex_raw.maker,
    dex_raw.project_contract_address,
    dex_raw.tx_hash,
    dex_raw.evt_index
FROM dex_raw
LEFT JOIN {{ source('glacier_avalanche_c', 'OdosRouterV2_evt_Swap') }} AS router
ON dex_raw.tx_hash = router.evt_tx_hash
AND dex_raw.evt_index+2 = router.evt_index