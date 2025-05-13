{{ config(
    schema = 'velodrome_ink'
    , alias = 'base_trades'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['tx_hash', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

WITH v3 as (
{{
    uniswap_compatible_v3_trades(
        blockchain = 'ink'
        , project = 'velodrome'
        , version = '3'
        , Pair_evt_Swap = source('velodrome_ink', 'clpool_evt_swap')
        , Factory_evt_PoolCreated = source('velodrome_ink', 'clfactory_evt_poolcreated')
        ,optional_columns = []
    )
}}
)
SELECT
    v3.blockchain,
    v3.project,
    v3.version,
    v3.block_month,
    v3.block_date,
    v3.block_time,
    v3.block_number,
    v3.token_bought_amount_raw,
    v3.token_sold_amount_raw,
    v3.token_bought_address,
    v3.token_sold_address,
    v3.taker,
    v3.maker,
    v3.project_contract_address,
    v3.tx_hash,
    v3.evt_index
FROM v3
