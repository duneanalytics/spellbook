/*
    TODO: This model is pending for Swap event table
    Currently only PoolCreated event is available
*/

{{
    config(
        schema = 'rabbitswap_xyz_viction',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

/* Uncomment when Swap event table is available
{{
    uniswap_compatible_v3_trades(
        blockchain = 'viction',
        project = 'rabbitswap',
        version = '3',
        Pair_evt_Swap = source('rabbitswap_xyz_viction', 'RabbitSwapV3Pool_evt_Swap'),
        Factory_evt_PoolCreated = source('rabbitswap_xyz_viction', 'RabbitSwapV3Factory_evt_PoolCreated')
    )
}}
*/

-- Return empty result set for now
SELECT
    'viction' as blockchain,
    'rabbitswap' as project,
    '3' as version,
    CAST(NULL as timestamp) as block_month,
    CAST(NULL as date) as block_date,
    CAST(NULL as timestamp) as block_time,
    CAST(NULL as bigint) as block_number,
    CAST(NULL as double) as token_bought_amount_raw,
    CAST(NULL as double) as token_sold_amount_raw,
    CAST(NULL as string) as token_bought_address,
    CAST(NULL as string) as token_sold_address,
    CAST(NULL as string) as taker,
    CAST(NULL as string) as maker,
    CAST(NULL as string) as project_contract_address,
    CAST(NULL as string) as tx_hash,
    CAST(NULL as integer) as evt_index
WHERE 1=0
