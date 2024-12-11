/*
    TODO: This model is pending for Swap event table
    Currently only PairCreated event is available
*/

{{
    config(
        schema = 'baryon_viction',
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
    uniswap_compatible_v2_trades(
        blockchain = 'viction',
        project = 'baryon',
        version = '2',
        Pair_evt_Swap = source('baryon_viction', 'CONTRACT_PAIR_evt_Swap'),
        Factory_evt_PairCreated = source('baryon_viction', 'CONTRACT_FACTORY_evt_PairCreated')
    )
}}
*/

-- Return empty result set for now
SELECT
    'viction' as blockchain,
    'baryon' as project,
    '2' as version,
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
