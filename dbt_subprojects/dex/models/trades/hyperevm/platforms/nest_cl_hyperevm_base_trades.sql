{{
    config(
        schema = 'nest_cl_hyperevm',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

WITH base_trades AS (
    {{
        uniswap_compatible_v3_trades(
            blockchain = 'hyperevm',
            project = 'nest',
            version = 'cl',
            Pair_evt_Swap = source('nest_hyperevm', 'algebrapool_evt_swap'),
            Factory_evt_PoolCreated = source('nest_hyperevm', 'algebrafactoryupgradeable_v3_evt_pool'),
            taker_column_name = 'recipient',
            optional_columns = [],
            pair_column_name = 'pool'
        )
    }}
),

deduplicated_trades AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY tx_hash, evt_index ORDER BY block_time) as rn
    FROM base_trades
)

SELECT
    blockchain,
    project,
    version,
    block_month,
    block_date,
    block_time,
    block_number,
    token_bought_amount_raw,
    token_sold_amount_raw,
    token_bought_address,
    token_sold_address,
    taker,
    maker,
    project_contract_address,
    tx_hash,
    evt_index
FROM deduplicated_trades
WHERE rn = 1
