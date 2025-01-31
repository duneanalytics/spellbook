{{ config(
    schema = 'dex'
    , alias = 'automated_trades_unmapped'
    , partition_by = ['blockchain', 'block_date']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['blockchain', 'block_number', 'tx_index', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

WITH base_trades AS (
    SELECT *
    FROM {{ ref('dex_automated_trades_all') }}
),

invalid_factories AS (
    SELECT DISTINCT factory_address, blockchain
    FROM base_trades bt
    WHERE NOT EXISTS (
        SELECT 1
        FROM {{ source('tokens', 'transfers') }} t1
        WHERE bt.blockchain = t1.blockchain
        AND bt.token_bought_address = t1.contract_address
        AND bt.block_date = t1.block_date
        AND bt.tx_hash = t1.tx_hash
    )
    AND NOT EXISTS (
        SELECT 1
        FROM {{ source('tokens', 'transfers') }} t2
        WHERE bt.blockchain = t2.blockchain
        AND bt.token_sold_address = t2.contract_address
        AND bt.block_date = t2.block_date
        AND bt.tx_hash = t2.tx_hash
    )
)

SELECT 
    base_trades.blockchain,
    concat('unknown-', base_trades.dex_type, '-', cast(varbinary_substring(base_trades.factory_address, 1, 5) as varchar)) as project,
    base_trades.version,
    base_trades.factory_address,
    base_trades.dex_type,
    base_trades.block_month,
    base_trades.block_date,
    base_trades.block_time,
    base_trades.block_number,
    base_trades.token_bought_symbol,
    base_trades.token_sold_symbol,
    base_trades.token_pair,
    base_trades.token_bought_amount,
    base_trades.token_sold_amount,
    base_trades.token_bought_amount_raw,
    base_trades.token_sold_amount_raw,
    base_trades.amount_usd,
    base_trades.token_bought_address,
    base_trades.token_sold_address,
    base_trades.taker,
    base_trades.maker,
    base_trades.project_contract_address,
    base_trades.tx_hash,
    base_trades.tx_from,
    base_trades.tx_to,
    base_trades.evt_index,
    base_trades.tx_index
FROM base_trades
WHERE NOT EXISTS (
    SELECT 1 
    FROM {{ ref('dex_mapping') }} AS dex_map
    WHERE base_trades.blockchain = dex_map.blockchain
    AND base_trades.factory_address = dex_map.factory_address
)
AND NOT EXISTS (
    SELECT 1
    FROM invalid_factories
    WHERE base_trades.blockchain = invalid_factories.blockchain
    AND base_trades.factory_address = invalid_factories.factory_address
)

