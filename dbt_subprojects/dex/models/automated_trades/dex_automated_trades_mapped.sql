{{ config(
    schema = 'dex'
    , alias = 'automated_trades_mapped'
    , materialized = 'view'
    )
}}

WITH base_trades AS (
    SELECT *
    FROM {{ ref('dex_automated_trades_all') }}
)

SELECT 
    base_trades.blockchain,
    dex_map.project_name as project,
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
INNER JOIN {{ ref('dex_mapping') }} AS dex_map
    ON base_trades.factory_address = dex_map.factory
    AND base_trades.blockchain = dex_map.blockchain 