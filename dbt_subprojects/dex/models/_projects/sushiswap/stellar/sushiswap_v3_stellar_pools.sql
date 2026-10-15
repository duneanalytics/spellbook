{{ config(
    schema = 'sushiswap_v3_stellar'
    , alias = 'pools'
    , materialized = 'view'
    , filtering_columns = ['pool']
    )
}}

-- SushiSwap V3 Stellar pool registry.
-- One row per pool from decoded pool_created events. Rank is computed here
-- over the full pool_evt table so incremental twins stay consistent.

WITH created AS (
    SELECT
        blockchain
        , project
        , version
        , pool_address AS pool
        , fee
        , token0
        , token1
        , tick_spacing
        , block_time AS creation_block_time
        , ledger_sequence AS creation_block_number
        , contract_id AS contract_address
        , sender
        , tx_hash
        , transaction_id
        , ROW_NUMBER() OVER (
            PARTITION BY pool_address
            ORDER BY block_time, CASE WHEN operation_id IS NOT NULL THEN 0 ELSE 1 END, transaction_id
        ) AS created_rank
    FROM {{ ref('sushiswap_v3_stellar_pool_evt') }}
    WHERE event_name = 'pool_created'
        AND pool_address IS NOT NULL
)

SELECT
    blockchain
    , project
    , version
    , pool
    , fee
    , token0
    , token1
    , tick_spacing
    , creation_block_time
    , creation_block_number
    , contract_address
    , sender
    , tx_hash
    , transaction_id
FROM created
WHERE created_rank = 1
