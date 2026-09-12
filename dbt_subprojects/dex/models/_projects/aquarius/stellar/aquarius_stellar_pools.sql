{{ config(
    schema = 'aquarius_stellar'
    , alias = 'pools'
    , materialized = 'table'
    , file_format = 'delta'
    , filtering_columns = ['pool']
    )
}}

-- Aquarius Stellar pool registry.
-- One row per pool from decoded add_pool / init_concentrated_pool events.
-- ci-stamp: 1

WITH created AS (
    SELECT
        blockchain
        , project
        , version
        , pool_address AS pool
        , pool_type
        , token0
        , token1
        , token2
        , block_time AS creation_block_time
        , ledger_sequence AS creation_block_number
        , contract_id AS contract_address
        , tx_hash
        , transaction_id
        , event_name
        , ROW_NUMBER() OVER (
            PARTITION BY pool_address
            ORDER BY block_time, CASE WHEN operation_id IS NOT NULL THEN 0 ELSE 1 END, transaction_id
        ) AS created_rank
    FROM {{ ref('aquarius_stellar_pool_evt') }}
    WHERE pool_address IS NOT NULL
)

SELECT
    blockchain
    , project
    , version
    , pool
    , pool_type
    , token0
    , token1
    , token2
    , creation_block_time
    , creation_block_number
    , contract_address
    , tx_hash
    , transaction_id
    , event_name
FROM created
WHERE created_rank = 1
