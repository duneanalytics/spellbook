{{
    config(
        schema = 'jelly_sei',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

WITH trades AS (
    SELECT * FROM (
        SELECT
            swaps.poolId,
            swaps.evt_tx_hash,
            swaps.evt_index,
            swaps.evt_block_number,
            bytearray_substring(swaps.poolId, 1, 20) AS contract_address,
            fees.swap_fee_percentage,
            ROW_NUMBER() OVER (PARTITION BY poolId, evt_tx_hash, evt_index ORDER BY block_number DESC, index DESC) AS rn
        FROM {{ source('jelly_swap_sei', 'Vault_evt_Swap') }} swaps
        {% if is_incremental() %}
        WHERE {{ incremental_predicate('swaps.evt_block_time') }}
        {% endif %}
    ) t
    WHERE t.rn = 1
)

-- , pools AS (
--     SELECT
--         'sei' AS blockchain,
--         poolId,
--         poolAddress AS pool_address,
--     FROM {{ source('jelly_swap_sei', 'Vault_evt_PoolRegistered') }}
-- )

, dexs AS (
    SELECT
        trade.evt_block_number AS block_number,
        trade.evt_block_time AS block_time,
        CAST(NULL AS VARBINARY) AS taker,
        CAST(NULL AS VARBINARY) AS maker,
        trade.amountOut AS token_bought_amount_raw,
        trade.amountIn AS token_sold_amount_raw,
        trade.tokenOut AS token_bought_address,
        trade.tokenIn AS token_sold_address,
        trade.contract_address AS project_contract_address,
        trade.evt_tx_hash AS tx_hash,
        trade.evt_index
    FROM trades trade
    LEFT JOIN pools p
        ON p.poolId = trade.poolId
)

SELECT
    'sei' AS blockchain,
    'jelly' AS project,
    '1' AS version,
    CAST(date_trunc('month', dexs.block_time) AS date) AS block_month,
    CAST(date_trunc('day', dexs.block_time) AS date) AS block_date,
    dexs.block_time,
    dexs.block_number,
    dexs.token_bought_amount_raw,
    dexs.token_sold_amount_raw,
    dexs.token_bought_address,
    dexs.token_sold_address,
    dexs.taker,
    dexs.maker,
    dexs.project_contract_address,
    dexs.tx_hash,
    dexs.evt_index
FROM dexs