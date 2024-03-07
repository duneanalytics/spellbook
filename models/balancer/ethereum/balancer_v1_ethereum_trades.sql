{{ config(
    schema = 'balancer_v1_ethereum',
    alias = 'trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_time', 'tx_hash', 'evt_index']
    )
}}

{% set project_start_date = '2020-03-13' %}


WITH 

swap_fees AS (
    SELECT * FROM (
        SELECT
            swaps.contract_address,
            swaps.evt_tx_hash,
            swaps.evt_block_time,
            swaps.evt_index,
            swaps.evt_block_number,
            fees.swapFee,
            ROW_NUMBER() OVER (PARTITION BY swaps.contract_address, evt_tx_hash, evt_index ORDER BY call_block_number DESC) AS row_num
        FROM {{ source('balancer_v1_ethereum', 'BPool_evt_LOG_SWAP') }} swaps
            LEFT JOIN {{ source('balancer_v1_ethereum', 'BPool_call_setSwapFee') }} fees
                ON fees.contract_address = swaps.contract_address
                AND fees.call_block_number < swaps.evt_block_number)
        WHERE row_num = 1
),

v1 AS (
    SELECT
        tokenOut AS token_bought_address,
        tokenAmountOut AS token_bought_amount_raw,
        tokenIn AS token_sold_address,
        tokenAmountIn AS token_sold_amount_raw,
        swaps.contract_address AS project_contract_address,
        (swapFee / 1e18) AS swap_fee_percentage,
        swaps.evt_block_time,
        swaps.evt_tx_hash,
        swaps.evt_index
    FROM {{ source('balancer_v1_ethereum', 'BPool_evt_LOG_SWAP') }} swaps
        LEFT JOIN swap_fees fees
            ON fees.evt_tx_hash = swaps.evt_tx_hash
            AND fees.evt_block_number = swaps.evt_block_number
            AND fees.contract_address = swaps.contract_address
            AND fees.evt_index = swaps.evt_index
    {% if not is_incremental() %}
        WHERE swaps.evt_block_time >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
        WHERE swaps.evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
),

prices AS (
    SELECT * 
    FROM {{ source('prices', 'usd') }}
    WHERE blockchain = 'ethereum'
    {% if not is_incremental() %}
        AND minute >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
        AND minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}
)


SELECT
    'ethereum' AS blockchain,
    'balancer' AS project,
    '1' AS version,
    DATE_TRUNC('DAY', evt_block_time) AS block_date,
    TRY_CAST(DATE_TRUNC('MONTH', evt_block_time) AS date) AS block_month,
    evt_block_time AS block_time,
    erc20a.symbol AS token_bought_symbol,
    erc20b.symbol AS token_sold_symbol,
    CASE
        WHEN lower(erc20a.symbol) > lower(erc20b.symbol) then concat(erc20b.symbol, '-', erc20a.symbol)
        ELSE concat(erc20a.symbol, '-', erc20b.symbol)
    END AS token_pair,
    token_bought_amount_raw / power(10, erc20a.decimals) AS token_bought_amount,
    token_sold_amount_raw / power(10, erc20b.decimals) AS token_sold_amount,
    CAST(token_bought_amount_raw AS UINT256) AS token_bought_amount_raw,
    CAST(token_sold_amount_raw AS UINT256) AS token_sold_amount_raw,
    coalesce(
        (token_bought_amount_raw / power(10, p_bought.decimals)) * p_bought.price,
        (token_sold_amount_raw / power(10, p_sold.decimals)) * p_sold.price
    ) AS amount_usd,
    token_bought_address,
    token_sold_address,
    tx."from" AS taker,
    CAST(NULL AS VARBINARY) AS maker,
    project_contract_address,
    CAST(NULL AS VARBINARY) AS pool_id,
    CAST(trades.swap_fee_percentage AS DOUBLE) AS swap_fee,
    evt_tx_hash AS tx_hash,
    tx."from" AS tx_from,
    tx.to AS tx_to,
    CAST(evt_index as BIGINT) as evt_index
FROM v1 trades
INNER JOIN {{ source('ethereum', 'transactions') }} tx
    ON trades.evt_tx_hash = tx.hash
    {% if not is_incremental() %}
    AND tx.block_time >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND tx.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
LEFT JOIN {{ source('tokens', 'erc20') }} erc20a
    ON trades.token_bought_address = erc20a.contract_address
    AND erc20a.blockchain = 'ethereum'
LEFT JOIN {{ source('tokens', 'erc20') }} erc20b
    ON trades.token_sold_address = erc20b.contract_address
    AND erc20b.blockchain = 'ethereum'
LEFT JOIN prices p_bought
    ON p_bought.minute = date_trunc('minute', trades.evt_block_time)
    AND p_bought.contract_address = trades.token_bought_address
LEFT JOIN prices p_sold
    ON p_sold.minute = date_trunc('minute', trades.evt_block_time)
    AND p_sold.contract_address = trades.token_sold_address
