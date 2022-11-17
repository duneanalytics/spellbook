{{ config(
    schema = 'balancer_v2_arbitrum',
    alias = 'trades',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address'],
    post_hook='{{ expose_spells(\'["arbitrum"]\',
                                "project",
                                "balancer_v2",
                                \'["mendesfabio", "jacektrocinski"]\') }}'
    )
}}

{% set project_start_date = '2021-04-20' %}

WITH dexs AS (
    SELECT
        vault_swap.evt_block_time AS block_time,
        '' AS taker,
        '' AS maker,
        vault_swap.amountOut AS token_bought_amount_raw,
        vault_swap.amountIn AS token_sold_amount_raw,
        CAST(NULL as double) AS amount_usd,
        vault_swap.tokenOut AS token_bought_address,
        vault_swap.tokenIn AS token_sold_address,
        vault_swap.poolId AS project_contract_address,
        vault_swap.evt_tx_hash AS tx_hash,
        '' AS trace_address,
        vault_swap.evt_index
    FROM {{ source('balancer_v2_arbitrum', 'Vault_evt_Swap') }} vault_swap
    LEFT JOIN {{ ref('balancer_v2_arbitrum_pools_fees') }} pools_fees
        ON pools_fees.contract_address = SUBSTRING(vault_swap.poolId, 0, 42)
        AND vault_swap.evt_block_time = (
            SELECT MAX(evt_block_time)
            FROM balancer_v2.view_pools_fees
            WHERE evt_block_time <= vault_swap.evt_block_time
            AND contract_address = SUBSTRING(vault_swap.`poolId` from 0 for 21)
        )
    WHERE
        vault_swap.tokenIn != SUBSTRING(vault_swap.`poolId`, 0, 42)
        AND vault_swap.tokenOut != SUBSTRING(vault_swap.`poolId`, 0, 42)
        {% if is_incremental() %}
        AND vault_swap.evt_block_time >= DATE_TRUNC("day", NOW() - interval '1 week')
        {% endif %}
)
SELECT
    'arbitrum' AS blockchain,
    'balancer' AS project,
    '2' AS version,
    TRY_CAST(DATE_TRUNC('DAY', dexs.block_time) AS DATE) AS block_date,
    dexs.block_time,
    erc20a.symbol AS token_bought_symbol,
    erc20b.symbol AS token_sold_symbol,
    CASE
        WHEN LOWER(erc20a.symbol) > LOWER(erc20b.symbol) THEN CONCAT(erc20b.symbol, '-', erc20a.symbol)
        ELSE CONCAT(erc20a.symbol, '-', erc20b.symbol)
    END AS token_pair,
    dexs.token_bought_amount_raw / POWER(10, erc20a.decimals) AS token_bought_amount,
    dexs.token_sold_amount_raw / POWER(10, erc20b.decimals) AS token_sold_amount,
    dexs.token_bought_amount_raw,
    dexs.token_sold_amount_raw,
    COALESCE(
        dexs.amount_usd
        ,(dexs.token_bought_amount_raw / POWER(10, p_bought.decimals)) * p_bought.price
        ,(dexs.token_sold_amount_raw / POWER(10, p_sold.decimals)) * p_sold.price
    ) AS amount_usd,
    dexs.token_bought_address,
    dexs.token_sold_address,
    -- Subqueries rely on this COALESCE to avoid redundant joins with the transactions table.
    COALESCE(dexs.taker, tx.from) AS taker, 
    dexs.maker,
    dexs.project_contract_address,
    dexs.tx_hash,
    tx.from AS tx_from,
    tx.to AS tx_to,
    dexs.trace_address,
    dexs.evt_index
FROM dexs
INNER JOIN {{ source('arbitrum', 'transactions') }} tx
    ON tx.hash = dexs.tx_hash
    {% if not is_incremental() %}
    AND tx.block_time >= '{{ project_start_date }}'
    {% endif %}
    {% if is_incremental() %}
    AND tx.block_time >= DATE_TRUNC("day", NOW() - interval '1 week')
    {% endif %}
LEFT JOIN {{ ref('tokens_erc20') }} erc20a
    ON erc20a.contract_address = dexs.token_bought_address
    AND erc20a.blockchain = 'arbitrum'
LEFT JOIN {{ ref('tokens_erc20') }} erc20b
    ON erc20b.contract_address = dexs.token_sold_address
    AND erc20b.blockchain = 'arbitrum'
LEFT JOIN {{ source('prices', 'usd') }} p_bought
    ON p_bought.minute = DATE_TRUNC('minute', dexs.block_time)
    AND p_bought.contract_address = dexs.token_bought_address
    AND p_bought.blockchain = 'arbitrum'
    {% if not is_incremental() %}
    AND p_bought.minute >= '{{ project_start_date }}'
    {% endif %}
    {% if is_incremental() %}
    AND p_bought.minute >= DATE_TRUNC("day", NOW() - interval '1 week')
    {% endif %}
LEFT JOIN {{ source('prices', 'usd') }} p_sold
    ON p_sold.minute = DATE_TRUNC('minute', dexs.block_time)
    AND p_sold.contract_address = dexs.token_sold_address
    AND p_sold.blockchain = 'arbitrum'
    {% if not is_incremental() %}
    AND p_sold.minute >= '{{ project_start_date }}'
    {% endif %}
    {% if is_incremental() %}
    AND p_sold.minute >= DATE_TRUNC("day", NOW() - interval '1 week')
    {% endif %}
;