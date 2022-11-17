{{ config(
    schema = 'balancer_v2_ethereum',
    alias = 'trades',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "balancer_v2",
                                \'["mendesfabio", "jacektrocinski"]\') }}'
    )
}}

{% set project_start_date = '2021-04-20' %}

WITH swap_fees AS (
    SELECT
        swap.evt_block_number,
        swap.evt_tx_hash,
        swap.evt_index,
        SUBSTRING(swap.`poolId`, 0, 42) AS contract_address,
        swap.evt_block_time,
        MAX(fees.evt_block_time) AS max_fee_evt_block_time
    FROM
        {{ source ('balancer_v2_ethereum', 'Vault_evt_Swap') }} swap
        LEFT JOIN {{ ref('balancer_v2_ethereum_pools_fees') }} fees
            ON fees.contract_address = SUBSTRING(swap.`poolId`, 0, 42)
            AND fees.evt_block_time <= swap.evt_block_time
    GROUP BY 1, 2, 3, 4, 5
),
dexs AS (
    SELECT
        swap.evt_block_time AS block_time,
        NULL AS taker,
        NULL AS maker,
        swap.`amountOut` AS token_bought_amount_raw,
        swap.`amountIn` AS token_sold_amount_raw,
        NULL AS amount_usd,
        swap.`tokenOut` AS token_bought_address,
        swap.`tokenIn` AS token_sold_address,
        swap.`poolId` AS project_contract_address,
        pools_fees.swap_fee_percentage  / POWER(10, 18) AS swap_fee,
        swap.evt_tx_hash AS tx_hash,
        NULL AS trace_address,
        swap.evt_index
    FROM
        swap_fees
        INNER JOIN {{ source ('balancer_v2_ethereum', 'Vault_evt_Swap') }} swap
            ON swap.evt_block_number = swap_fees.evt_block_number
            AND swap.evt_tx_hash = swap_fees.evt_tx_hash
            AND swap.evt_index = swap_fees.evt_index
        LEFT JOIN {{ ref('balancer_v2_ethereum_pools_fees') }}
            ON pools_fees.contract_address = swap_fees.contract_address
            AND pools_fees.evt_block_time = swap_fees.max_fee_evt_block_time
    WHERE
        swap.tokenIn != swap_fees.contract_address
        AND swap.tokenOut != swap_fees.contract_address
        -- Incremental logic.
        {% if is_incremental() %}
        AND swap.evt_block_time >= DATE_TRUNC("day", NOW() - interval '1 week')
        {% endif %}
)
SELECT
    'ethereum' AS blockchain,
    'balancer' AS project,
    '2' AS version,
    TRY_CAST (DATE_TRUNC('DAY', dexs.block_time) AS DATE) AS block_date,
    dexs.block_time,
    erc20a.symbol AS token_bought_symbol,
    erc20b.symbol AS token_sold_symbol,
    CASE WHEN LOWER(erc20a.symbol) > LOWER(erc20b.symbol) THEN
        CONCAT(erc20b.symbol, '-', erc20a.symbol)
    ELSE
        CONCAT(erc20a.symbol, '-', erc20b.symbol)
    END AS token_pair,
    dexs.token_bought_amount_raw / POWER(10, erc20a.decimals) AS token_bought_amount,
    dexs.token_sold_amount_raw / POWER(10, erc20b.decimals) AS token_sold_amount,
    dexs.token_bought_amount_raw,
    dexs.token_sold_amount_raw,
    COALESCE(
        dexs.amount_usd,
        dexs.token_bought_amount_raw / POWER(10, p_bought.decimals) * p_bought.price,
        dexs.token_sold_amount_raw / POWER(10, p_sold.decimals) * p_sold.price
    ) AS amount_usd,
    dexs.token_bought_address,
    dexs.token_sold_address,
    -- Subqueries rely on this COALESCE to avoid redundant joins with the transactions table.
    COALESCE(dexs.taker, tx.from) AS taker,
    dexs.maker,
    dexs.project_contract_address,
    dexs.swap_fee_percentage,
    dexs.tx_hash,
    tx.from AS tx_from,
    tx.to AS tx_to,
    dexs.trace_address,
    dexs.evt_index
FROM
    dexs
    INNER JOIN {{ source ('ethereum', 'transactions') }} tx
        ON tx.hash = dexs.tx_hash
        -- Incremental logic.
        {% if not is_incremental () %}
        AND tx.block_time >= '{{ project_start_date }}'
        {% endif %}
        {% if is_incremental () %}
        AND tx.block_time >= DATE_TRUNC("day", NOW() - interval '1 week')
        {% endif %}
    LEFT JOIN {{ ref ('tokens_erc20') }} erc20a
        ON erc20a.contract_address = dexs.token_bought_address
        AND erc20a.blockchain = 'ethereum'
    LEFT JOIN {{ ref ('tokens_erc20') }} erc20b
        ON erc20b.contract_address = dexs.token_sold_address
        AND erc20b.blockchain = 'ethereum'
    LEFT JOIN {{ source ('prices', 'usd') }} p_bought
        ON p_bought.minute = DATE_TRUNC('minute', dexs.block_time)
        AND p_bought.contract_address = dexs.token_bought_address
        AND p_bought.blockchain = 'ethereum'
        -- Incremental logic.
        {% if not is_incremental () %}
        AND p_bought.minute >= '{{ project_start_date }}'
        {% endif %}
        {% if is_incremental () %}
        AND p_bought.minute >= DATE_TRUNC("day", NOW() - interval '1 week')
        {% endif %}
    LEFT JOIN {{ source ('prices', 'usd') }} p_sold 
        ON p_sold.minute = DATE_TRUNC('minute', dexs.block_time)
        AND p_sold.contract_address = dexs.token_sold_address
        AND p_sold.blockchain = 'ethereum' 
        -- Incremental logic.
        {% if not is_incremental () %}
        AND p_sold.minute >= '{{ project_start_date }}'
        {% endif %}
        {% if is_incremental () %}
        AND p_sold.minute >= DATE_TRUNC("day", NOW() - interval '1 week')
        {% endif %}
;
