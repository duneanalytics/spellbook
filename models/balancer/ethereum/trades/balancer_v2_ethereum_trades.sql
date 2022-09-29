{{ config(
    schema = 'balancer_v2_ethereum',
    alias = 'trades',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'unique_trade_id'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "balancer_v2",
                                \'["mendesfabio"]\') }}'
    )
}}

WITH dexs AS (
    SELECT
        evt_block_time AS block_time,
        '' AS taker,
        '' AS maker,
        amountOut AS token_bought_amount_raw,
        amountIn AS token_sold_amount_raw,
        NULL AS amount_usd,
        tokenOut AS token_bought_address,
        tokenIn AS token_sold_address,
        poolId AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM {{ source('balancer_v2_ethereum', 'Vault_evt_Swap') }}
    WHERE tokenIn != SUBSTRING(poolId, 0, 42)
    AND tokenOut != SUBSTRING(poolId, 0, 42)
    {% if is_incremental() %}
    AND evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
)

SELECT
    'ethereum' AS blockchain,
    'balancer' AS project,
    '2' AS version,
    TRY_CAST(date_trunc('DAY', dexs.block_time) AS date) AS block_date,
    dexs.block_time,
    erc20a.symbol AS token_bought_symbol,
    erc20b.symbol AS token_sold_symbol,
    case
        when lower(erc20a.symbol) > lower(erc20b.symbol) then concat(erc20b.symbol, '-', erc20a.symbol)
        else concat(erc20a.symbol, '-', erc20b.symbol)
    end as token_pair,
    dexs.token_bought_amount_raw / power(10, erc20a.decimals) AS token_bought_amount,
    dexs.token_sold_amount_raw / power(10, erc20b.decimals) AS token_sold_amount,
    dexs.token_bought_amount_raw,
    dexs.token_sold_amount_raw,
    coalesce(
        dexs.amount_usd
        ,(dexs.token_bought_amount_raw / power(10, p_bought.decimals)) * p_bought.price
        ,(dexs.token_sold_amount_raw / power(10, p_sold.decimals)) * p_sold.price
    ) AS amount_usd,
    dexs.token_bought_address,
    dexs.token_sold_address,
    coalesce(dexs.taker, tx.from) AS taker, -- subqueries rely on this COALESCE to avoid redundant joins with the transactions table
    dexs.maker,
    dexs.project_contract_address,
    dexs.tx_hash,
    tx.from AS tx_from,
    tx.to AS tx_to,
    dexs.trace_address,
    dexs.evt_index,
    'balancer' ||'-'|| '2' ||'-'|| dexs.tx_hash ||'-'|| IFNULL(dexs.evt_index, '') ||'-'|| IFNULL(dexs.trace_address, '') AS unique_trade_id
FROM dexs
INNER JOIN {{ source('ethereum', 'transactions') }} tx
    ON tx.hash = dexs.tx_hash
    {% if not is_incremental() %}
    -- The date below is derrived from `select min(evt_block_time) from balancer_v2_ethereum.Vault_evt_PoolRegistered;`
    -- If dexs above is changed then this will also need to be changed.
    AND tx.block_time >= "2021-04-20 00:00:00"
    {% endif %}
    {% if is_incremental() %}
    AND tx.block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
LEFT JOIN {{ ref('tokens_erc20') }} erc20a ON erc20a.contract_address = dexs.token_bought_address AND erc20a.blockchain = 'ethereum'
LEFT JOIN {{ ref('tokens_erc20') }} erc20b ON erc20b.contract_address = dexs.token_sold_address  AND erc20b.blockchain = 'ethereum'
LEFT JOIN {{ source('prices', 'usd') }} p_bought ON p_bought.minute = date_trunc('minute', dexs.block_time)
    AND p_bought.contract_address = dexs.token_bought_address
    AND p_bought.blockchain = 'ethereum'
    {% if not is_incremental() %}
    -- The date below is derrived from `select min(evt_block_time) from balancer_v2_ethereum.Vault_evt_PoolRegistered;`
    -- If dexs above is changed then this will also need to be changed.
    AND p_bought.minute >= "2021-04-20 00:00:00"
    {% endif %}
    {% if is_incremental() %}
    AND p_bought.minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}
LEFT JOIN {{ source('prices', 'usd') }} p_sold ON p_sold.minute = date_trunc('minute', dexs.block_time)
    AND p_sold.contract_address = dexs.token_sold_address
    AND p_sold.blockchain = 'ethereum'
    {% if not is_incremental() %}
    -- The date below is derrived from `select min(evt_block_time) from balancer_v2_ethereum.Vault_evt_PoolRegistered;`
    -- If dexs above is changed then this will also need to be changed.
    AND p_sold.minute >= "2021-04-20 00:00:00"
    {% endif %}
    {% if is_incremental() %}
    AND p_sold.minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}