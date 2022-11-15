with dexs as (
    SELECT
        t.evt_block_time AS block_time,
        NULL AS taker, -- this relies on the outer query coalescing to tx."from"
        NULL AS maker,
        t.amountOut AS token_bought_amount_raw,
        t.amountIn AS token_sold_amount_raw,
        NULL AS amount_usd,
        t.tokenOut AS token_bought_address,
        t.tokenIn AS token_sold_address,
        t.poolId AS project_contract_address,
        t.evt_tx_hash AS tx_hash,
        '' AS trace_address,
        t.evt_index
    FROM
        balancer_v2_arbitrum.Vault_evt_Swap t
    )
SELECT 
    'Balancer' AS project,
    '2' AS version,
    dexs.block_time,
    erc20a.symbol AS token_bought_symbol,
    erc20b.symbol AS token_sold_symbol,
    dexs.token_bought_amount_raw / power(10, erc20a.decimals) AS token_bought_amount,
    dexs.token_sold_amount_raw / power(10, erc20b.decimals) AS token_sold_amount,
    tx.from as taker, -- subqueries rely on this COALESCE to avoid redundant joins with the transactions table
    maker,
    token_bought_amount_raw,
    token_sold_amount_raw,
    coalesce(
        dexs.amount_usd
        ,(dexs.token_bought_amount_raw / power(10, p_bought.decimals)) * p_bought.price
        ,(dexs.token_sold_amount_raw / power(10, p_sold.decimals)) * p_sold.price
    ) AS amount_usd,
    token_bought_address,
    token_sold_address,
    project_contract_address,
    tx_hash,
    tx.from as tx_from,
    tx.to as tx_to,
    trace_address,
    evt_index
    FROM dexs
    INNER JOIN arbitrum.transactions tx
        ON dexs.tx_hash = tx.hash
    LEFT JOIN tokens.erc20 erc20a ON erc20a.contract_address = dexs.token_bought_address and erc20a.blockchain = 'arbitrum'
    LEFT JOIN tokens.erc20 erc20b ON erc20b.contract_address = dexs.token_sold_address and erc20b.blockchain = 'arbitrum'
    LEFT JOIN prices.usd p_bought ON p_bought.minute = date_trunc('minute', dexs.block_time)
        AND p_bought.contract_address = dexs.token_bought_address
    LEFT JOIN prices.usd p_sold ON p_sold.minute = date_trunc('minute', dexs.block_time)
        AND p_sold.contract_address = dexs.token_sold_address