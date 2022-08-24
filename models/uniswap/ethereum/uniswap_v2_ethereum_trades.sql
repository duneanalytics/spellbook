{{ config(
    schema = 'uniswap_v2_ethereum',
    alias = 'trades',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'unique_trade_id']
    )
}}
WITH dexs AS
(
    -- Uniswap v2
    SELECT
        t.evt_block_time AS block_time
        ,t.to AS taker
        ,'' AS maker
        ,CASE WHEN amount0Out = 0 THEN amount1Out ELSE amount0Out END AS token_bought_amount_raw
        ,CASE WHEN amount0In = 0 OR amount1Out = 0 THEN amount1In ELSE amount0In END AS token_sold_amount_raw
        ,NULL AS amount_usd
        ,CASE WHEN amount0Out = 0 THEN f.token1 ELSE f.token0 END AS token_bought_address
        ,CASE WHEN amount0In = 0 OR amount1Out = 0 THEN f.token1 ELSE f.token0 END AS token_sold_address
        ,t.contract_address AS project_contract_address
        ,t.evt_tx_hash AS tx_hash
        ,'' AS trace_address
        ,t.evt_index
    FROM
        {{ source('uniswap_v2_ethereum', 'Pair_evt_Swap') }} t
    INNER JOIN {{ source('uniswap_v2_ethereum', 'Factory_evt_PairCreated') }} f
        ON f.pair = t.contract_address
    WHERE t.contract_address NOT IN (
        '0xed9c854cb02de75ce4c9bba992828d6cb7fd5c71', -- remove WETH-UBOMB wash trading pair
        '0xf9c1fa7d41bf44ade1dd08d37cc68f67ae75bf92', -- remove WETH-WETH wash trading pair
        '0x854373387e41371ac6e307a1f29603c6fa10d872' ) -- remove FEG/ETH token pair
    {% if is_incremental() %}
    AND t.evt_block_time >= (SELECT MAX(block_time) FROM {{ this }})
    {% endif %}
)
SELECT
    'ethereum' AS blockchain
    ,'uniswap' AS project
    ,'2' AS version
    ,TRY_CAST(date_trunc('DAY', dexs.block_time) AS date) AS block_date
    ,dexs.block_time
    ,erc20a.symbol AS token_bought_symbol
    ,erc20b.symbol AS token_sold_symbol
    ,case
        when lower(erc20a.symbol) > lower(erc20b.symbol) then concat(erc20b.symbol, '-', erc20a.symbol)
        else concat(erc20a.symbol, '-', erc20b.symbol)
    end as token_pair
    ,dexs.token_bought_amount_raw / power(10, erc20a.decimals) AS token_bought_amount
    ,dexs.token_sold_amount_raw / power(10, erc20b.decimals) AS token_sold_amount
    ,dexs.token_bought_amount_raw
    ,dexs.token_sold_amount_raw
    ,coalesce(
        dexs.amount_usd
        ,(dexs.token_bought_amount_raw / power(10, p_bought.decimals)) * p_bought.price
        ,(dexs.token_sold_amount_raw / power(10, p_sold.decimals)) * p_sold.price
    ) AS amount_usd
    ,dexs.token_bought_address
    ,dexs.token_sold_address
    ,coalesce(dexs.taker, tx.from) AS taker -- subqueries rely on this COALESCE to avoid redundant joins with the transactions table
    ,dexs.maker
    ,dexs.project_contract_address
    ,dexs.tx_hash
    ,tx.from AS tx_from
    ,tx.to AS tx_to
    ,dexs.trace_address
    ,dexs.evt_index
    ,'uniswap' ||'-'|| '2' ||'-'|| dexs.tx_hash ||'-'|| IFNULL(dexs.evt_index, '') ||'-'|| IFNULL(dexs.trace_address, '') AS unique_trade_id
FROM dexs
INNER JOIN {{ source('ethereum', 'transactions') }} tx
    ON tx.hash = dexs.tx_hash
    {% if not is_incremental() %}
    AND tx.block_time >= (SELECT MIN(block_time) FROM dexs)
    {% endif %}
    {% if is_incremental() %}
    AND TRY_CAST(date_trunc('DAY', tx.block_time) AS date) = TRY_CAST(date_trunc('DAY', dexs.block_time) AS date)
    {% endif %}
LEFT JOIN {{ ref('tokens_ethereum_erc20') }} erc20a ON erc20a.contract_address = dexs.token_bought_address
LEFT JOIN {{ ref('tokens_ethereum_erc20') }} erc20b ON erc20b.contract_address = dexs.token_sold_address
LEFT JOIN {{ source('prices', 'usd') }} p_bought ON p_bought.minute = date_trunc('minute', dexs.block_time)
    AND p_bought.contract_address = dexs.token_bought_address
    AND p_bought.blockchain = 'ethereum'
    {% if not is_incremental() %}
    AND p_bought.minute >= (SELECT MIN(block_time) FROM dexs)
    {% endif %}
    {% if is_incremental() %}
    AND p_bought.minute >= (SELECT MAX(block_time) FROM {{ this }})
    {% endif %}
LEFT JOIN {{ source('prices', 'usd') }} p_sold ON p_sold.minute = date_trunc('minute', dexs.block_time)
    AND p_sold.contract_address = dexs.token_sold_address
    AND p_sold.blockchain = 'ethereum'
    {% if not is_incremental() %}
    AND p_sold.minute >= (SELECT MIN(block_time) FROM dexs)
    {% endif %}
    {% if is_incremental() %}
    AND p_sold.minute >= (SELECT MAX(block_time) FROM {{ this }})
    {% endif %}