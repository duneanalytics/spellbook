{{ config(
    schema = 'uniswap_v3_arbitrum',
    alias = 'trades',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address'],
    post_hook='{{ expose_spells(\'["arbitrum"]\',
                                "project",
                                "uniswap_v3",
                                \'["jeff-dude", "markusbkoch", "masquot", "milkyklim", "0xBoxer", "mewwts", "hagaetc","mtitus6"]\') }}'
    )
}}

{% set project_start_date = '2021-06-01' %}

-- Uniswap v3 trades are derived from the Swap event emitted by the pool contract
-- The Swap event contains the amount of token0 and token1 that were bought and sold
-- The Swap event also contains the address of the pool contract
-- The pool address can be used to join with the UniswapV3Factory_evt_PoolCreated event
-- The UniswapV3Factory_evt_PoolCreated event contains the addresses of token0 and token1
-- The addresses of token0 and token1 can be used to join with the tokens_erc20 table
-- The tokens_erc20 table contains the symbol and decimals of token0 and token1
-- The symbol and decimals of token0 and token1 can be used to calculate the display amount of token0 and token1 that were bought and sold
-- The amount of token0 and token1 that were bought and sold can be used to calculate the amount of USD that was bought and sold
-- The amount of USD that was bought and sold can be used to calculate the price of token0 and token1
-- The prices.usd table contains the price of token0 and token1 and is joined using the contract_address of token0 and token1
-- The Swap event does not contain the address of the trader that bought and sold token0 and token1
-- The address of the trader that bought and sold token0 and token1 can be derived from the transaction that emitted the Swap event
-- The transaction that emitted the Swap event can be joined with the transactions table
-- The transactions table contains the address of the trader that bought and sold token0 and token1
-- The address of the trader that bought and sold token0 and token1 can be used to calculate the amount of USD that was bought and sold

WITH dexs AS
(
    --Uniswap v3
    SELECT
        t.evt_block_time AS block_time
        ,t.recipient AS taker
        ,'' AS maker -- Uniswap v3 does not have a maker
        ,CASE WHEN amount0 < '0' THEN abs(amount0) ELSE abs(amount1) END AS token_bought_amount_raw -- when amount0 is negative it means trader_a is buying token0 from the pool
        ,CASE WHEN amount0 < '0' THEN abs(amount1) ELSE abs(amount0) END AS token_sold_amount_raw -- when amount0 is negative it means trader_a is buying token1 from the pool
        ,NULL AS amount_usd -- Uniswap v3 does not have a price oracle in the Swap event
        ,CASE WHEN amount0 < '0' THEN f.token0 ELSE f.token1 END AS token_bought_address -- when amount0 is negative it means trader_a is buying token0 from the pool
        ,CASE WHEN amount0 < '0' THEN f.token1 ELSE f.token0 END AS token_sold_address -- when amount0 is negative it means trader_a is buying token1 from the pool
        ,CAST(t.contract_address as string) as project_contract_address -- Uniswap v3 pool contract address
        ,t.evt_tx_hash AS tx_hash -- the transaction in which this event was emitted
        ,'' AS trace_address 
        ,t.evt_index -- the index of this event in the transaction
        FROM
        {{ source('uniswap_v3_arbitrum', 'Pair_evt_Swap') }} t -- all Uniswap V3 Swap events on Arbitrum
        INNER JOIN 
        uniswap_v3_arbitrum.UniswapV3Factory_evt_PoolCreated f ON f.pool = t.contract_address  -- Joining Uniswap V3 Factory PoolCreated event to get token0 and token1 addresses which are needed for further joins

)
SELECT
    'arbitrum' AS blockchain -- add the blockchain name to the results
    ,'uniswap' AS project -- add the project name to the results
    ,'3' AS version -- add the version of the project to the results
    ,TRY_CAST(date_trunc('DAY', dexs.block_time) AS date) AS block_date -- adding block_date to partition the data on
    ,dexs.block_time 
    ,erc20a.symbol AS token_bought_symbol -- adding the symbol of token0 to the results
    ,erc20b.symbol AS token_sold_symbol -- adding the symbol of token1 to the results
    ,case
        when lower(erc20a.symbol) > lower(erc20b.symbol) then concat(erc20b.symbol, '-', erc20a.symbol)
        else concat(erc20a.symbol, '-', erc20b.symbol)
        end as token_pair -- ordering the symbols of token0 and token1 alphabetically and concatenating them with a dash in order to create consistent token_pairs
    ,dexs.token_bought_amount_raw / power(10, erc20a.decimals) AS token_bought_amount -- calculating the display amount of token0 that was bought
    ,dexs.token_sold_amount_raw / power(10, erc20b.decimals) AS token_sold_amount -- calculating the display amount of token1 that was sold
    ,dexs.token_bought_amount_raw 
    ,dexs.token_sold_amount_raw
    ,coalesce(
        dexs.amount_usd
        ,(dexs.token_bought_amount_raw / power(10, p_bought.decimals)) * p_bought.price
        ,(dexs.token_sold_amount_raw / power(10, p_sold.decimals)) * p_sold.price
        ) AS amount_usd -- calculating the amount of USD that was bought or sold by using either the price of the token sold or the token bought from the prices.usd table
    ,dexs.token_bought_address -- the address of the token that was bought
    ,dexs.token_sold_address -- the address of the token that was sold
    ,coalesce(dexs.taker, tx.from) AS taker -- subqueries rely on this COALESCE to avoid redundant joins with the transactions table
    ,dexs.maker 
    ,dexs.project_contract_address -- the address of the Uniswap V3 pool contract
    ,dexs.tx_hash -- the transaction in which this event was emitted
    ,tx.from AS tx_from -- the address that initated the transaction in which this event was emitted
    ,tx.to AS tx_to -- the address that received the top level call in which this transaction
    ,dexs.trace_address 
    ,dexs.evt_index -- the index of this event in the transaction
    FROM dexs
    -- join the tokens_erc20 tables to get the symbol, decimals of the tokens that were bought and sold. No time element so can always do this
    LEFT JOIN {{ ref('tokens_erc20') }} erc20a          ON erc20a.contract_address = dexs.token_bought_address      AND erc20a.blockchain = 'arbitrum'
    LEFT JOIN {{ ref('tokens_erc20') }} erc20b          ON erc20b.contract_address = dexs.token_sold_address        AND erc20b.blockchain = 'arbitrum'

    {% if not is_incremental() %} -- if the run is not incremental, join all transactions and prices since the project start date
    INNER JOIN  {{ source('arbitrum', 'transactions') }} tx ON tx.hash = dexs.tx_hash AND tx.block_time >= '{{project_start_date}}'
    LEFT JOIN {{ source('prices', 'usd') }} p_bought ON p_bought.minute = date_trunc('minute', dexs.block_time)  AND p_bought.contract_address = dexs.token_bought_address   AND p_bought.blockchain = 'arbitrum' AND p_bought.minute >= '{{project_start_date}}'
    LEFT JOIN {{ source('prices', 'usd') }} p_sold   ON p_sold.minute   = date_trunc('minute', dexs.block_time)  AND p_sold.contract_address   = dexs.token_bought_address   AND p_sold.blockchain   = 'arbitrum' AND p_sold.minute   >= '{{project_start_date}}'
    {% endif %}
            
    {% if is_incremental() %} -- if the run is incremental, join only the transactions and prices of the last week that happened in the last week
    INNER JOIN  {{ source('arbitrum', 'transactions') }} tx ON tx.hash = dexs.tx_hash AND tx.block_time >= date_trunc("day", now() - interval '1 week')
    LEFT JOIN {{ source('prices', 'usd') }} p_bought ON p_bought.minute = date_trunc('minute', dexs.block_time)  AND p_bought.contract_address = dexs.token_bought_address   AND p_bought.blockchain = 'arbitrum' AND p_bought.minute >= date_trunc("day", now() - interval '1 week')
    LEFT JOIN {{ source('prices', 'usd') }} p_sold   ON p_sold.minute   = date_trunc('minute', dexs.block_time)  AND p_sold.contract_address   = dexs.token_bought_address   AND p_sold.blockchain   = 'arbitrum' AND p_sold.minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}
