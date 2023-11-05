{% macro generic_uniswap_v2_fork(blockchain, transactions, logs, contracts) %}


with decoding_raw_forks as 
(
    Select 
    contract_address
    ,tx_hash as evt_tx_hash
    ,index as evt_index
    ,block_time as evt_block_time
    ,block_number as evt_block_number
    ,varbinary_to_uint256(varbinary_substring(data, 1, 32)) as amount0In  
    ,varbinary_to_uint256(varbinary_substring(data, 33, 32)) as amount1In
    ,varbinary_to_uint256(varbinary_substring(data, 66, 32)) as amount0Out
    ,varbinary_to_uint256(varbinary_substring(data, 99, 32)) as amount1Out
    ,varbinary_substring(topic1, 13, 20) as sender
    ,varbinary_substring(topic2, 13, 20) as to
from  {{logs}}
    where topic0 = 0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822 --topic0 for uniswap_v2 swap event
)

,dexs AS
(
-- Uniswap v2
SELECT
    t.evt_block_time AS block_time
    ,t.to AS taker
    ,CAST(NULL as VARBINARY) as maker
    ,CASE WHEN amount0Out = UINT256 '0' THEN amount1Out ELSE amount0Out END AS token_bought_amount_raw
    ,CASE WHEN amount0In = UINT256 '0' OR amount1Out = UINT256 '0' THEN amount1In ELSE amount0In END AS token_sold_amount_raw
    ,NULL AS amount_usd
    ,CASE WHEN amount0Out = UINT256 '0' THEN f.token1 ELSE f.token0 END AS token_bought_address
    ,CASE WHEN amount0In = UINT256 '0' OR amount1Out = UINT256 '0' THEN f.token1 ELSE f.token0 END AS token_sold_address
    ,t.contract_address as project_contract_address
    ,t.evt_tx_hash AS tx_hash
    ,t.evt_index
    ,f.contract_address as deployed_by_contract_address
FROM decoding_raw_forks t
INNER JOIN (Select 
             contract_address
             ,tx_hash
             ,index
             ,block_time
             ,block_number
             ,VARBINARY_SUBSTRING(data, 13,20) as pair
             ,VARBINARY_SUBSTRING(topic1, 13, 20) AS token0
             ,VARBINARY_SUBSTRING(topic2, 13, 20) AS token1 
         from  {{logs}}
         where topic0 = 0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9 --topic0 for uniswap_v2 factory event Pair_created
         ) f 
 ON f.pair = t.contract_address
WHERE t.contract_address NOT IN (SELECT address FROM {{contracts}}) --excluding already decoded contracts to avoid duplicates in dex.trades
)
SELECT
    '{{blockchain}}' AS blockchain
    ,coalesce(fac.project,'unknown_uniswap_v2_fork') AS project
    ,'2' AS version
    ,CAST(date_trunc('month', dexs.block_time) AS date) AS block_month
    ,CAST(date_trunc('DAY', dexs.block_time) AS date) AS block_date
    ,dexs.block_time
    ,erc20a.symbol AS token_bought_symbol
    ,erc20b.symbol AS token_sold_symbol
    ,case
        when lower(erc20a.symbol) > lower(erc20b.symbol) then concat(erc20b.symbol, '-', erc20a.symbol)
        else concat(erc20a.symbol, '-', erc20b.symbol)
    end as token_pair
    ,dexs.token_bought_amount_raw / power(10, erc20a.decimals) AS token_bought_amount
    ,dexs.token_sold_amount_raw / power(10, erc20b.decimals) AS token_sold_amount
    ,dexs.token_bought_amount_raw  AS token_bought_amount_raw
    ,dexs.token_sold_amount_raw AS token_sold_amount_raw
    ,coalesce(
        dexs.amount_usd
        ,(dexs.token_bought_amount_raw / power(10, p_bought.decimals)) * p_bought.price
        ,(dexs.token_sold_amount_raw / power(10, p_sold.decimals)) * p_sold.price
    ) AS amount_usd
    ,dexs.token_bought_address
    ,dexs.token_sold_address
    ,coalesce(dexs.taker, tx."from") AS taker -- subqueries rely on this COALESCE to avoid redundant joins with the transactions table
    ,dexs.maker
    ,dexs.project_contract_address
    ,dexs.tx_hash
    ,tx."from" AS tx_from
    ,tx.to AS tx_to
    ,dexs.evt_index
    ,deployed_by_contract_address
FROM dexs
INNER JOIN {{transactions}} tx
    ON tx.hash = dexs.tx_hash 
    AND tx.block_time >= TIMESTAMP '2020-05-05'
LEFT JOIN tokens.erc20 erc20a
    ON erc20a.contract_address = dexs.token_bought_address
    AND erc20a.blockchain = '{{blockchain}}'
LEFT JOIN tokens.erc20 erc20b
    ON erc20b.contract_address = dexs.token_sold_address
    AND erc20b.blockchain = '{{blockchain}}'
LEFT JOIN delta_prod.prices.usd p_bought
    ON p_bought.minute = date_trunc('minute', dexs.block_time)
    AND p_bought.contract_address = dexs.token_bought_address
    AND p_bought.blockchain = '{{blockchain}}'
    AND p_bought.minute >= TIMESTAMP '2020-05-05'
LEFT JOIN delta_prod.prices.usd p_sold
    ON p_sold.minute = date_trunc('minute', dexs.block_time)
    AND p_sold.contract_address = dexs.token_sold_address
    AND p_sold.blockchain = '{{blockchain}}'
    AND p_sold.minute >= TIMESTAMP '2020-05-05'
LEFT JOIN {{ref('dex_uniswap_v2_fork_mapping') }} fac
    ON dexs.project_contract_address = fac.factory_address

{% endmacro %}