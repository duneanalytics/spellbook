-- ref dbt_subprojects/dex/models/_projects/paraswap/ethereum/paraswap_v6_ethereum_trades.sql
-- ref dbt_subprojects/dex/models/_projects/paraswap/ethereum/paraswap_v6_ethereum_trades_decoded.sql




with

settle_swap_withParsedOrderData AS (
    SELECT         
        call_trace_address,
        call_block_time, 
        call_block_number, 
        call_tx_hash, 
        JSON_EXTRACT(data, '$.orderData') AS parsed_order_data,
        contract_address
    FROM 
        paraswapdelta_ethereum.ParaswapDeltav1_call_settleSwap
        where call_success = true
        
),
settle_swap_parsedOrderWithSig AS (
    SELECT 
        JSON_EXTRACT_SCALAR(JSON_PARSE(TRY_CAST(parsed_order_data AS VARCHAR)), '$.feeAmount') AS feeAmount,
        JSON_EXTRACT(JSON_PARSE(TRY_CAST(parsed_order_data AS VARCHAR)), '$.orderWithSig') AS orderWithSig,
        JSON_EXTRACT(JSON_PARSE(TRY_CAST(parsed_order_data AS VARCHAR)), '$.calldataToExecute') AS calldataToExecute,
        * 
    FROM 
        settle_swap_withParsedOrderData
),
settle_swap_unparsedOrders AS (
  SELECT
    JSON_EXTRACT(JSON_PARSE(TRY_CAST(orderWithSig AS VARCHAR)), '$.order') AS "order",
    JSON_EXTRACT(JSON_PARSE(TRY_CAST(orderWithSig AS VARCHAR)), '$.signature') AS signature,
    *
  FROM settle_swap_parsedOrderWithSig
),
settle_swap_parsedOrders AS (
  SELECT
    from_hex(JSON_EXTRACT_SCALAR(JSON_PARSE(TRY_CAST("order" AS VARCHAR)), '$.owner')) AS "order_owner",
    FROM_HEX(JSON_EXTRACT_SCALAR(JSON_PARSE(TRY_CAST("order" AS VARCHAR)), '$.srcToken')) AS "src_token",
    FROM_HEX(JSON_EXTRACT_SCALAR(JSON_PARSE(TRY_CAST("order" AS VARCHAR)), '$.destToken')) AS "dest_token",
    JSON_EXTRACT_SCALAR(JSON_PARSE(TRY_CAST("order" AS VARCHAR)), '$.srcAmount')  AS "src_amount",
    JSON_EXTRACT_SCALAR(JSON_PARSE(TRY_CAST("order" AS VARCHAR)), '$.destAmount')  AS "dest_amount",
    JSON_EXTRACT_SCALAR(JSON_PARSE(TRY_CAST("order" AS VARCHAR)), '$.permit')  AS "permit",
    *
  FROM settle_swap_unparsedOrders
),
settle_swap_withUSDs AS (
  SELECT
    CASE 
        WHEN dest_token = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee THEN 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 
        ELSE dest_token 
    END AS dest_token_for_joining,
    CASE 
        WHEN src_token = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee THEN 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 
        ELSE src_token 
    END AS src_token_for_joining,
    *
  FROM settle_swap_parsedOrders
), delta_v1_settle_swap_model as (
SELECT 
    w.*, 
    w.dest_token AS fee_token,
    CAST(s.price AS DECIMAL(38,18)) AS src_token_price_usd,
    CAST(d.price AS DECIMAL(38,18)) AS dest_token_price_usd, 
    COALESCE( 
        d.price *  CAST (w.feeAmount AS uint256) / POWER(10, d.decimals),
        -- src cost 
        
        (s.price *  CAST (w.src_amount AS uint256) / POWER(10, s.decimals))
        * CAST (w.feeAmount AS uint256) / CAST (w.dest_amount AS uint256),
        0
        
    )  AS gas_fee_usd,
    s.price *  CAST (w.src_amount AS uint256) / POWER(10, s.decimals)  AS src_token_order_usd,
    d.price *  CAST (w.dest_amount AS uint256) / POWER(10, d.decimals)  AS dest_token_order_usd
    
FROM settle_swap_withUSDs w 
LEFT JOIN prices.usd d
  ON d.blockchain = 'ethereum'
  AND d.minute > TIMESTAMP '2024-06-01'
  
  AND d.contract_address = w.dest_token_for_joining
  AND d.minute = DATE_TRUNC('minute', w.call_block_time)
LEFT JOIN prices.usd s
  ON s.blockchain = 'ethereum'
  AND s.minute > TIMESTAMP '2024-06-01'
  
  AND s.contract_address = w.src_token_for_joining
  AND s.minute = DATE_TRUNC('minute', w.call_block_time)
)

,
safe_settle_batch_swap_ExpandedOrders AS (
  SELECT    
    call_trace_address,
    call_block_time,
    call_block_number,
    call_tx_hash,
    output_successfulOrders,
    JSON_EXTRACT(data, '$.ordersData') AS parsed_orders,
    contract_address
  FROM paraswapdelta_ethereum.ParaswapDeltav1_call_safeSettleBatchSwap
   where call_success = true
   
), safe_settle_batch_swap_parsedOrderItems AS (
  SELECT
    index,
    JSON_ARRAY_GET(parsed_orders, index) AS parsed_order_data,
    *
  FROM safe_settle_batch_swap_ExpandedOrders
  CROSS JOIN UNNEST(SEQUENCE(0, CARDINALITY(output_successfulOrders) - 1)) AS t(index)
  WHERE
    output_successfulOrders[index + 1]
), safe_settle_batch_swap_parsedOrdersWithSig  AS (
  SELECT
    JSON_EXTRACT_SCALAR(parsed_order_data, '$.feeAmount') AS feeAmount,
    JSON_EXTRACT(parsed_order_data, '$.orderWithSig') AS orderWithSig,
    JSON_EXTRACT(parsed_order_data, '$.calldataToExecute') AS calldataToExecute,
    *
  FROM safe_settle_batch_swap_parsedOrderItems
), safe_settle_batch_swap_unparsedOrders AS (
  SELECT
    JSON_EXTRACT(JSON_PARSE(TRY_CAST(orderWithSig AS VARCHAR)), '$.order') AS "order",
    JSON_EXTRACT(JSON_PARSE(TRY_CAST(orderWithSig AS VARCHAR)), '$.signature') AS signature,
    *
  FROM safe_settle_batch_swap_parsedOrdersWithSig 
), safe_settle_batch_swap_parsedOrders AS (
  SELECT
    from_hex(JSON_EXTRACT_SCALAR(JSON_PARSE(TRY_CAST("order" AS VARCHAR)), '$.owner')) AS "order_owner",
    FROM_HEX(JSON_EXTRACT_SCALAR(JSON_PARSE(TRY_CAST("order" AS VARCHAR)), '$.srcToken')) AS "src_token",
    FROM_HEX(JSON_EXTRACT_SCALAR(JSON_PARSE(TRY_CAST("order" AS VARCHAR)), '$.destToken')) AS "dest_token",
    JSON_EXTRACT_SCALAR(JSON_PARSE(TRY_CAST("order" AS VARCHAR)), '$.srcAmount')  AS "src_amount",
    JSON_EXTRACT_SCALAR(JSON_PARSE(TRY_CAST("order" AS VARCHAR)), '$.destAmount')  AS "dest_amount",
    *
  FROM safe_settle_batch_swap_unparsedOrders
), safe_settle_batch_swap_withUSDs AS (
  SELECT
    CASE 
        WHEN dest_token = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee THEN 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 
        ELSE dest_token 
    END AS dest_token_for_joining,
    CASE 
        WHEN src_token = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee THEN 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 
        ELSE src_token 
    END AS src_token_for_joining,
    *
  FROM safe_settle_batch_swap_parsedOrders
), delta_v1_safe_settle_batch_swap_model as (
SELECT 
    w.*, 
    w.dest_token AS fee_token,
    CAST(s.price AS DECIMAL(38,18)) AS src_token_price_usd,
    CAST(d.price AS DECIMAL(38,18)) AS dest_token_price_usd, 
    COALESCE( 
        d.price *  CAST (w.feeAmount AS uint256) / POWER(10, d.decimals),
        -- src cost 
        
        (s.price *  CAST (w.src_amount AS uint256) / POWER(10, s.decimals))
        * CAST (w.feeAmount AS uint256) / CAST (w.dest_amount AS uint256)
        
    )  AS gas_fee_usd,
    s.price *  CAST (w.src_amount AS uint256) / POWER(10, s.decimals)  AS src_token_order_usd,
    d.price *  CAST (w.dest_amount AS uint256) / POWER(10, d.decimals)  AS dest_token_order_usd
    
FROM safe_settle_batch_swap_withUSDs w 
LEFT JOIN prices.usd d
  ON d.blockchain = 'ethereum'
  AND d.minute > TIMESTAMP '2024-06-01'
  
  AND d.contract_address = w.dest_token_for_joining
  AND d.minute = DATE_TRUNC('minute', w.call_block_time)
LEFT JOIN prices.usd s
  ON s.blockchain = 'ethereum'
  AND s.minute > TIMESTAMP '2024-06-01'
  
  AND s.contract_address = w.src_token_for_joining
  AND s.minute = DATE_TRUNC('minute', w.call_block_time)
ORDER BY
  CARDINALITY(w.output_successfulOrders)
)

select 
    'delta_v1_settle_swap_model' as method,
    call_trace_address,
    call_block_number,
    call_block_time,
    call_tx_hash,
    -- parsed_order_data,
    feeAmount as fee_amount,
    orderWithSig as order_with_sig,
    calldataToExecute as calldata_to_execute,
    "order",
    signature,
    order_owner,
    src_token,
    dest_token,
    src_amount,
    dest_amount,
    src_token_for_joining,
    dest_token_for_joining,
    fee_token,
    src_token_price_usd,
    dest_token_price_usd,
    gas_fee_usd,
    src_token_order_usd,
    dest_token_order_usd,
    contract_address
 from delta_v1_settle_swap_model
union all
select 
    'delta_v1_safe_settle_batch_swap_model' as method,
    call_trace_address,
    call_block_number,
    call_block_time,
    call_tx_hash,
    -- parsed_order_data,
    feeAmount as fee_amount,
    orderWithSig as order_with_sig,
    calldataToExecute as calldata_to_execute,
    "order",
    signature,
    order_owner,
    src_token,
    dest_token,
    src_amount,
    dest_amount,
    src_token_for_joining,
    dest_token_for_joining,
    fee_token,
    src_token_price_usd,
    dest_token_price_usd,
    gas_fee_usd,
    src_token_order_usd,
    dest_token_order_usd,
    contract_address
from delta_v1_safe_settle_batch_swap_model

-- with dexs AS (
--         SELECT
--             blockTime AS block_time,
--             blockNumber AS block_number,
--             from_hex(beneficiary) AS taker,
--             null AS maker,  -- TODO: can parse from traces
--             receivedAmount AS token_bought_amount_raw,
--             fromAmount AS token_sold_amount_raw,
--             CAST(NULL AS double) AS amount_usd,
--             method,
--             CASE
--                 WHEN from_hex(destToken) = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee
--                 THEN 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 -- WETH
--                 ELSE from_hex(destToken)
--             END AS token_bought_address,
--             CASE
--                 WHEN from_hex(srcToken) = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee
--                 THEN 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 -- WETH
--                 ELSE from_hex(srcToken)
--             END AS token_sold_address,
--             projectContractAddress as project_contract_address,
--             txHash AS tx_hash,
--             callTraceAddress AS trace_address,
--             CAST(-1 as integer) AS evt_index
--         FROM paraswap_v6_ethereum.trades_decoded
--         
-- )
-- SELECT 'ethereum' AS blockchain,
--     'paraswap' AS project,
--     '6' AS version,
--     cast(date_trunc('day', d.block_time) as date) as block_date,
--     cast(date_trunc('month', d.block_time) as date) as block_month,
--     d.block_time,
-- method,
--     e1.symbol AS token_bought_symbol,
--     e2.symbol AS token_sold_symbol,
--     CASE
--         WHEN lower(e1.symbol) > lower(e2.symbol) THEN concat(e2.symbol, '-', e1.symbol)
--         ELSE concat(e1.symbol, '-', e2.symbol)
--     END AS token_pair,
--     d.token_bought_amount_raw / power(10, e1.decimals) AS token_bought_amount,
--     d.token_sold_amount_raw / power(10, e2.decimals) AS token_sold_amount,
--     d.token_bought_amount_raw,
--     d.token_sold_amount_raw,
--     coalesce(
--         d.amount_usd
--         ,(d.token_bought_amount_raw / power(10, p1.decimals)) * p1.price
--         ,(d.token_sold_amount_raw / power(10, p2.decimals)) * p2.price
--     ) AS amount_usd,
--     d.token_bought_address,
--     d.token_sold_address,
--     coalesce(d.taker, tx."from") AS taker,
--     coalesce(d.maker, tx."from") as maker,
--     d.project_contract_address,
--     d.tx_hash,
--     tx."from" AS tx_from,
--     tx.to AS tx_to,
--     d.trace_address,
--     d.evt_index
-- FROM dexs d
-- INNER JOIN delta_prod.ethereum.transactions tx ON d.tx_hash = tx.hash
--     AND d.block_number = tx.block_number
--     
--     AND tx.block_time >= TIMESTAMP '2024-05-01'
--     
--     
-- LEFT JOIN delta_prod.tokens.erc20 e1 ON e1.contract_address = d.token_bought_address
--     AND e1.blockchain = 'ethereum'
-- LEFT JOIN delta_prod.tokens.erc20 e2 ON e2.contract_address = d.token_sold_address
--     AND e2.blockchain = 'ethereum'
-- LEFT JOIN delta_prod.prices.usd p1 ON p1.minute = date_trunc('minute', d.block_time)
--     AND p1.contract_address = d.token_bought_address
--     AND p1.blockchain = 'ethereum'
--     
--     AND p1.minute >= TIMESTAMP '2024-05-01'
--     
--     
-- LEFT JOIN delta_prod.prices.usd p2 ON p2.minute = date_trunc('minute', d.block_time)
--     AND p2.contract_address = d.token_sold_address
--     AND p2.blockchain = 'ethereum'
--     
--     AND p2.minute >= TIMESTAMP '2024-05-01'
--     
--     