-- ref dbt_subprojects/dex/macros/models/_project/paraswap/v6/paraswap_v6_trades_master.sql
{% macro delta_safe_settle_batch_swap(blockchain) %}
safe_settle_batch_swap_ExpandedOrders AS (
  SELECT    
    call_trace_address,
    call_block_time,
    call_block_number,
    call_tx_hash,
    output_successfulOrders,
    JSON_EXTRACT(data, '$.ordersData') AS parsed_orders,
    contract_address
  FROM {{ source("paraswapdelta_"+blockchain, "ParaswapDeltav1_call_safeSettleBatchSwap") }}
   where call_success = true
   {% if is_incremental() %}
      AND {{ incremental_predicate('call_block_time') }}
   {% endif %}
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
    JSON_EXTRACT_SCALAR(JSON_PARSE(TRY_CAST(orderWithSig AS VARCHAR)), '$.signature') AS signature,
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
LEFT JOIN {{ source('prices', 'usd') }} d
  ON d.blockchain = '{{blockchain}}'
  AND d.minute > TIMESTAMP '2024-06-01'
  {% if is_incremental() %}
      AND {{ incremental_predicate('d.minute') }}
   {% endif %}
  AND d.contract_address = w.dest_token_for_joining
  AND d.minute = DATE_TRUNC('minute', w.call_block_time)
LEFT JOIN {{ source('prices', 'usd') }} s
  ON s.blockchain = '{{blockchain}}'
  AND s.minute > TIMESTAMP '2024-06-01'
  {% if is_incremental() %}
      AND {{ incremental_predicate('s.minute') }}
   {% endif %}
  AND s.contract_address = w.src_token_for_joining
  AND s.minute = DATE_TRUNC('minute', w.call_block_time)
ORDER BY
  CARDINALITY(w.output_successfulOrders)
)
{% endmacro %}