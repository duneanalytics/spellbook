{% macro delta_settle_swap(blockchain) %}
settle_swap_withParsedOrderData AS (
    SELECT         
        call_trace_address,
        call_block_time, 
        call_block_number, 
        call_tx_hash, 
        call_tx_from, -- varbinary
        call_tx_to, -- varbinary
        JSON_EXTRACT(data, '$.orderData') AS parsed_order_data,
        contract_address    
    FROM {{ source("paraswapdelta_"+ blockchain, "ParaswapDeltav1_call_settleSwap") }}        
        where call_success = true
        {% if is_incremental() %}
            AND {{ incremental_predicate('call_block_time') }}
        {% endif %}
),
settle_swap_parsedOrderWithSig AS (
    SELECT 
        JSON_EXTRACT_SCALAR(JSON_PARSE(TRY_CAST(parsed_order_data AS VARCHAR)), '$.feeAmount') AS feeAmount,
        JSON_EXTRACT(JSON_PARSE(TRY_CAST(parsed_order_data AS VARCHAR)), '$.orderWithSig') AS orderWithSig,
        JSON_EXTRACT_SCALAR(JSON_PARSE(TRY_CAST(parsed_order_data AS VARCHAR)), '$.calldataToExecute') AS calldataToExecute,
        * 
    FROM 
        settle_swap_withParsedOrderData
),
settle_swap_unparsedOrders AS (
  SELECT
    JSON_EXTRACT(JSON_PARSE(TRY_CAST(orderWithSig AS VARCHAR)), '$.order') AS "order",
    JSON_EXTRACT_SCALAR(JSON_PARSE(TRY_CAST(orderWithSig AS VARCHAR)), '$.signature') AS signature,
    *
  FROM settle_swap_parsedOrderWithSig
),
settle_swap_parsedOrders AS (
  SELECT
    from_hex(JSON_EXTRACT_SCALAR(JSON_PARSE(TRY_CAST("order" AS VARCHAR)), '$.owner')) AS "owner",
    FROM_HEX(JSON_EXTRACT_SCALAR(JSON_PARSE(TRY_CAST("order" AS VARCHAR)), '$.srcToken')) AS "src_token",
    FROM_HEX(JSON_EXTRACT_SCALAR(JSON_PARSE(TRY_CAST("order" AS VARCHAR)), '$.destToken')) AS "dest_token",
    cast(JSON_EXTRACT_SCALAR(JSON_PARSE(TRY_CAST("order" AS VARCHAR)), '$.srcAmount') as uint256) AS "src_amount",
    cast(JSON_EXTRACT_SCALAR(JSON_PARSE(TRY_CAST("order" AS VARCHAR)), '$.destAmount') as uint256) AS "dest_amount",
    JSON_EXTRACT_SCALAR(JSON_PARSE(TRY_CAST("order" AS VARCHAR)), '$.permit')  AS "permit",
    *
  FROM settle_swap_unparsedOrders
),
settle_swap_with_wrapped_native AS (
  SELECT
  {{to_wrapped_native_token(blockchain, 'dest_token', 'dest_token_for_joining')}},
  {{to_wrapped_native_token(blockchain, 'src_token', 'src_token_for_joining')}},
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
    
FROM settle_swap_with_wrapped_native w 
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
), delta_v1_settleSwap as (  
select 
    '{{blockchain}}' as blockchain,
    'delta_v1_settle_swap_model' as method,
    0 as order_index,
    call_trace_address,
    call_block_number,
    call_block_time,
    call_tx_hash,
    call_tx_from, -- varbinary
    call_tx_to, -- varbinary
    cast(NULL as bigint) as evt_index, -- no events in delta v1
    -- parsed_order_data,
    feeAmount as fee_amount,
    -- orderWithSig as order_with_sig,
    calldataToExecute as calldata_to_execute,
    -- "order",
    signature,
    owner,
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
)
{% endmacro %}