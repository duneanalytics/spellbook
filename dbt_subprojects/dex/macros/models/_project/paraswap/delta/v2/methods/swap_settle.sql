{% macro delta_v2_swap_settle(blockchain) %}
-- since this call always is a whole-order-at-once fulfillment, can source it from method calls and no need to join with events as all data is in the call
-- {% set method_start_date = '2024-10-01' %}
{% set method_start_date = '2025-05-10' %}

-- order_hash_computed



-- ParaswapDeltav2_evt_OrderSettled
-- contract_address varbinary
-- evt_tx_hash varbinary
-- evt_tx_from varbinary
-- evt_tx_to varbinary
-- evt_index bigint
-- evt_block_time timestamp
-- evt_block_number bigint
-- owner varbinary
-- beneficiary varbinary
-- orderHash varbinary
-- srcToken varbinary
-- destToken varbinary
-- srcAmount uint256
-- destAmount uint256
-- returnAmount uint256   -- TODO: add this field to the model
-- protocolFee uint256    -- TODO: add this field to the model
-- partnerFee uint256     -- TODO: add this field to the model


v2_swap_settle_withParsedOrderData AS (
    SELECT
        call_trace_address,
        call_block_time, 
        call_block_number, 
        call_tx_hash,
        call_tx_from,
        call_tx_to,
        orderWithSig,
        executor,
        executorData,
        contract_address    
    FROM {{ source("paraswapdelta_"+ blockchain, "ParaswapDeltav2_call_swapSettle") }}        
        WHERE call_block_time > TIMESTAMP '{{method_start_date}}'
        AND call_success = true        
        {% if is_incremental() %}
            AND {{ incremental_predicate('call_block_time') }}
        {% endif %}
),
v2_swap_settle_parsedOrderWithSig AS (
    SELECT         
        * 
    FROM 
        v2_swap_settle_withParsedOrderData
),
v2_swap_settle_unparsedOrders AS (
  SELECT
    JSON_EXTRACT_SCALAR(JSON_PARSE(TRY_CAST(orderWithSig AS VARCHAR)), '$.order') AS "order",
    JSON_EXTRACT_SCALAR(JSON_PARSE(TRY_CAST(orderWithSig AS VARCHAR)), '$.signature') AS signature,
    *
  FROM v2_swap_settle_parsedOrderWithSig
),
v2_swap_settle_parsedOrders AS (
  SELECT
    JSON_EXTRACT_SCALAR(JSON_PARSE(TRY_CAST("order" AS VARCHAR)), '$.bridge') AS "bridge",
    from_hex(JSON_EXTRACT_SCALAR("order", '$.owner')) as owner,
    from_hex(JSON_EXTRACT_SCALAR("order", '$.beneficiary')) as beneficiary,
    from_hex(JSON_EXTRACT_SCALAR("order", '$.srcToken')) as srcToken,
    from_hex(JSON_EXTRACT_SCALAR("order", '$.destToken')) as destToken,
    cast(JSON_EXTRACT_SCALAR("order", '$.srcAmount') as uint256) as srcAmount,
    cast(JSON_EXTRACT_SCALAR("order", '$.destAmount') as uint256) as destAmount,
    cast(JSON_EXTRACT_SCALAR("order", '$.expectedDestAmount') as uint256) as expectedDestAmount,
    cast(JSON_EXTRACT_SCALAR("order", '$.deadline') as uint256) as deadline,
    cast(JSON_EXTRACT_SCALAR("order", '$.nonce') as uint256) as nonce,
    cast(JSON_EXTRACT_SCALAR("order", '$.partnerAndFee') as uint256) as partnerAndFee,
    from_hex(JSON_EXTRACT_SCALAR("order", '$.permit')) as permit,
    {{executor_fee_amount()}},    
    * 
  FROM v2_swap_settle_unparsedOrders
),
v2_swap_settle_with_wrapped_native AS (
  SELECT
  from_hex(JSON_EXTRACT_SCALAR("bridge", '$.multiCallHandler')) as bridgeMultiCallHandler,
  from_hex(JSON_EXTRACT_SCALAR("bridge", '$.outputToken')) as bridgeOutputToken,
  cast(JSON_EXTRACT_SCALAR("bridge", '$.maxRelayerFee') as uint256) as bridgeMaxRelayerFee,
  cast(JSON_EXTRACT_SCALAR("bridge", '$.destinationChainId') as uint256) as bridgeDestinationChainId,
{{to_wrapped_native_token(blockchain, 'destToken', 'dest_token_for_joining')}},
{{to_wrapped_native_token(blockchain, 'srcToken', 'src_token_for_joining')}},
    *
  FROM v2_swap_settle_parsedOrders
), v2_swap_settle_with_wrapped_native_with_orderhash as (
  select 
    *,
    CASE WHEN bridge IS NULL THEN
      {{ compute_order_hash(blockchain) }}
    ELSE
      {{ compute_order_hash_with_bridge(blockchain) }}
    END as computed_order_hash     
  from v2_swap_settle_with_wrapped_native
),
delta_v2_swapSettle_master as (
select 
    'swapSettle' as method,
    COALESCE(CAST(s.price AS DECIMAL(38,18)), 0) AS src_token_price_usd,
    COALESCE(CAST(d.price AS DECIMAL(38,18)), 0) AS dest_token_price_usd, 
    {{ gas_fee_usd() }},
    s.price *  w.srcAmount / POWER(10, s.decimals)  AS src_token_order_usd,
    d.price *  w.destAmount / POWER(10, d.decimals)  AS dest_token_order_usd,
    w.destToken AS fee_token,
    w.*,
    events.evt_index,    
    events.returnAmount as evt_return_amount,
    events.protocolFee as evt_protocol_fee,
    events.partnerFee as evt_partner_fee,
    events.orderHash as evt_order_hash    
    FROM v2_swap_settle_with_wrapped_native_with_orderhash w 
    LEFT JOIN {{ source('prices', 'usd') }} d
    ON d.minute > TIMESTAMP '{{method_start_date}}'
    AND d.blockchain = '{{blockchain}}'
    {% if is_incremental() %}
      AND {{ incremental_predicate('d.minute') }}
    {% endif %}
    AND d.contract_address = w.dest_token_for_joining
    AND d.minute = DATE_TRUNC('minute', w.call_block_time)
    LEFT JOIN {{ source('prices', 'usd') }} s
    ON s.minute > TIMESTAMP '{{method_start_date}}'
    AND s.blockchain = '{{blockchain}}'
    {% if is_incremental() %}
      AND {{ incremental_predicate('s.minute') }}
    {% endif %}
    AND s.contract_address = w.src_token_for_joining
    AND s.minute = DATE_TRUNC('minute', w.call_block_time)
    
    LEFT JOIN {{ source("paraswapdelta_"+ blockchain, "ParaswapDeltav2_evt_OrderSettled") }} events 
      ON
        evt_block_time = call_block_time  
      -- suffices for fill-all-at-once methods (unlike partials, because with partials there's an edge case when you can mismatch still, although very unlikely to happen in real life)
      AND computed_order_hash = events.orderHash    
      AND call_tx_hash = events.evt_tx_hash
), delta_v2_swapSettle as (  
SELECT 
    -- NB: columns mapping must match accross all the methods, since they're uninoned into one in master macro
    '{{blockchain}}' as blockchain,
    method,
    0 as order_index,
    call_trace_address,
    call_block_number,
    call_block_time,    
    call_tx_hash,
    call_tx_from,
    call_tx_to,
    evt_index,
    executorFeeAmount as fee_amount,
    -- orderWithSig as order_with_sig,
    executor,
    executorData as calldata_to_execute,
    -- "order",
    signature,
    owner,
    srcToken as src_token,
    destToken as dest_token,
    srcAmount as src_amount,
    destAmount as dest_amount,
    src_token_for_joining,
    dest_token_for_joining,
    fee_token,
    src_token_price_usd,
    dest_token_price_usd,
    gas_fee_usd,    
    src_token_order_usd,
    dest_token_order_usd,
    contract_address,
    partnerAndFee,
    computed_order_hash,
    evt_order_hash,
    evt_return_amount,
    evt_protocol_fee,
    evt_partner_fee,
    bridgeMultiCallHandler,
    bridgeOutputToken,
    bridgeMaxRelayerFee,
    bridgeDestinationChainId,
    bridge,
    "order"
    
  FROM delta_v2_swapSettle_master
)
{% endmacro %}