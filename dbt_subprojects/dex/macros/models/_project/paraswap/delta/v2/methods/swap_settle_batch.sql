{% macro delta_v2_swap_settle_batch(blockchain) %}

{% set method_start_date = '2025-04-01' %}

delta_v2_swap_settle_batch_ExpandedOrders as (
    select            
      order_index,
      contract_address, -- varbinary
      -- call_success, -- boolean
      call_tx_hash, -- varbinary
      call_tx_from, -- varbinary
      call_tx_to, -- varbinary
      call_trace_address, -- array(bigint)
      call_block_time, -- timestamp
      call_block_number, -- bigint
      -- ordersWithSigs[order_index] as extractedOrderWithSig,              
      JSON_EXTRACT_SCALAR(ordersWithSigs[order_index], '$.order') as "order", -- returns json
      JSON_EXTRACT_SCALAR(ordersWithSigs[order_index], '$.signature') as signature,                            
      1 as ordersCount, -- TODO
      executor,
      executorData[order_index] as executorData,
      raw_txs.gas_used as raw_tx_gas_used,
      raw_txs.gas_price as raw_tx_gas_price
  FROM {{ source("paraswapdelta_"+ blockchain, "ParaswapDeltav2_call_swapSettleBatch") }} ssb                   
  left join {{source(blockchain, "transactions")}} raw_txs 
     on block_time > TIMESTAMP '{{method_start_date}}' 
      AND raw_txs.hash = ssb.call_tx_hash
      CROSS JOIN UNNEST (
          -- SQL array indices start at 1
          -- also NB: if one order fails -- whole batch fails
          -- also NB: edge case: a multi-tx call, where some other method emits event with similar signature --> not valid as it won't end up in the table
          SEQUENCE(1, CARDINALITY(ordersWithSigs) )
      ) AS t (order_index)
  WHERE call_block_time > TIMESTAMP '{{method_start_date}}'
    AND call_success = true        
    {% if is_incremental() %}
      AND {{ incremental_predicate('call_block_time') }}
    {% endif %}        
     
),
delta_v2_swap_settle_batch_parsed_orders as (
  select
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
from
    delta_v2_swap_settle_batch_ExpandedOrders    
), 
delta_v2_swap_settle_batch_parsed_orders_with_orderhash as (
  select 
    *,
    from_hex(JSON_EXTRACT_SCALAR("bridge", '$.multiCallHandler')) as bridgeMultiCallHandler,
    from_hex(JSON_EXTRACT_SCALAR("bridge", '$.outputToken')) as bridgeOutputToken,
    cast(JSON_EXTRACT_SCALAR("bridge", '$.maxRelayerFee') as uint256) as bridgeMaxRelayerFee,
    cast(JSON_EXTRACT_SCALAR("bridge", '$.destinationChainId') as uint256) as bridgeDestinationChainId    
  from delta_v2_swap_settle_batch_parsed_orders
),
delta_v2_swap_settle_batch_withWrapped as (
  SELECT     
    CASE WHEN bridge IS NULL THEN
      {{ compute_order_hash(blockchain) }}
    ELSE
      {{ compute_order_hash_with_bridge(blockchain) }}
    END as computed_order_hash,
    {{to_wrapped_native_token(blockchain, 'orders.destToken', 'dest_token_for_joining')}},
    {{to_wrapped_native_token(blockchain, 'orders.srcToken', 'src_token_for_joining')}},
    orders.*
  FROM delta_v2_swap_settle_batch_parsed_orders_with_orderhash orders
),
delta_v2_swap_settle_batch_withEvents as (
  select   
    events.evt_index,
    events.orderHash as evt_order_hash,
    events.returnAmount as evt_return_amount,
    events.protocolFee as evt_protocol_fee,
    events.partnerFee as evt_partner_fee,
    withWrapped.*     
   from delta_v2_swap_settle_batch_withWrapped withWrapped
     --- NB: sourcing from calls and joining events, not the opposite, because some methods emit different events (*fill* -> OrderSettled / OrderPartiallyFilled). Sorting them by evt_index would make them match orders sorted by their index in array + call_trace_address -> source of truth bettr be calls
  LEFT JOIN {{ source("paraswapdelta_"+ blockchain, "ParaswapDeltav2_evt_OrderSettled") }} events 
    ON
      evt_block_time = call_block_time  
    -- suffices for fill-all-at-once methods (unlike partials, because with partials there's an edge case when you can mismatch still, although very unlikely to happen in real life)
    AND computed_order_hash = events.orderHash    
    AND call_tx_hash = events.evt_tx_hash
  
),
 delta_v2_swapSettleBatch_master as (

select 
    'swapSettleBatch' as method,
    --
    COALESCE(CAST(s.price AS DECIMAL(38,18)), 0) AS src_token_price_usd,
    COALESCE(CAST(d.price AS DECIMAL(38,18)), 0) AS dest_token_price_usd,         
    s.price *  w.srcAmount / POWER(10, s.decimals)  AS src_token_order_usd,
    d.price *  w.destAmount / POWER(10, d.decimals)  AS dest_token_order_usd,
    w.destToken AS fee_token,
    wrapped_native_token_address,
    COALESCE(CAST(wnt_usd.price AS DECIMAL(38,18)), 0) AS wnt_price_usd, 
    {{ gas_fee_usd() }},    
    w.*
from delta_v2_swap_settle_batch_withEvents w

 LEFT JOIN {{ source('prices', 'usd') }} d
    ON d.minute > TIMESTAMP '{{method_start_date}}'
    AND d.blockchain = '{{blockchain}}'
    {% if is_incremental() %}
      AND {{ incremental_predicate('d.minute') }}
    {% endif %}
    AND d.contract_address = w.dest_token_for_joining
    AND d.minute = DATE_TRUNC('minute', w.call_block_time)
    left join {{ source('evms','info') }} evm_info on evm_info.blockchain='{{blockchain}}'
    LEFT JOIN {{ source('prices', 'usd') }} wnt_usd
    ON wnt_usd.minute > TIMESTAMP '{{method_start_date}}'
    AND wnt_usd.blockchain = '{{blockchain}}'
    {% if is_incremental() %}
      AND {{ incremental_predicate('wnt_usd.minute') }}
    {% endif %}
    AND wnt_usd.contract_address =  evm_info.wrapped_native_token_address
    AND wnt_usd.minute = DATE_TRUNC('minute', w.call_block_time)
    LEFT JOIN {{ source('prices', 'usd') }} s
    ON s.minute > TIMESTAMP '{{method_start_date}}'
    AND s.blockchain = '{{blockchain}}'
    {% if is_incremental() %}
      AND {{ incremental_predicate('s.minute') }}
    {% endif %}
    AND s.contract_address = w.src_token_for_joining
    AND s.minute = DATE_TRUNC('minute', w.call_block_time)
    

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
-- returnAmount uint256 
-- protocolFee uint256 
-- partnerFee uint256 
), delta_v2_swapSettleBatch as (  
SELECT
    -- NB: columns mapping must match accross all the methods, since they're unioned into one in master macro
    '{{blockchain}}' as blockchain,    
    method,
    order_index,
    call_trace_address,
    call_block_number,
    call_block_time,    
    call_tx_hash,
    call_tx_from,
    call_tx_to,
    evt_index,
    executorFeeAmount as executor_fee_amount,
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
    raw_tx_gas_used,
    raw_tx_gas_price, 
    wnt_price_usd,
    ordersCount,
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
    "order",
    wrapped_native_token_address
  FROM delta_v2_swapSettleBatch_master
)
{% endmacro %}