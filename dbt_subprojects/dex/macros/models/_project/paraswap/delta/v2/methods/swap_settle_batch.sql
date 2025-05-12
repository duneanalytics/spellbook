{% macro delta_v2_swap_settle_batch(blockchain) %}

-- {% set method_start_date = '2024-10-01' %}
{% set method_start_date = '2025-05-10' %}

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
      executor,
      executorData[order_index] as executorData                     
    FROM {{ source("paraswapdelta_"+ blockchain, "ParaswapDeltav2_call_swapSettleBatch") }}                    
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
    from_hex(JSON_EXTRACT_SCALAR("order", '$.owner')) as owner,
    from_hex(JSON_EXTRACT_SCALAR("order", '$.beneficiary')) as beneficiary,
    from_hex(JSON_EXTRACT_SCALAR("order", '$.srcToken')) as srcToken,
    from_hex(JSON_EXTRACT_SCALAR("order", '$.destToken')) as destToken,
    cast(JSON_EXTRACT_SCALAR("order", '$.srcAmount') as uint256) as srcAmount,
    cast(JSON_EXTRACT_SCALAR("order", '$.destAmount') as uint256) as destAmount,
    cast(JSON_EXTRACT_SCALAR("order", '$.expectedDestAmount') as uint256) as expectedDestAmount,
    JSON_EXTRACT_SCALAR("order", '$.deadline') as deadline,
    JSON_EXTRACT_SCALAR("order", '$.nonce') as nonce,
    cast(JSON_EXTRACT_SCALAR("order", '$.partnerAndFee') as uint256) as partnerAndFee,
    cast(JSON_EXTRACT_SCALAR("order", '$.permit') as varbinary) as permit,
    {{executor_fee_amount()}},    
    * 
from
    delta_v2_swap_settle_batch_ExpandedOrders    
), 
delta_v2_swap_settle_batch_parsed_orders_with_orderhash as (
  select 
    *,
    {{ compute_order_hash(blockchain) }} as computed_order_hash 
  from delta_v2_swap_settle_batch_parsed_orders
),
delta_v2_swap_settle_batch_withWrapped as (
  SELECT     
    {{to_wrapped_native_token(blockchain, 'orders.destToken', 'dest_token_for_joining')}},
    {{to_wrapped_native_token(blockchain, 'orders.srcToken', 'src_token_for_joining')}},    
    events.evt_index,
    events.orderHash as evt_order_hash,
    events.returnAmount as evt_return_amount,
    events.protocolFee as evt_protocol_fee,
    events.partnerFee as evt_partner_fee,
    orders.*    
  FROM delta_v2_swap_settle_batch_parsed_orders_with_orderhash orders
  --- NB: sourcing from calls and joining events, not the opposite, because some methods emit different events (*fill* -> OrderSettled / OrderPartiallyFilled). Sorting them by evt_index would make them match orders sorted by their index in array + call_trace_address -> source of truth bettr be calls
  LEFT JOIN {{ source("paraswapdelta_"+ blockchain, "ParaswapDeltav2_evt_OrderSettled") }} events 
    ON
      evt_block_time = call_block_time  
    -- suffices for fill-all-at-once methods (unlike partials, because with partials there's an edge case when you can mismatch still, although very unlikely to happen in real life)
    AND computed_order_hash = events.orderHash    
    AND orders.call_tx_hash = events.evt_tx_hash
), delta_v2_swapSettleBatch_master as (

select 
    'swapSettleBatch' as method,
    COALESCE(CAST(s.price AS DECIMAL(38,18)), 0) AS src_token_price_usd,
    COALESCE(CAST(d.price AS DECIMAL(38,18)), 0) AS dest_token_price_usd,     
    {{ gas_fee_usd() }},
    s.price *  w.srcAmount / POWER(10, s.decimals)  AS src_token_order_usd,
    d.price *  w.destAmount / POWER(10, d.decimals)  AS dest_token_order_usd,
    w.destToken AS fee_token,
    w.*
from delta_v2_swap_settle_batch_withWrapped w

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
    evt_partner_fee
  FROM delta_v2_swapSettleBatch_master
)
{% endmacro %}