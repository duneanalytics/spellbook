{% macro delta_v2_swap_settle_batch(blockchain) %}
delta_v2_swap_settle_batch_ExpandedOrders as (
    select            
        ROW_NUMBER() OVER (ORDER BY call_block_time, call_tx_hash, call_trace_address, order_index) AS rn,
        * from 
        (
              SELECT 
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
          WHERE 
            call_success = true
            {% if is_incremental() %}
              AND {{ incremental_predicate('call_block_time') }}
            {% endif %}        
        )
),
delta_v2_swap_settle_batch_OrderSettledEvents as (
  SELECT 
    -- TODO: need a sample tx to make sure this ordering and then joining by the order down below is correct
    ROW_NUMBER() OVER (ORDER BY evt_block_time, evt_tx_hash, evt_index) AS rn,
    *
  FROM 
  
  (
    SELECT * FROM {{ source("paraswapdelta_"+ blockchain, "ParaswapDeltav2_evt_OrderSettled") }}
    -- important conditional - since OrderSettled is emitted by multilple methods
    -- this filtering still not 100% fix -- as theoretically multiple methods can be combined in on call
    -- consider case when settleSwap and settleBatchSwap are combined in one call
    WHERE evt_tx_hash in (select call_tx_hash from delta_v2_swap_settle_batch_ExpandedOrders)
    {% if is_incremental() %}
        AND {{ incremental_predicate('evt_block_time') }}
      {% endif %}        
  )
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
    JSON_EXTRACT_SCALAR("order", '$.partnerAndFee') as partnerAndFee,
    JSON_EXTRACT_SCALAR("order", '$.permit') as permit,        
    {{executor_fee_amount()}},    
    * 
from
    delta_v2_swap_settle_batch_ExpandedOrders    
), delta_v2_swap_settle_batch_withWrapped as (
  SELECT     
    {{to_wrapped_native_token(blockchain, 'orders.destToken', 'dest_token_for_joining')}},
    {{to_wrapped_native_token(blockchain, 'orders.srcToken', 'src_token_for_joining')}},
    events.returnAmount,
    events.protocolFee,
    events.partnerFee,
    events.evt_index,
    orders.*    
  FROM delta_v2_swap_settle_batch_parsed_orders orders
  --- NB: sourcing from calls and joining events, not the opposite, because some methods emit different events (*fill* -> OrderSettled / OrderPartiallyFilled). Sorting them by evt_index would make them match orders sorted by their index in array + call_trace_address -> source of truth bettr be calls
  LEFT JOIN delta_v2_swap_settle_batch_OrderSettledEvents events 
    ON orders.rn = events.rn 
    AND orders.call_tx_hash = events.evt_tx_hash
    -- TODO: compute hash and join by orderHash instead -- that would be sufficiently strict for fill-all-at-once methods (unlike partials, because with partials there's an edge case when you can mismatch still, although very unlikely to happen in real life)
    -- but for "full-single-fulfillment" methods, -- this template will work
    -- https://paraswap.slack.com/archives/C073CNPHUS2/p1736331626761619?thread_ts=1736325828.398779&cid=C073CNPHUS2
    AND orders.owner = events.owner
    AND orders.beneficiary = events.beneficiary    
    AND orders.srcToken = events.srcToken
    AND orders.destToken = events.destToken
    AND orders.srcAmount = events.srcAmount
    AND orders.destAmount = events.destAmount
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
    -- NB: columns mapping must match accross all the methods, since they're uninoned into one in master macro
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
    contract_address
  FROM delta_v2_swapSettleBatch_master
)
{% endmacro %}