{% macro delta_v2_swap_settle_batch(blockchain) %}

-- standard fields:
-- contract_address varbinary
-- call_success boolean
-- call_tx_hash varbinary
-- call_tx_from varbinary
-- call_tx_to varbinary
-- call_trace_address array(bigint)
-- call_block_time timestamp
-- call_block_number bigint

-- usefull payload:
-- ordersWithSigs array(varchar)
-- executorData array(varbinary)
-- executor varbinary

-- util payload:
-- call_trace_address - should be explicitely sorted by this field, if there's a call that combines multiple calls 
-- evt_index - matching events should be sorted by evt_index correspondingly

-- useful utils & info
--- explode an array:                 SEQUENCE(1, CARDINALITY(output_successfulOrders))
--- access data: JSON_EXTRACT / JSON_EXTRACT_SCALAR 

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
                  -- call_tx_from, -- varbinary
                  -- call_tx_to, -- varbinary
                  call_trace_address, -- array(bigint)
                  call_block_time, -- timestamp
                  call_block_number, -- bigint
                  -- ordersWithSigs[order_index] as extractedOrderWithSig,
                  -- JSON_EXTRACT(JSON_PARSE(TRY_CAST(ordersWithSigs[order_index] AS VARCHAR)), '$.order') AS "order", -- kinda works, but returns a string
                  
                  JSON_EXTRACT_SCALAR(ordersWithSigs[order_index], '$.order') as "order", -- works best, returns json
                  JSON_EXTRACT_SCALAR(ordersWithSigs[order_index], '$.signature') as signature, -- works best, returns json            
                  -- JSON_EXTRACT_SCALAR(JSON_EXTRACT_SCALAR(ordersWithSigs[order_index], '$.order'), '$.owner') as owner,
                  
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
-- {
--   "owner": "0xb0326588271f2531fbfc9a13d52d3da45be1d956",
--   "beneficiary": "0xb0326588271f2531fbfc9a13d52d3da45be1d956",
--   "srcToken": "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",
--   "destToken": "0x940181a94a35a4569e4529a3cdfb74e38fd98631",
--   "srcAmount": 500000,
--   "destAmount": 245364045719396700,
--   "expectedDestAmount": 258277942862522850,
--   "deadline": 1733555737,
--   "nonce": 140,
--   "partnerAndFee": 0,
--   "permit": "0x"
-- }
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
    -- NB: at the time of writting the only ExecutorData shape known is following. On adding new executors needs to be-reconsidered 
    -- struct ExecutorData {
--         // The address of the src token
--         address srcToken;
--         // The address of the dest token
--         address destToken;
--         // The amount of fee to be paid for the swap 
--         uint256 feeAmount;                                                                   <- the field in question
--         // The calldata to execute the swap
--         bytes calldataToExecute;
--         // The address to execute the swap
--         address executionAddress;
--         // The address to receive the fee, if not set the tx.origin will receive the fee
--         address feeRecipient;
--     }
    varbinary_to_uint256(varbinary_substring(executorData,  161, 32)) as "executorFeeAmount",
    * 
from
    delta_v2_swap_settle_batch_ExpandedOrders    
), delta_v2_swap_settle_batch_withWrapped as (
  SELECT 
  -- TODO: 2. native to wrapped conversion before joining with USD
    {{to_wrapped_native_token(blockchain, 'orders.destToken', 'dest_token_for_joining')}},
    {{to_wrapped_native_token(blockchain, 'orders.srcToken', 'src_token_for_joining')}},
    events.returnAmount,
    events.protocolFee,
    events.partnerFee,
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
    COALESCE( 
        d.price * w.executorFeeAmount / POWER(10, d.decimals),
        -- src cost 
        
        -- TODO: not sure about this calc, needs verifying 
        -- used to have this fallback but maybe it shouldn't be here, and it might have been wrong
        -- (s.price *  CAST (w.src_amount AS uint256) / POWER(10, s.decimals))
        -- * CAST (w.feeAmount AS DECIMAL) / (CAST (w.dest_amount AS DECIMAL)+ CAST (w.feeAmount AS DECIMAL)),
        0
    )  AS gas_fee_usd,
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
-- returnAmount uint256
-- protocolFee uint256
-- partnerFee uint256   
), delta_v2_swapSettleBatch as (  
SELECT 
    '{{blockchain}}' as blockchain,
    method,
    order_index,
    call_trace_address,
    call_block_number,
    call_block_time,    
    call_tx_hash,
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