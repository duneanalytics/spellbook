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
--- explode an array:                 SEQUENCE(0, CARDINALITY(output_successfulOrders) - 1)
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
                  executorData[order_index] as extractedExecutor                     
                FROM {{ source("paraswapdelta_"+ blockchain, "ParaswapDeltav2_call_swapSettleBatch") }}                    
                  CROSS JOIN UNNEST (
                      -- SQL array indices start at 1
                      -- also NB: if one order fails -- whole batch fails
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
        ROW_NUMBER() OVER (ORDER BY evt_block_time, evt_tx_hash, evt_index) AS rn,
        *
      FROM 
      
      (
        SELECT * FROM {{ source("paraswapdelta_"+ blockchain, "ParaswapDeltav2_evt_OrderSettled") }}
        {% if is_incremental() %}
            AND {{ incremental_predicate('call_block_time') }}
          {% endif %}        
      )
    ),
delta_v2_swap_settle_batch_model as (
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
    JSON_EXTRACT_SCALAR("order", '$.owner') as owner,
    JSON_EXTRACT_SCALAR("order", '$.beneficiary') as beneficiary,
    JSON_EXTRACT_SCALAR("order", '$.srcToken') as srcToken,
    JSON_EXTRACT_SCALAR("order", '$.destToken') as destToken,
    JSON_EXTRACT_SCALAR("order", '$.srcAmount') as srcAmount,
    JSON_EXTRACT_SCALAR("order", '$.destAmount') as destAmount,
    JSON_EXTRACT_SCALAR("order", '$.expectedDestAmount') as expectedDestAmount,
    JSON_EXTRACT_SCALAR("order", '$.deadline') as deadline,
    JSON_EXTRACT_SCALAR("order", '$.nonce') as nonce,
    JSON_EXTRACT_SCALAR("order", '$.partnerAndFee') as partnerAndFee,
    JSON_EXTRACT_SCALAR("order", '$.permit') as permit,
    orders.*,
    events.*    
from
    delta_v2_swap_settle_batch_ExpandedOrders orders
    left join delta_v2_swap_settle_batch_OrderSettledEvents events on
    orders.rn = events.rn

    limit 1000
)
--- NB: sourcing from calls and joining events, not the opposite, because some methods emit different events (*fill* -> OrderSettled / OrderPartiallyFilled). Sorting them by evt_index would make them match orders sorted by their index in array + call_trace_address -> source of truth bettr be calls
-- TODO: 1. join data from events
-- TODO: 2. native to wrapped conversion before joining with USD
{# {{to_wrapped_native_token(blockchain, 'dest_token', 'dest_token_for_joining')}}, #}
{# {{to_wrapped_native_token(blockchain, 'src_token', 'src_token_for_joining')}}, #}
-- TODO: 2. then from USD prices

{% endmacro %}