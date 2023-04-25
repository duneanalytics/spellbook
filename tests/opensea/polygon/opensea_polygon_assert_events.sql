-- Check if all fractal events make it into the processed events table
with opensea_trades_test AS (
   select call_block_time,
        call_tx_hash,
        case when length(rightOrder:makerAssetData) = 650 then cast(bytea2numeric_v3(substr(rightOrder:makerAssetData,332,64)) as string)
                else cast(bytea2numeric_v3(substr(rightOrder:makerAssetData,76,64)) as string)
        end AS token_id,
        least(cast(output_matchedFillResults:left:takerFeePaid as numeric), cast(output_matchedFillResults:right:makerFeePaid as numeric)) AS number_of_items
   from {{ source('opensea_polygon_v2_polygon','ZeroExFeeWrapper_call_matchOrders') }} a
   where 1=1
     and call_success
     and call_block_time >= '2022-06-01'
     and call_block_time < '2022-07-01'
),

raw_events as (
    SELECT call_block_time as raw_block_time,
        call_tx_hash as raw_tx_hash,
        call_tx_hash || '-Trade-0-' || token_id || '-' || cast(coalesce(number_of_items, 1) as string) as raw_unique_trade_id
    from opensea_trades_test
),

processed_events AS (
    SELECT block_time,
        tx_hash,
        unique_trade_id
    FROM {{ ref('opensea_polygon_events') }}
    WHERE block_time >= '2022-06-01'
        AND block_time < '2022-07-01'
        AND evt_type = 'Trade'
)

SELECT *
FROM raw_events r
FULL JOIN processed_events n ON r.raw_block_time = n.block_time AND r.raw_unique_trade_id = n.unique_trade_id
WHERE r.raw_unique_trade_id IS NULL 
    Or n.unique_trade_id IS NULL
