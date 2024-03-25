 {{
  config(
        schema = 'jupiter_solana',
        alias = 'perp_events',
        partition_by = ['block_month'],
        materialized='incremental',
        file_format = 'delta',
        incremental_strategy='merge',
        unique_key = ['position_change','position_key','tx_id'],
        post_hook='{{ expose_spells(\'["jupiter"]\',
                                    "project",
                                    "jupiter_solana",
                                    \'["ilemi"]\') }}')
}}

-- IncreasePositionEvent
    --create position request
    --then fillers spam create increase/decrease until it settles (preswap can happen but just ignore it)
    --orders are aggregated under same position key
    --example long https://solana.fm/tx/3to8mJ9DPmb3vSXXLF7QU2vjggHW21JoGCwtuJ29HKVvbf8iBWHetFmf3Ewx8E54XA2ypvQkqZqKajFJpYx8Qxjd?cluster=mainnet-qn1
    --example short https://solana.fm/tx/5eUVyRYyFVUX7vBZLhqbz3ahLTYLPzYSfpFskdukAvtQatEAT1adEzvrdWn7xoAwuL5wYjMFG2o7PtXGFjoT5fKa?cluster=mainnet-qn1
SELECT
    'increase' AS position_change
    , bytearray_to_bigint(bytearray_reverse(bytearray_substring(data,1+291,8))) / 1e6 as size_usd --this is the levered size delta (change)
    , bytearray_to_bigint(bytearray_reverse(bytearray_substring(data,1+299,8))) / 1e6 as collateral_usd --collateral size, transferred into pool
    , bytearray_to_bigint(bytearray_reverse(bytearray_substring(data,1+307,8))) as collateral_token --collateral notional size, transferred into pool
    , bytearray_to_bigint(bytearray_reverse(bytearray_substring(data,1+340,8))) / 1e6 as fee_usd --open fee
    , bytearray_to_bigint(bytearray_reverse(bytearray_substring(data,1+315,8))) / 1e6 as price_usd --entry price
    , null as liq_fee_usd
    , null as pnl_direction
    , null as pnl_usd --no pnl on increase
    , to_base58(bytearray_substring(data,1+227,32)) as owner
    , to_base58(bytearray_substring(data,1+16,32)) as position_key
    , bytearray_to_bigint(bytearray_reverse(bytearray_substring(data,1+16+32,1))) as position_side --1 is long, 2 is short
    , to_base58(bytearray_substring(data,1+16+32+1,32)) as custody_position_key --ties to the token mint of long or short token
    , to_base58(bytearray_substring(data,1+16+32+1+32,32)) as custody_collateral_key --ties to the token mint of the collateral token
    , block_slot
    , block_time
    , cast(date_trunc('month', block_time) as date) as block_month
    , tx_id
FROM {{ source('solana','instruction_calls') }}
WHERE executing_account = 'PERPHjGBqRHArX4DySjwM6UJHiR3sWAatqfdBS2qQJu'
AND bytearray_substring(data,1+8,8) = 0xf5715534d6bb9984 -- IncreasePosition
AND tx_success = true
{% if is_incremental() %}
AND {{ incremental_predicate('block_time') }}
{% endif %}

UNION ALL 

-- DecreasePositionEvent
-- decrease has pnl and has profit indicators.
-- example decrease https://solana.fm/tx/5PURicyWYuAiYsJHgzgubuBfsoSYXQYX812RhmdbyCUtLRrpd6WRq6GNcrhEGXnkB5hZ9zHr4LMiagQGrfPYnGYo?cluster=mainnet-qn1
SELECT
    'decrease' AS position_change
    , bytearray_to_bigint(bytearray_reverse(bytearray_substring(data,1+292,8))) / 1e6 as size_usd --this is the levered size delta (change)
    , bytearray_to_bigint(bytearray_reverse(bytearray_substring(data,1+300,8))) / 1e6 as collateral_usd --collateral size, transferred back to owner
    , bytearray_to_bigint(bytearray_reverse(bytearray_substring(data,1+309,8))) as collateral_token --collateral notional size, transferred back to owner
    , case when bytearray_to_bigint(bytearray_reverse(bytearray_substring(data,1+325,1))) = 1 
        then bytearray_to_bigint(bytearray_reverse(bytearray_substring(data,1+334,8))) / 1e6 
        else bytearray_to_bigint(bytearray_reverse(bytearray_substring(data,1+326,8))) / 1e6
        end as fee_usd --close fee, has an optional param priceSlippage before it so we need this case when.
    , bytearray_to_bigint(bytearray_reverse(bytearray_substring(data,1+317,8))) / 1e6 as price_usd --exit price
    , null as liq_fee_usd
    , bytearray_to_bigint(bytearray_reverse(bytearray_substring(data,1+219,1))) as pnl_direction --0 means negative, 1 means positive
    , bytearray_to_bigint(bytearray_reverse(bytearray_substring(data,1+220,8))) / 1e6 as pnl_usd --pnl only on decrease position and liquidation
    , to_base58(bytearray_substring(data,1+228,32)) as owner
    , to_base58(bytearray_substring(data,1+16,32)) as position_key
    , bytearray_to_bigint(bytearray_reverse(bytearray_substring(data,1+16+32,1))) as position_side --1 is long, 2 is short
    , to_base58(bytearray_substring(data,1+16+32+1,32)) as custody_position_key --ties to the token mint of long or short token
    , to_base58(bytearray_substring(data,1+16+32+1+32,32)) as custody_collateral_key --ties to the token mint of the collateral token
    , block_slot
    , block_time
    , cast(date_trunc('month', block_time) as date) as block_month
    , tx_id
FROM {{ source('solana','instruction_calls') }}
WHERE executing_account = 'PERPHjGBqRHArX4DySjwM6UJHiR3sWAatqfdBS2qQJu'
AND bytearray_substring(data,1+8,8) = 0x409c2b4a6d83107f -- DecreasePosition
AND tx_success = true
{% if is_incremental() %}
AND {{ incremental_predicate('block_time') }}
{% endif %}

UNION ALL 

-- LiquidatePositionEvent (there is also a LiquidateFullPositionEvent)
SELECT
    'liquidate' AS position_change
    , bytearray_to_bigint(bytearray_reverse(bytearray_substring(data,1+177,8))) / 1e6 as size_usd --this is the levered size delta (change)
    , bytearray_to_bigint(bytearray_reverse(bytearray_substring(data,1+258,8))) / 1e6 as collateral_usd --collateral size
    , bytearray_to_bigint(bytearray_reverse(bytearray_substring(data,1+266,8))) as collateral_token --collateral notional size, stays in pool.
    , bytearray_to_bigint(bytearray_reverse(bytearray_substring(data,1+282,8))) / 1e6 as fee_usd --close fee
    , bytearray_to_bigint(bytearray_reverse(bytearray_substring(data,1+274,8))) / 1e6 as price_usd --exit price
    , case when bytearray_substring(data,1+8,8) = 0x806547a880485654 then bytearray_to_bigint(bytearray_reverse(bytearray_substring(data,1+290,8))) / 1e6 
        else 0 end as liq_fee_usd --liq fee, is only on new liquidation events
    , bytearray_to_bigint(bytearray_reverse(bytearray_substring(data,1+185,1))) as pnl_direction --0 means negative, 1 means positive
    , bytearray_to_bigint(bytearray_reverse(bytearray_substring(data,1+186,8))) / 1e6 as pnl_usd --pnl only on decrease position and liquidation
    , to_base58(bytearray_substring(data,1+194,32)) as owner
    , to_base58(bytearray_substring(data,1+16,32)) as position_key
    , bytearray_to_bigint(bytearray_reverse(bytearray_substring(data,1+16+32,1))) as position_side --1 is long, 2 is short
    , to_base58(bytearray_substring(data,1+16+32+1,32)) as custody_position_key --ties to the token mint of long or short token
    , to_base58(bytearray_substring(data,1+16+32+1+32,32)) as custody_collateral_key --ties to the token mint of the collateral token
    , block_slot
    , block_time
    , cast(date_trunc('month', block_time) as date) as block_month
    , tx_id
FROM {{ source('solana','instruction_calls') }}
WHERE executing_account = 'PERPHjGBqRHArX4DySjwM6UJHiR3sWAatqfdBS2qQJu'
AND bytearray_substring(data,1+8,8) IN (0x68452084d423bf2f, 0x806547a880485654) --LiquidatePosition, LiquidateFullPosition
AND tx_success = true
{% if is_incremental() %}
AND {{ incremental_predicate('block_time') }}
{% endif %}