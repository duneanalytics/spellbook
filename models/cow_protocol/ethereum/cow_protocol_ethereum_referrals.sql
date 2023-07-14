{{ config(
        alias = alias('referrals'),
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "cow_protocol",
                                    \'["bh2smith"]\') }}'
)}}

-- PoC Query: https://dune.com/queries/1789628?d=1
WITH
referral_map as (
    select
        distinct app_hash,
        referrer
    from {{ ref('cow_protocol_ethereum_app_data_legacy') }}
    where referrer is not null
)

-- Table with first trade per user. Used to determine their referral
,ordered_user_trades AS (
    SELECT
        ROW_NUMBER() OVER(PARTITION BY trader ORDER BY block_time, evt_index) AS user_trade_index,
        trader,
        app_data
    FROM {{ ref('cow_protocol_ethereum_trades_legacy') }}
    GROUP BY trader, block_time, app_data, evt_index
)
,user_first_trade as (
    select
        trader,
        app_data
    from ordered_user_trades
    -- Only considers app_data from first trade!
    where user_trade_index = 1
)

,referrals as (
    select
        trader,
        referrer
    from referral_map
    inner join user_first_trade
        on app_hash = app_data
)

select * from referrals