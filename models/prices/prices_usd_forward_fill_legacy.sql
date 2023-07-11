{{ config(
        schema='prices',
        alias = alias('usd_forward_fill', legacy_model=True),
        post_hook='{{ expose_spells_hide_trino(\'["ethereum", "solana", "arbitrum", "gnosis", "optimism", "bnb", "avalanche_c", "polygon"]\',
                                    "sector",
                                    "prices",
                                    \'["0xRob"]\') }}'
        )
}}

-- how much time we look back, anything before is considered finalized, anything after is forward filled.
-- we could decrease this to optimize query performance but it's a tradeoff with resiliency to lateness.
{%- set lookback_interval = '2 day' %}


WITH
  finalized as (
    select *
    FROM {{ source('prices', 'usd') }}
    where minute <= now() - interval {{lookback_interval}}
)

, unfinalized as (
    select *,
        lead(minute) over (partition by blockchain,contract_address,decimals,symbol order by minute asc) as next_update_minute
    FROM {{ source('prices', 'usd') }}
    where minute > now() - interval {{lookback_interval}}
)

, timeseries as (
    select explode(sequence(
        date_trunc('minute', now() - interval {{lookback_interval}})
        ,date_trunc('minute', now())
        ,interval 1 minute)) as minute
)

, forward_fill as (
    select
    t.minute
    ,blockchain
    ,contract_address
    ,decimals
    ,symbol
    ,price
    from timeseries t
    left join unfinalized p
    ON t.minute >= p.minute and (p.next_update_minute is null OR t.minute < p.next_update_minute) -- perform forward fill
)

SELECT
    minute
    ,blockchain
    ,contract_address
    ,decimals
    ,symbol
    ,price
FROM finalized
UNION ALL
SELECT
    minute
    ,blockchain
    ,contract_address
    ,decimals
    ,symbol
    ,price
FROM forward_fill
;
