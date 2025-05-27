{{ config(
        schema = 'balances_bitcoin',
        alias = 'satoshi_day',
        materialized='incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')],
        unique_key = ['wallet_address', 'day'],
        partition_by = ['day'],
        post_hook='{{ expose_spells(\'["bitcoin"]\',
                                        "sector",
                                        "balances",
                                        \'["longnhbkhn"]\') }}'
        )
}}


with 
    days as (
        with list_day as (
            select sequence(
                {% if is_incremental() %}
                    (select max(day) from {{ this }})
                {% else %}
                    date('2009-01-03')
                {% endif %},
                date(now()),
                interval '1' day
            ) as day
        )
        select u.day from list_day cross join unnest(day) as u(day)
    )
  , daily_balances as
 (SELECT
    wallet_address,
    amount_raw,
    amount_raw as amount,
    amount_transfer_usd,
    day,
    lead(day, 1, date(now())) OVER (PARTITION BY wallet_address ORDER BY day) AS next_day
    FROM {{ ref('transfers_bitcoin_satoshi_rolling_day') }}
    {% if is_incremental() %}
    where {{ incremental_predicate('day') }}
    {% endif %}
    )

SELECT
    'bitcoin' as blockchain,
    d.day,
    b.wallet_address,
    b.amount_raw,
    b.amount,
    p.price as price_btc,
    b.amount_transfer_usd as profit,
    b.amount * p.price as amount_usd,
    b.amount * p.price + b.amount_transfer_usd as total_asset,
    now() as updated_at
FROM daily_balances b
INNER JOIN days d ON b.day <= d.day AND d.day < b.next_day
LEFT JOIN {{ source('prices', 'usd') }} p
    ON d.day = p.minute
    AND p.symbol='BTC'
    AND p.blockchain is null
    {% if is_incremental() %}
    and {{ incremental_predicate('p.minute') }}
    {% endif %}
