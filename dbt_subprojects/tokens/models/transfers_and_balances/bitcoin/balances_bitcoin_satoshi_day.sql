{{ config(
        schema = 'balances_bitcoin',
        alias = 'satoshi_day',
        partition_by = ['day'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['day', 'wallet_address'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')],
        post_hook='{{ expose_spells(\'["bitcoin"]\',
                                        "sector",
                                        "balances",
                                        \'["longnhbkhn"]\') }}'
        )
}}

with balances as (
    {{
        balances_incremental_subset_daily(
            blockchain = 'bitcoin',
            address_list = 'transfers_bitcoin_satoshi_rolling_day',
            start_date = '2009-01-03'
        )
    }}
)

SELECT
    'bitcoin' as blockchain,
    b.day,
    b.wallet_address,
    b.amount_raw,
    b.amount,
    p.price as price_btc,
    b.amount_transfer_usd as profit,
    b.amount * p.price as amount_usd,
    b.amount * p.price + b.amount_transfer_usd as total_asset,
    now() as updated_at
FROM balances b
LEFT JOIN {{ source('prices', 'usd') }} p
    ON b.day = p.minute
    AND p.symbol='BTC'
    AND p.blockchain is null
