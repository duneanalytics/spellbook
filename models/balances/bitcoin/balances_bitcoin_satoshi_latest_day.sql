{{ config(
        schema = 'balances_bitcoin',
        alias = 'satoshi_latest_day',
        materialized='view',

        post_hook='{{ expose_spells(\'["bitcoin"]\',
                                        "sector",
                                        "balances",
                                        \'["gandalf"]\') }}'
        )
}}

WITH
      updated_balances as (
            SELECT
                blockchain
                , day
                , wallet_address
                , amount_raw
                , amount
                , price_btc
                , profit
                , amount_usd
                , total_asset
                , row_number() OVER (partition by wallet_address order by day desc) as latest_balance
            FROM {{ ref('balances_bitcoin_satoshi_day') }}
      )

SELECT
    blockchain
    , day
    , wallet_address
    , amount_raw
    , amount
    , price_btc
    , profit
    , amount_usd
    , total_asset
    , now() as updated_at
FROM updated_balances ub
WHERE latest_balance = 1
