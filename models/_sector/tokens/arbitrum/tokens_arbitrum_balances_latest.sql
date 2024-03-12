{{ config(
        schema = 'tokens_arbitrum',
        alias = 'balances_latest',
        materialized = 'view',
        post_hook = '{{ expose_spells(\'["arbitrum"]\',
                                    "sector",
                                    "tokens",
                                    \'["0xRob"]\') }}'
        )
}}

select * from {{ref('tokens_arbitrum_balances_daily')}}
where day = date_trunc('day',now())
