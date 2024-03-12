{{ config(
        schema = 'tokens_optimism',
        alias = 'balances_latest',
        materialized = 'view',
        post_hook = '{{ expose_spells(\'["optimism"]\',
                                    "sector",
                                    "tokens",
                                    \'["0xRob"]\') }}'
        )
}}

select * from {{ref('tokens_optimism_balances_daily')}}
where day = date_trunc('day',now())
