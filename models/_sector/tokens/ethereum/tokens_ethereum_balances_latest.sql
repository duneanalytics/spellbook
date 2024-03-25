{{ config(
        schema = 'tokens_ethereum',
        alias = 'balances_latest',
        materialized = 'view',
        post_hook = '{{ expose_spells(\'["ethereum"]\',
                                    "sector",
                                    "tokens",
                                    \'["0xRob"]\') }}'
        )
}}

select * from {{ref('tokens_ethereum_balances_daily')}}
where day = date_trunc('day',now())
