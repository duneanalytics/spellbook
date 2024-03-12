{{ config(
        schema = 'tokens_base',
        alias = 'balances_latest',
        materialized = 'view',
        post_hook = '{{ expose_spells(\'["base"]\',
                                    "sector",
                                    "tokens",
                                    \'["0xRob"]\') }}'
        )
}}

select * from {{ref('tokens_base_balances_daily')}}
where day = date_trunc('day',now())
