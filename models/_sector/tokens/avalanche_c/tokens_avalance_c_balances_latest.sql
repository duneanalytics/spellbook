{{ config(
        schema = 'tokens_avalance_c',
        alias = 'balances_latest',
        materialized = 'view',
        post_hook = '{{ expose_spells(\'["avalance_c"]\',
                                    "sector",
                                    "tokens",
                                    \'["0xRob"]\') }}'
        )
}}

select * from {{ref('tokens_avalance_c_balances_daily')}}
where day = date_trunc('day',now())
