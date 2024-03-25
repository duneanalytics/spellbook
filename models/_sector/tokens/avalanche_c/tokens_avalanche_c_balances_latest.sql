{{ config(
        schema = 'tokens_avalanche_c',
        alias = 'balances_latest',
        materialized = 'view',
        post_hook = '{{ expose_spells(\'["avalanche_c"]\',
                                    "sector",
                                    "tokens",
                                    \'["0xRob"]\') }}'
        )
}}

select * from {{ref('tokens_avalanche_c_balances_daily')}}
where day = date_trunc('day',now())
