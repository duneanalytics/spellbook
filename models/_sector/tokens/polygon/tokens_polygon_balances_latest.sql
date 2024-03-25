{{ config(
        schema = 'tokens_polygon',
        alias = 'balances_latest',
        materialized = 'view',
        post_hook = '{{ expose_spells(\'["polygon"]\',
                                    "sector",
                                    "tokens",
                                    \'["0xRob"]\') }}'
        )
}}

select * from {{ref('tokens_polygon_balances_daily')}}
where day = date_trunc('day',now())
