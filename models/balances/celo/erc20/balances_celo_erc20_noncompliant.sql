{{ 
    config(
        tags = ['dunesql'],
        schema = 'balances_celo',
        alias = alias('erc20_noncompliant'),
        materialized = 'table',
        file_format = 'delta'
    )
}}

select DISTINCT token_address
from {{ ref('transfers_celo_erc20_rolling_day') }}
WHERE round(amount/power(10, 18), 6) < -0.001
