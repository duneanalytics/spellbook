{{ 
    config(
        tags = ['dunesql'],
        schema = 'balances_celo',
        alias = alias('erc20_noncompliant'),
        materialized = 'table',
        file_format = 'delta'
    )
}}

select token_address
from {{ ref('transfers_celo_erc20_rolling_hour') }}
where recency_index = 1
group by 1
having sum(amount) < -0.001
