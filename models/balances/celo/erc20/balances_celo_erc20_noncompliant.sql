{{ 
    config(
        tags = ['dunesql'],
        alias = alias('erc20_noncompliant'),
        post_hook='{{ expose_spells(\'["celo"]\',
                                    "sector",
                                    "balances",
                                    \'["tomfutago"]\') }}'
    )
}}

select token_address
from {{ ref('transfers_celo_erc20_rolling_hour') }}
where recency_index = 1
group by 1
having sum(amount) < -0.001
