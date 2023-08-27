{{ 
    config(
        tags = ['dunesql'],
        alias = alias('erc20_noncompliant'),
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['token_address'],
        post_hook='{{ expose_spells(\'["celo"]\',
                                    "sector",
                                    "balances",
                                    \'["soispoke", "dot2dotseurat", "tomfutago"]\') }}'
    )
}}

select distinct token_address
from {{ ref('balances_celo_erc20_hour') }}
where recency_index = 1
  and amount < -0.001
