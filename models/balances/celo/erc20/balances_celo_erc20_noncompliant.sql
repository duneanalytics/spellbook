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
from {{ ref('transfers_celo_erc20_rolling_day') }}
where amount < -0.001
