{{config(
    schema = 'tokens_arbitrum',
    alias = 'balances_latest',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['unique_key'],
    post_hook='{{ expose_spells(\'["arbitrum"]\',
                                "sector",
                                "balances_latest",
                                \'["et-dynamic"]\') }}'
)
}}

{{
    balances_latest(
        balances = ref('tokens_arbitrum_balances'),
    )
}}
