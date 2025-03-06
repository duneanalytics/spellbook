{{config(
    schema = 'tokens_optimism',
    alias = 'balances_latest',
    tags = ['prod_exclude'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['unique_key'],
    post_hook='{{ expose_spells(\'["optimism"]\',
                                "sector",
                                "balances_latest",
                                \'["et-dynamic"]\') }}'
)
}}

{{
    balances_latest(
        balances = ref('tokens_optimism_balances'),
    )
}}
