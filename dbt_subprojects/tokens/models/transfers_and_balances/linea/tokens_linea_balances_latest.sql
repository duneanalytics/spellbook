{{config(
    schema = 'tokens_linea',
    alias = 'balances_latest',
    tags = ['prod_exclude'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['unique_key'],
    post_hook='{{ expose_spells(\'["linea"]\',
                                "sector",
                                "balances_latest",
                                \'["et-dynamic"]\') }}'
)
}}

{{
    balances_latest(
        balances = ref('tokens_linea_balances'),
    )
}}
