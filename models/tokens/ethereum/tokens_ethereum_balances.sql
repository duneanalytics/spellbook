{{ config(
        alias = 'balances',
        tags=['dunesql'],
        materialized = 'view',
        )
}}

{{balances_enrich(
    balances_base = ref('tokens_ethereum_base_balances'),
)}}
