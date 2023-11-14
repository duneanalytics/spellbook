{{ config(
        schema = 'tokens_ethereum',
        alias = 'balances',
        materialized = 'view'
        )
}}

{{
    balances_enrich(
        balances_base = ref('tokens_ethereum_base_balances')
    )
}}