{{ config(
        schema = 'tokens_ethereum',
        alias = 'balances',
        materialized = 'view',
        tags = ['prod_exclude']
        )
}}

{{
    balances_enrich(
        balances_base = ref('tokens_ethereum_base_balances')
    )
}}