{{ config(
        tags = ['dunesql'],
        schema = 'balances_arbitrum',
        alias = alias('erc20_noncompliant'),
        materialized ='table',
        file_format = 'delta'
        )
}}

{{
    balances_fungible_noncompliant(
        transfers_rolling_day = ref('transfers_arbitrum_erc20_rolling_day')
    )
}}
