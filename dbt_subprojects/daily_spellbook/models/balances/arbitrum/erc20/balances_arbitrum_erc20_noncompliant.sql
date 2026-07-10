{{ config(
        
        alias = 'erc20_noncompliant',
        materialized ='table',
        file_format = 'delta'
        )
}}

{{
    balances_fungible_noncompliant(
        transfers_agg_day = ref('transfers_arbitrum_erc20_agg_day')
    )
}}
