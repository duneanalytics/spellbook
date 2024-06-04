{{ config(
        schema = 'balances_gnosis',
        
        alias = 'erc20_noncompliant',
        materialized ='table',
        file_format = 'delta'
        )
}}


{{
    balances_fungible_noncompliant(
        transfers_rolling_day = ref('transfers_gnosis_erc20_rolling_day')
    )
}}
