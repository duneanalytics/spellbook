{{ config(
        schema = 'balances_polygon',
        tags = ['dunesql'],
        alias = alias('erc20_noncompliant'),
        materialized ='table',
        file_format = 'delta'
        )
}}


{{
    balances_fungible_noncompliant(
        transfers_rolling_day = ref('transfers_polygon_erc20_rolling_day')
    )
}}
