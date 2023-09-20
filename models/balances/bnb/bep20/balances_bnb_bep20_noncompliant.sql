{{ config(
        tags = ['dunesql'],
        schema = 'balances_bnb_bep20',
        alias = alias('noncompliant'),
        materialized ='table',
        file_format = 'delta'
        )
}}

{{
    balances_fungible_noncompliant(
        transfers_rolling_day = ref('transfers_bnb_bep20_rolling_day')
    )
}}
