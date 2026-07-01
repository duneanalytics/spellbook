{{ config(
        
        alias = 'noncompliant',
        materialized ='table',
        file_format = 'delta'
        )
}}

{{
    balances_fungible_noncompliant(
        transfers_agg_day = ref('transfers_bnb_bep20_agg_day'),
        day_column = 'day'
    )
}}
