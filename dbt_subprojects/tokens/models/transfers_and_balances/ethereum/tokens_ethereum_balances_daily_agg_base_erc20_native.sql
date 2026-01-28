{{ config(
        schema = 'tokens_ethereum',
        alias = 'balances_daily_agg_base_erc20_native',
        materialized='view'        
        )
}}

select
        blockchain
        ,day
        ,address
        ,token_address
        ,token_standard
        ,balance_raw
        ,unique_key
from {{ref('tokens_ethereum_balances_daily_agg')}}
where token_standard in ('erc20', 'native')