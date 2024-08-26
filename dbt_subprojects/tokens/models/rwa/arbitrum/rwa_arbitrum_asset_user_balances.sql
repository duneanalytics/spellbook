{{
  config(
    schema = 'rwa_arbitrum',
    alias = 'asset_user_balances',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['day', 'address', 'token_address'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')]
  )
}}

with 
rwa_assets as (
  select 
    'rwa' as category,
    project,
    'asset' as version,
    token_address
  from {{ref('rwa_arbitrum_assets')}}
  where type = 'RWA'
),

{{
  balances_something_user_daily(
        balances_daily_agg = ref('tokens_arbitrum_balances_daily_agg'), 
        something = 'rwa_assets',
        start_date = '2023-11-17', 
        native_token='ETH'
  )
}}
