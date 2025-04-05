{{
  config(
    schema = 'polymarket_polygon',
    alias = 'positions_raw',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    partition_by = ['month'],
    unique_key = ['month', 'day', 'address', 'token_address', 'token_id', 'blockchain'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')]
  )
}}

with token_list as (
    select
        0x4D97DCd97eC945f40cF65F87097ACe5EA0476045 as token_address
), 
balances as (
    {{
      balances_incremental_subset_daily(
            blockchain = 'polygon',
            token_list = 'token_list',
            start_date = '2020-09-03'
      )
    }}
)

select
    'polygon' as blockchain
    , cast(date_trunc('month', day) as date) as month
    , day
    , address
    , token_address
    , token_id
    , balance / 1e6 AS balance
from balances