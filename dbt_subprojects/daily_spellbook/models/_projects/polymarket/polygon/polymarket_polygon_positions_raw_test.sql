{{
  config(
    schema = 'polymarket_polygon',
    alias = 'positions_raw_test',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    partition_by = ['month'],
    unique_key = ['month', 'day', 'address', 'token_address', 'token_id', 'blockchain'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')]
  )
}}

-- test model: uses optimized macro balances_incremental_subset_daily_test
-- key optimization: in incremental mode, reads last known state from target table
-- instead of re-scanning all historical source data

with token_list as (
    select
        0x4D97DCd97eC945f40cF65F87097ACe5EA0476045 as token_address
),

balances as (
    {{
      balances_incremental_subset_daily_test(
            blockchain = 'polygon',
            token_list = 'token_list',
            start_date = '2020-09-03'
      )
    }}
)

select
    blockchain
    , cast(date_trunc('month', day) as date) as month
    , day
    , address
    , token_address
    , token_standard
    , token_id
    , balance_raw
    , balance_raw / 1e6 as balance
from balances
