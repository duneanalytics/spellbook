with delays as (
    SELECT
        project
        , blockchain
        , datediff(now(), max(block_time)) age_of_last_record_days
    from {{ ref('dex_trades') }}
    group by 1,2
)

, sources as
-- Add sources for decoded projects where trades may not happen daily
(
select
        'fraxswap' as project
        , 'avalanche_c' as blockchain
        , datediff(now(), max(evt_block_time)) age_of_last_record_days
 from {{ source('fraxswap_avalanche_c', 'FraxswapPair_evt_Swap') }}
 group by 1,2
 union
 select
        'fraxswap' as project
        , 'bnb' as blockchain
        , datediff(now(), max(evt_block_time)) age_of_last_record_days
 from {{ source('fraxswap_bnb','FraxswapPair_evt_Swap') }}
 group by 1,2
  union
 select
        'dfx' as project
        , 'ethereum' as blockchain
        , datediff(now(), max(evt_block_time)) age_of_last_record_days
 from {{ source('dfx_finance_ethereum','Curve_evt_Trade') }}
 group by 1,2
   union
 select
        'hashflow' as project
        , 'ethereum' as blockchain
        , datediff(now(), max(evt_block_time)) age_of_last_record_days
 from {{ source('hashflow_ethereum','pool_evt_trade') }}
 group by 1,2
    union
 select
        'hashflow' as project
        , 'avalanche_c' as blockchain
        , datediff(now(), max(evt_block_time)) age_of_last_record_days
 from {{ source('hashflow_avalanche_c','Pool_evt_Trade') }}
 group by 1,2
      union
 select
        'zigzag' as project
        , 'arbitrum' as blockchain
        , datediff(now(), max(call_block_time)) age_of_last_record_days
 from {{ source('zigzag_test_v6_arbitrum','zigzag_settelment_call_matchOrders') }}
 group by 1,2
)

select
    d.project,
    d.blockchain,
    coalesce(s.age_of_last_record_days, 0) - d.age_of_last_record_days as age_of_last_record_days_source_minus_table
from delays d
left join sources s
on d.project = s.project and d.blockchain = s.blockchain
where coalesce(s.age_of_last_record_days, 0) - d.age_of_last_record_days != 0
