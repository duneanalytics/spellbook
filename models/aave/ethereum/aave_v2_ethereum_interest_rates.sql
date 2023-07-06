{{ config(
  schema = 'aave_v2_ethereum'
  , alias='interest'
  )
}}

select 
  a.reserve, 
  t.symbol,
  date_trunc('hour',a.evt_block_time) as hour, 
  avg(CAST(a.liquidityRate AS DOUBLE)) / 1e27 as deposit_apy, 
  avg(CAST(a.stableBorrowRate AS DOUBLE)) / 1e27 as stable_borrow_apy, 
  avg(CAST(a.variableBorrowRate AS DOUBLE)) / 1e27 as variable_borrow_apy
from {{ source('aave_v2_ethereum', 'LendingPool_evt_ReserveDataUpdated') }} a
left join {{ ref('tokens_ethereum_erc20_legacy') }} t
on CAST(a.reserve AS VARCHAR(100)) = t.contract_address
group by 1,2,3
;