{{ config(
   schema = 'aave_v3_optimism'
  , materialized = 'incremental'
  , file_format = 'delta'
  , incremental_strategy = 'merge'
  , unique_key = ['reserve', 'symbol', 'hour']
  , alias = 'interest'
  , post_hook='{{ expose_spells(\'["optimism"]\',
                                  "project",
                                  "aave_v3",
                                  \'["batwayne", "chuxin"]\') }}'
  )
}}

select 
  a.reserve, 
  t.symbol,
  date_trunc('hour',a.evt_block_time) as hour, 
  avg(CAST(a.liquidityRate AS DOUBLE)) / 1e27 as deposit_apy, 
  avg(CAST(a.stableBorrowRate AS DOUBLE)) / 1e27 as stable_borrow_apy, 
  avg(CAST(a.variableBorrowRate AS DOUBLE)) / 1e27 as variable_borrow_apy
from {{ source('aave_v3_optimism', 'Pool_evt_ReserveDataUpdated') }} a
left join {{ source('tokens_optimism', 'erc20') }} t
on a.reserve = t.contract_address
{% if is_incremental() %}
    WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
{% endif %}
group by 1,2,3
