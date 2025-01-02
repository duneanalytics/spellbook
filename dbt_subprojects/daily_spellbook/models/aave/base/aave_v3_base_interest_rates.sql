{{ config(
   schema = 'aave_v3_base'
  , alias = 'interest_rates'
  , materialized = 'incremental'
  , file_format = 'delta'
  , incremental_strategy = 'merge'
  , unique_key = ['reserve', 'symbol', 'hour']
  , post_hook='{{ expose_spells(blockchains = \'["base"]\',
                                spell_type = "project",
                                spell_name = "aave_v3",
                                contributors = \'["mikeghen1","batwayne", "chuxin"]\') }}'
  )
}}

select 
  a.reserve, 
  t.symbol,
  date_trunc('hour',a.evt_block_time) as hour, 
  avg(CAST(a.liquidityRate AS DOUBLE)) / 1e27 as deposit_apy, 
  avg(CAST(a.stableBorrowRate AS DOUBLE)) / 1e27 as stable_borrow_apy, 
  avg(CAST(a.variableBorrowRate AS DOUBLE)) / 1e27 as variable_borrow_apy
from {{ source('aave_v3_base', 'L2Pool_evt_ReserveDataUpdated') }} a
left join {{ source('tokens', 'erc20') }} t
on a.reserve = t.contract_address and t.blockchain = 'base'
{% if is_incremental() %}
    WHERE {{ incremental_predicate('evt_block_time') }}
{% endif %}
group by 1,2,3
