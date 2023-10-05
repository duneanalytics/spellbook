{{ config(
	tags=['legacy'],
	
  schema = 'aave_v3_optimism'
  , materialized = 'incremental'
  , file_format = 'delta'
  , incremental_strategy = 'merge'
  , unique_key = ['reserve', 'symbol', 'hour']
  , alias = alias('interest', legacy_model=True)
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
left join {{ ref('tokens_optimism_erc20_legacy') }} t
on CAST(a.reserve AS VARCHAR(100)) = t.contract_address
{% if is_incremental() %}
    WHERE evt_block_time >= date_trunc('day', now() - interval '1 week')
{% endif %}
group by 1,2,3
