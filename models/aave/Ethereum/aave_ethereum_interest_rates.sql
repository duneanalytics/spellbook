{{ config(materialized='view', alias='aave_interest') }}

select reserve, 
date_trunc('hour',evt_block_time) as hour, 
avg(liquidityRate) / 1e27 as deposit_apy, 
avg(stableBorrowRate) / 1e27 as stable_borrow_apy, 
avg(variableBorrowRate) / 1e27 as variable_borrow_apy
from {{ source('aave_v2_ethereum', 'LendingPool_evt_ReserveDataUpdated') }}
group by 1,2
