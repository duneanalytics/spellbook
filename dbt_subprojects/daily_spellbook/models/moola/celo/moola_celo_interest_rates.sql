{{ 
    config(
        
        schema = 'moola_celo',
        alias = 'interest',
        materialized = 'view',
        post_hook='{{ expose_spells(\'["celo"]\',
                                    "sector",
                                    "moola",
                                    \'["tomfutago"]\') }}'
    )
}}

select
  a.reserve,
  t.symbol,
  date_trunc('hour', a.evt_block_time) as evt_block_hour,
  avg(cast(a.liquidityRate as double)) / 1e27 as deposit_apy,
  avg(cast(a.stableBorrowRate as double)) / 1e27 as stable_borrow_apy,
  avg(cast(a.variableBorrowRate as double)) / 1e27 as variable_borrow_apy
from {{ source('moolainterestbearingmoo_celo', 'LendingPool_evt_ReserveDataUpdated') }} a
  left join {{ source('tokens_celo', 'erc20') }} t on a.reserve = t.contract_address
group by 1,2,3
