{{ config(alias='user_address_daily_transactions', materialized = 'table',
          post_hook='{{ expose_spells(\'["optimism"]\',
                                        "sector",
                                        "ovm1",
                                        \'["msilb7", "chuxinh"]\') }}') }}
select 
  *
from ({{ ref('ovm1_optimism_q1_user_address_daily_transactions') }})

union all 

select 
  *
from ({{ ref('ovm1_optimism_q1_user_address_daily_transactions') }})

union all

select 
  *
from ({{ ref('ovm1_optimism_q1_user_address_daily_transactions') }})

union all

select 
  *
from ({{ ref('ovm1_optimism_q1_user_address_daily_transactions') }})
