{{ config(alias='user_address_daily_transactions', materialized = 'table') }}
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
