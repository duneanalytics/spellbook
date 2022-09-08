{{ config(alias='user_address_daily_transactions', materialized = 'table') }}
select 
  *
from ({{ ref('ovm1_q1_user_address_daily_transactions') }})

union all 

select 
  *
from ({{ ref('ovm1_q2_user_address_daily_transactions') }})

union all

select 
  *
from ({{ ref('ovm1_q3_user_address_daily_transactions') }})

union all

select 
  *
from ({{ ref('ovm1_q4_user_address_daily_transactions') }})
