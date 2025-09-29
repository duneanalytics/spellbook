{% macro dex_pools_balances(
    blockchain = null
    , start_date  = null
    , pools_table = null
    )
%}

with 

pool_addresses as (
    select 
        id as address 
    from 
    {{ pools_table }}
),

filtered_balances AS (
  {{ balances_incremental_subset_daily(
       blockchain=blockchain,
       start_date='{{start_date}}',
       address_list='pool_addresses'
  ) }}
)

select 
    * 
from 
filtered_balances

{% endmacro %}

