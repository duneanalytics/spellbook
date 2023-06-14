 {{
  config(
        alias='self_destruct_contracts',
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key='contract_address',
        post_hook='{{ expose_spells(\'["optimism"]\',
                                    "sector",
                                    "contracts",
                                    \'["msilb7", "chuxin"]\') }}'
  )
}}

with creates as (
    select 
      block_time as created_time
      ,tx_hash as creation_tx_hash
      ,address as contract_address
      ,trace_address[0] as trace_element
    from {{ source('optimism', 'traces') }}
    where 
      type = 'create'
      and success
      and tx_success
      {% if is_incremental() %}
      and block_time >= date_trunc('day', now() - interval '1 week')
      {% endif %}
)

SELECT
created_time, creation_tx_hash, contract_address, trace_element
FROM (
  select
    cr.created_time 
    ,cr.creation_tx_hash 
    ,cr.contract_address 
    ,cr.trace_element
    , ROW_NUMBER() OVER (PARTITION BY cr.contract_address ORDER BY created_time ASC) as rn
  from creates as cr
  join {{ source('optimism', 'traces') }} as sd
    on cr.creation_tx_hash = sd.tx_hash
    and cr.created_time = sd.block_time
    and cr.trace_element = sd.trace_address[0]
    and sd.`type` = 'suicide'
    {% if is_incremental() %}
    and sd.block_time >= date_trunc('day', now() - interval '1 week')
    and cr.contract_address NOT IN (SELECT contract_address FROM {{this}} ) --ensure no duplicates
    {% endif %}
  group by 1, 2, 3, 4
) a 
WHERE rn = 1
;
