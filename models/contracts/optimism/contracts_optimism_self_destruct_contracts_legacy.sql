 {{
  config(
	tags=['legacy'],
	
        alias = alias('self_destruct_contracts', legacy_model=True),
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
      , block_number AS created_block_number
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
created_time, created_block_number, creation_tx_hash, contract_address, trace_element
FROM (

  SELECT
  created_time, created_block_number, creation_tx_hash, contract_address, trace_element
      , ROW_NUMBER() OVER (PARTITION BY contract_address ORDER BY created_block_number DESC) as rn
  FROM (
    --self destruct method 1: same tx
    select
      cr.created_time 
      ,cr.created_block_number
      ,cr.creation_tx_hash 
      ,cr.contract_address 
      ,cr.trace_element
    from creates as cr
    join {{ source('optimism', 'traces') }} as sd
      on cr.creation_tx_hash = sd.tx_hash
      and cr.created_time = sd.block_time
      AND cr.created_block_number = sd.block_number
      and cr.trace_element = sd.trace_address[0]
      and sd.`type` = 'suicide'
      {% if is_incremental() %}
      and sd.block_time >= date_trunc('day', now() - interval '1 week')
      and cr.contract_address NOT IN (SELECT contract_address FROM {{this}} ) --ensure no duplicates
      {% endif %}
    group by 1, 2, 3, 4, 5

    UNION ALL

    --self destruct method 2: later tx
    select
      cr.created_time 
      ,cr.created_block_number
      ,cr.creation_tx_hash 
      ,cr.contract_address 
      ,cr.trace_element
    from creates as cr

    JOIN {{ source('optimism', 'traces') }} as sds
      ON cr.contract_address = sds.address
      AND cr.created_time <= sds.block_time
      AND cr.created_block_number <= sds.block_number
      AND sds.type = 'suicide'
      AND sds.address IS NOT NULL
      {% if is_incremental() %}
      and sds.block_time >= date_trunc('day', now() - interval '1 week')
      and cr.contract_address NOT IN (SELECT contract_address FROM {{this}} ) --ensure no duplicates
      {% endif %}
  ) inter

) a 
WHERE rn = 1
