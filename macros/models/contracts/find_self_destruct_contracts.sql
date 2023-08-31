{% macro find_self_destruct_contracts_by_chain( chain ) %}

with creates as (

    select 
      '{{chain}}' as blockchain
      , block_time as created_time
      , block_number AS created_block_number
      ,tx_hash as creation_tx_hash
      ,address as contract_address
      ,(CASE WHEN cardinality(trace_address) = 0 then cast(-1 as bigint) else trace_address[1] end) as trace_element
    from {{ source(chain , 'traces') }}
    where 
      type = 'create'
      and success
      and tx_success
      {% if is_incremental() %}
      and block_time >= date_trunc('day', now() - interval '7' day)
      {% endif %}

)

SELECT
blockchain, created_time, created_block_number, creation_tx_hash, contract_address, trace_element
  ,destructed_time, destructed_block_number, destructed_tx_hash
FROM (

  SELECT
  blockchain, created_time, created_block_number, creation_tx_hash, contract_address, trace_element
  ,destructed_time, destructed_block_number, destructed_tx_hash
  , ROW_NUMBER() OVER (PARTITION BY blockchain, contract_address ORDER BY created_block_number DESC) as rn
  FROM (

    --self destruct method 1: same tx
    select
      cr.blockchain
      ,cr.created_time 
      ,cr.created_block_number
      ,cr.creation_tx_hash 
      ,cr.contract_address 
      ,cr.trace_element
      ,sd.block_time as destructed_time
      ,sd.block_number as destructed_block_number
      ,sd.tx_hash as destructed_tx_hash 
    from creates as cr
    join {{ source( chain , 'traces') }} as sd
      on cr.creation_tx_hash = sd.tx_hash
      and cr.created_time = sd.block_time
      AND cr.created_block_number = sd.block_number
      and cr.trace_element = (CASE WHEN cardinality(sd.trace_address) = 0 then cast(-1 as bigint) else sd.trace_address[1] end)
      and sd.type = 'suicide'
      AND cr.blockchain = '{{chain}}'
      {% if is_incremental() %}
      and sd.block_time >= date_trunc('day', now() - interval '7' day)
      and cr.contract_address NOT IN (SELECT contract_address FROM {{this}} ) --ensure no duplicates
      {% endif %}

    -- WHERE cr.blockchain = '{{chain}}'
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9

    UNION ALL

    --self destruct method 2: later tx
    select
      cr.blockchain
      , cr.created_time 
      ,cr.created_block_number
      ,cr.creation_tx_hash 
      ,cr.contract_address 
      ,cr.trace_element
      ,sds.block_time as destructed_time
      ,sds.block_number as destructed_block_number
      ,sds.tx_hash as destructed_tx_hash 
    from creates as cr

    JOIN {{ source( chain , 'traces') }} as sds
      ON cr.contract_address = sds.address
      AND cr.created_time <= sds.block_time
      AND cr.created_block_number <= sds.block_number
      AND sds.type = 'suicide'
      AND sds.address IS NOT NULL
      AND cr.blockchain = '{{chain}}'
      {% if is_incremental() %}
      and sds.block_time >= date_trunc('day', now() - interval '7' day)
      and cr.contract_address NOT IN (SELECT contract_address FROM {{this}} th WHERE th.blockchain = '{{chain}}' ) --ensure no duplicates
      {% endif %}
    -- WHERE cr.blockchain = '{{chain}}'
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9

  ) inter

) a 
WHERE rn = 1

{% endmacro %}