 {{
  config(
        tags = ['dunesql'],
        alias = alias('self_destruct_contracts'),
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key=['blockchain','contract_address'],
        post_hook='{{ expose_spells(\'["ethereum", "optimism", "arbitrum", "avalanche_c", "polygon", "bnb", "gnosis", "fantom", "base", "goerli"]\',
                                    "sector",
                                    "contracts",
                                    \'["msilb7", "chuxin"]\') }}'
  )
}}

{% set evm_chains = all_evm_mainnets_testnets_chains() %} --macro: all_evm_mainnets_testnets_chains.sql



with creates as (
{% for chain in evm_chains %}
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
{% if not loop.last %}
UNION ALL
{% endif %}
{% endfor %}
)

SELECT
blockchain, created_time, created_block_number, creation_tx_hash, contract_address, trace_element
FROM (

  SELECT
  blockchain, created_time, created_block_number, creation_tx_hash, contract_address, trace_element
      , ROW_NUMBER() OVER (PARTITION BY blockchain, contract_address ORDER BY created_block_number DESC) as rn
  FROM (
    {% for chain in evm_chains %}
    --self destruct method 1: same tx
    select
      cr.blockchain
      ,cr.created_time 
      ,cr.created_block_number
      ,cr.creation_tx_hash 
      ,cr.contract_address 
      ,cr.trace_element
    from creates as cr
    join {{ source( chain , 'traces') }} as sd
      on cr.creation_tx_hash = sd.tx_hash
      and cr.created_time = sd.block_time
      AND cr.created_block_number = sd.block_number
      and cr.trace_element = (CASE WHEN cardinality(sd.trace_address) = 0 then cast(-1 as bigint) else sd.trace_address[1] end)
      and sd.type = 'suicide'
      AND cr.blockchain = 'blockchain'
      {% if is_incremental() %}
      and sd.block_time >= date_trunc('day', now() - interval '7' day)
      and cr.contract_address NOT IN (SELECT contract_address FROM {{this}} ) --ensure no duplicates
      {% endif %}

    WHERE cr.blockchain = '{{chain}}'
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
    WHERE cr.blockchain = '{{chain}}'

    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}

  ) inter

) a 
WHERE rn = 1
