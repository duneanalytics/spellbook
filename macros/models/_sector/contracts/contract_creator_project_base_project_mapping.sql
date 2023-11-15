{% macro contract_creator_project_base_project_mapping( chain ) %}

WITH unified_contract_sources AS (

  select 
    blockchain
    ,trace_creator_address
    ,ic.creator_address
    ,deployer_address
    ,contract_address
    ,cast(NULL as varchar) AS contract_project --to be filled later
    ,cast(NULL as varchar) contract_name --to be filled later
    ,created_time
    ,created_month
    ,source
    ,top_level_time
    ,created_tx_hash
    ,created_block_number
    ,top_level_tx_hash
    ,top_level_block_number
    ,top_level_tx_from
    ,top_level_tx_to
    ,top_level_tx_method_id
    ,created_tx_from
    ,created_tx_to
    ,created_tx_method_id
    ,created_tx_index
    ,code_bytelength
    ,code_deploy_rank_by_chain
    ,ic.code
    ,1 as map_rank
  from {{ref('contracts_' + chain + '_contract_creator_project_iterated_creators') }} ic

---
-- predeploys
---
  union all
  
  select 
      blockchain
      ,trace_creator_address
      ,creator_address
      ,creator_address AS deployer_address
      ,contract_address
      ,contract_project
      ,contract_name
      ,created_time
      ,cast( DATE_TRUNC('month',created_time) as date) AS created_month
      ,source
      ,created_time as top_level_time
      ,CAST(NULL AS varbinary) as top_level_tx_hash
      ,cast(NULL as bigint) as top_level_block_number
      ,CAST(NULL AS varbinary) as created_tx_hash
      ,cast(NULL as bigint) as created_block_number
      ,CAST(NULL AS varbinary) as top_level_tx_from
      ,CAST(NULL AS varbinary) as top_level_tx_to
      ,CAST(NULL AS varbinary) as top_level_tx_method_id
      ,CAST(NULL AS varbinary) as created_tx_from
      ,CAST(NULL AS varbinary) as created_tx_to
      ,CAST(NULL AS varbinary) as created_tx_method_id
      ,cast(NULL as bigint) AS created_tx_index
      ,cast(NULL as bigint) as code_bytelength --todo
      ,1 as code_deploy_rank_by_chain
      ,CAST(NULL AS varbinary) AS code
      ,2 as map_rank

    FROM {{ ref('contracts_predeploys') }} pre
    where 
    1=1
    and pre.blockchain = '{{chain}}'
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11

  -- ---
  -- -- missing contracts
  -- ---

  union all
  
  select 
    '{{chain}}' AS blockchain
    ,COALESCE(ct."from",oc."from",0xdeaddeaddeaddeaddeaddeaddeaddeaddead0006) AS trace_creator_address
    ,COALESCE(ct."from",oc."from",0xdeaddeaddeaddeaddeaddeaddeaddeaddead0006) AS creator_address
    ,COALESCE(ct."from",oc."from",0xdeaddeaddeaddeaddeaddeaddeaddeaddead0006) AS deployer_address
    ,l.contract_address
    ,oc.namespace as contract_project 
    ,oc.name as contract_name 
    ,COALESCE(ct.block_time, oc.created_at, MIN(l.block_time)) AS created_time
    ,cast( DATE_TRUNC('month',COALESCE(ct.block_time, oc.created_at, MIN(l.block_time)) ) as date) AS created_month
    ,'missing contracts' as source
    ,COALESCE(ct.block_time, oc.created_at, MIN(l.block_time)) as top_level_time
    ,CAST(NULL AS varbinary) as top_level_tx_hash
    ,cast(NULL as bigint) as top_level_block_number
    ,CAST(NULL AS varbinary) as created_tx_hash
    ,cast(NULL as bigint) as created_block_number
    ,CAST(NULL AS varbinary) as top_level_tx_from
    ,CAST(NULL AS varbinary) as top_level_tx_to
    ,CAST(NULL AS varbinary) as top_level_tx_method_id
    ,CAST(NULL AS varbinary) as created_tx_from
    ,CAST(NULL AS varbinary) as created_tx_to
    ,CAST(NULL AS varbinary) as created_tx_method_id
    ,l.tx_index AS created_tx_index
    ,bytearray_length(oc.code) as code_bytelength
    ,1 as code_deploy_rank_by_chain
    ,oc.code
    ,3 as map_rank
  from {{ source( chain , 'logs') }} as l
    left join {{ source( chain , 'creation_traces') }} as ct 
      ON l.contract_address = ct.address
    left join {{ source( chain , 'contracts') }} as oc 
      ON l.contract_address = oc.address
    
    {% if is_incremental() %} -- this filter will only be applied on an incremental run 
      AND {{ incremental_predicate('l.block_time') }}
    {% endif %}

  GROUP BY ct."from",oc."from", l.contract_address, oc.namespace, oc.name, ct.block_time, oc.created_at, l.tx_index, oc.code
  
)

SELECT
    blockchain
    ,trace_creator_address
    ,creator_address
    ,deployer_address
    ,u.contract_address
    ,contract_project
    ,contract_name
    ,created_time
    ,created_month
    ,source
    ,top_level_time
    ,created_tx_hash
    ,created_block_number
    ,top_level_tx_hash
    ,top_level_block_number
    ,top_level_tx_from
    ,top_level_tx_to
    ,top_level_tx_method_id
    ,created_tx_from
    ,created_tx_to
    ,created_tx_method_id
    ,created_tx_index
    ,code_bytelength
    ,code_deploy_rank_by_chain
    ,code
    ,token_standard --erc20 only - this only exists until we have an ERC20 Tokens table with ALL tokens
    ,map_rank
FROM unified_contract_sources u 
left join (
            -- We have an all NFTs table, but don't yet hand an all ERC20s table
            SELECT contract_address, MIN(evt_block_number) AS min_block_number, 'erc20' as token_standard
            FROM {{source('erc20_' + chain, 'evt_transfer')}} r
            WHERE 1=1
            AND r.contract_address NOT IN (SELECT contract_address FROM {{ ref('tokens_' + chain + '_erc20')}} )
            group by 1
          ) ts 
  ON u.contract_address = ts.contract_address

{% endmacro %}