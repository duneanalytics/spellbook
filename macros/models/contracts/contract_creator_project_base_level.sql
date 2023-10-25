{% macro contract_creator_project_base_level( chain ) %}


SELECT 
         blockchain
        ,trace_creator_address
        ,creator_address
        ,deployer_address
        ,contract_address
        
        ,created_time
        ,created_block_number
        ,creation_tx_hash
        ,created_tx_from
        ,created_tx_to
        ,created_tx_method_id
        ,created_tx_index

        ,top_level_time
        ,top_level_block_number
        ,top_level_tx_hash
        ,top_level_tx_from
        ,top_level_tx_to
        ,top_level_tx_method_id

        ,code_bytelength
        ,code_deploy_rank_by_chain
        ,to_iterate_creators
        ,code
        
        ,is_new_contract
  FROM (
  select 
    blockchain
    ,trace_creator_address -- get the original contract creator address
    ,creator_address --top level creator, where we iterate up through factories
    ,COALESCE(deployer_address --if already mapped, pull deployer. else this is the intermediate creator
            , creator_address) as deployer_address -- deployer from the trace - does not iterate up
    ,contract_address

    ,created_time
    ,created_block_number
    ,creation_tx_hash
    ,created_tx_from
    ,created_tx_to
    ,created_tx_method_id
    ,created_tx_index

    ,top_level_time
    ,top_level_block_number
    ,top_level_tx_hash
    ,top_level_tx_from
    ,top_level_tx_to
    ,top_level_tx_method_id

    ,code_bytelength
    ,code_deploy_rank_by_chain
    ,to_iterate_creators
    ,code
    
    ,is_new_contract
    ,ROW_NUMBER() OVER (PARTITION BY contract_address ORDER BY created_block_number ASC, is_new_contract DESC ) AS contract_order -- to ensure no dupes

  from (
    WITH incremental_contracts AS (
        select 
            '{{chain}}' AS blockchain
            ,ct."from" as trace_creator_address
            ,CASE WHEN ct."from" IN (SELECT creator_address from {{ref('contracts_deterministic_contract_creators')}} ) THEN t."from" --tx sender
              WHEN aa.contract_project = 'Gnosis Safe' THEN t.to --smart wallet
              WHEN aa.contract_project = 'ERC4337' THEN ( --smart wallet sender
                  CASE WHEN bytearray_substring(t.data, 145,18) = 0x000000000000000000000000000000000000 THEN bytearray_substring(t.data, 49,20)
                  ELSE bytearray_substring(t.data, 145,20) END
                  )
              ELSE ct."from"
            END as creator_address
            ,CAST(NULL AS varbinary) as deployer_address -- deployer from the trace - does not iterate up
            ,ct.address as contract_address
            ,ct.block_time as created_time
            ,ct.block_number as created_block_number
            ,ct.tx_hash as creation_tx_hash
            ,t.block_time as top_level_time
            ,t.block_number as top_level_block_number
            ,t.hash as top_level_tx_hash
            ,t."from" AS top_level_tx_from
            ,t.to AS top_level_tx_to
            ,bytearray_substring(t.data,1,4) AS top_level_tx_method_id
            ,t."from" AS created_tx_from
            ,t.to AS created_tx_to
            ,bytearray_substring(t.data,1,4) AS created_tx_method_id
            ,t.index as created_tx_index
            ,ct.code
            ,CAST(NULL AS bigint) as code_deploy_rank_by_chain
            ,bytearray_length(ct.code) AS code_bytelength
            ,1 AS to_iterate_creators
            ,1 AS is_new_contract
          from {{ source( chain , 'transactions') }} as t 
          inner join  {{ source( chain , 'creation_traces') }} as ct 
            ON t.hash = ct.tx_hash
            AND t.block_time = ct.block_time
            AND t.block_number = ct.block_number
            {% if is_incremental() %}
            and t.block_time >= date_trunc('day', now() - interval '7' day)
            AND ct.block_time >= date_trunc('day', now() - interval '7' day)
            {% endif %}
          left join {{ ref('contracts_'+ chain +'_find_self_destruct_contracts') }} as sd 
            on ct.address = sd.contract_address
            and ct.tx_hash = sd.creation_tx_hash
            and ct.block_time = sd.created_time
            AND sd.blockchain = '{{chain}}'
            {% if is_incremental() %}
            and sd.created_time >= date_trunc('day', now() - interval '7' day)
            {% endif %}
          left join {{ ref('evm_smart_account_method_ids') }} aa 
            ON aa.method_id = bytearray_substring(t.data,1,4)
          where 
            1=1
            {% if is_incremental() %}
            and ct.block_time >= date_trunc('day', now() - interval '7' day)
            {% endif %} -- incremental filter
      )
    SELECT * FROM incremental_contracts

    {% if is_incremental() %}
    -- to get existing history of contract mapping / only select those we want to re-run
    union all 

    select 
      t.blockchain
      ,t.trace_creator_address
      ,t.creator_address
      ,t.deployer_address
      ,t.contract_address
      ,t.created_time
      ,t.created_block_number
      ,t.creation_tx_hash
      -- If the creator becomes marked as deterministic, we want to re-map
      ,CASE WHEN nd.creator_address IS NOT NULL THEN t.created_time
        ELSE t.top_level_time END AS top_level_time

      ,CASE WHEN nd.creator_address IS NOT NULL THEN t.created_block_number
        ELSE t.top_level_block_number END AS top_level_block_number

      ,CASE WHEN nd.creator_address IS NOT NULL THEN t.creation_tx_hash
        ELSE t.top_level_tx_hash END AS top_level_tx_hash

      ,CASE WHEN nd.creator_address IS NOT NULL THEN created_tx_from
        ELSE t.top_level_tx_from END AS top_level_tx_from

      ,CASE WHEN nd.creator_address IS NOT NULL THEN created_tx_to
        ELSE t.top_level_tx_to END AS top_level_tx_to

      ,CASE WHEN nd.creator_address IS NOT NULL THEN created_tx_method_id
        ELSE t.top_level_tx_method_id END AS top_level_tx_method_id
      ---
      ,t.created_tx_from
      ,t.created_tx_to
      ,t.created_tx_method_id
      ,t.created_tx_index
      ,t.code
      ,t.code_deploy_rank_by_chain
      ,t.code_bytelength
      , CASE
        WHEN nd.creator_address IS NOT NULL THEN 1
        ELSE 0 END AS to_iterate_creators
      , 0 AS is_new_contract
    from {{ this }} t

    -- If the creator becomes marked as deterministic, we want to re-run it.
    left join {{ref('contracts_deterministic_contract_creators')}} as nd 
      ON nd.creator_address = t.creator_address

    
    WHERE t.blockchain = '{{chain}}'

    {% endif %} -- incremental filter

  ) as x
  group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23
) y 
--Don't run the same contract twice (i.e. incremental and existing)
WHERE contract_order = 1


{% endmacro %}