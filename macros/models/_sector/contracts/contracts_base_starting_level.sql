{% macro contracts_base_starting_level( chain ) %}


SELECT 
         blockchain
        ,trace_creator_address
        ,creator_address
        ,deployer_address
        ,contract_address
        
        ,created_time
        ,cast( DATE_TRUNC('month',created_time) as date) AS created_month
        ,created_block_number
        ,created_tx_hash
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

        ,reinit_rank
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
    ,created_tx_hash
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

    -- used to make sure we don't double map self-destruct contracts that are created multiple times. We'll opt to take the last one
    , ROW_NUMBER() OVER (PARTITION BY contract_address ORDER BY created_block_number DESC, created_block_number DESC) AS reinit_rank

  from (

        select 
            '{{chain}}' AS blockchain
            ,ct."from" as trace_creator_address
            ,CASE WHEN ct."from" IN (SELECT creator_address from {{ref('contracts_deterministic_contract_creators')}} ) THEN t."from" --tx sender
              -- --Gnosis Safe Logic
              WHEN aa.contract_project = 'Gnosis Safe' THEN t.to --smart wallet
              -- -- AA Wallet Logic
              -- WHEN aa.contract_project = 'ERC4337' THEN ( --smart wallet sender
              --     CASE WHEN bytearray_substring(t.data, 145,18) = 0x000000000000000000000000000000000000 THEN bytearray_substring(t.data, 49,20)
              --     ELSE bytearray_substring(t.data, 145,20) END
              --     )
              -- -- Else
              ELSE ct."from"
            END as creator_address
            ,CAST(NULL AS varbinary) as deployer_address -- deployer from the trace - does not iterate up
            ,ct.address as contract_address
            ,ct.block_time as created_time
            ,ct.block_number as created_block_number
            ,ct.tx_hash as created_tx_hash
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

          left join {{ ref('evm_smart_account_method_ids') }} aa 
            ON aa.method_id = bytearray_substring(t.data,1,4)
          where 
            1=1

  ) as x
  group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23
) y 



{% endmacro %}