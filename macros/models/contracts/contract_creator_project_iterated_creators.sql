{% macro contract_creator_project_iterated_creators( chain ) %}

-- maybe split out contract naming mappings in to a separate thing
-- do token and name mappings at the end

-- set max number of levels to trace root contract, eventually figure out how to make this properly recursive
{% set max_levels = 3 %} --NOTE: This will make the "creator address" not accurate, if the levels are too low - pivot to use deployer_address


WITH levels as (
-- starting from 0 
-- u = next level up contract (i.e. the factory)
-- b = base-level contract
{% for i in range(max_levels) -%}

{% if i == 0 %}
with level0
{% else %}
,level{{i}}
{% endif %}
  as (
    select
      {{i}} as level 
      ,b.blockchain
      ,b.trace_creator_address -- get the original contract creator address
      ,case when nd.creator_address IS NOT NULL
        THEN b.created_tx_from --when deterministic creator, we take the tx sender
        ELSE coalesce(u.creator_address, b.creator_address)
      END as creator_address -- get the highest-level creator we know of
      ,b.deployer_address
      ,b.contract_address
      -- store the raw created data
      ,b.created_time
      ,b.created_block_number
      ,b.creation_tx_hash
      ,b.created_tx_from
      ,b.created_tx_to
      ,b.created_tx_method_id
      ,b.created_tx_index

      -- when deterministic, pull the tx-level data
      ,case when nd.creator_address IS NOT NULL
        then b.top_level_time ELSE COALESCE(u.top_level_time, b.top_level_time ) END AS top_level_time
      ,case when nd.creator_address IS NOT NULL
        then b.top_level_block_number else COALESCE(u.top_level_block_number, b.top_level_block_number ) end AS top_level_block_number
      ,case when nd.creator_address IS NOT NULL
        then b.top_level_tx_hash else COALESCE(u.top_level_tx_hash, b.top_level_tx_hash ) end AS top_level_tx_hash
      ,case when nd.creator_address IS NOT NULL
        then b.created_tx_from ELSE COALESCE(u.created_tx_from, b.created_tx_from ) END AS top_level_tx_from
      ,case when nd.creator_address IS NOT NULL
        then b.created_tx_to else COALESCE(u.created_tx_to, b.created_tx_to ) end AS top_level_tx_to
      ,case when nd.creator_address IS NOT NULL
        then b.created_tx_method_id else COALESCE(u.created_tx_method_id, b.created_tx_method_id ) end AS top_level_tx_method_id

      ,b.code_bytelength
      ,b.token_standard
      ,b.code_deploy_rank_by_chain
      ,b.to_iterate_creators --check if base needs to be iterated, keep the base option
      ,b.code
      ,b.is_new_contract
      ,b.contract_order

    {% if loop.first -%}
    from {{ref('contracts_' + chain + '_contract_creator_project_base_level') }} as b
    left join {{ref('contracts_' + chain + '_contract_creator_project_base_level') }} as u --get info about the contract that created this contract
      on b.creator_address = u.contract_address
      AND ( b.created_time >= u.created_time OR u.created_time IS NULL) --base level was created on or after its creator
      AND b.blockchain = u.blockchain
    {% else -%}
    from level{{i-1}} as b
    left join {{ref('contracts_' + chain + '_contract_creator_project_base_level') }} as u --get info about the contract that created this contract
      on b.creator_address = u.contract_address
      AND ( b.created_time >= u.created_time OR u.created_time IS NULL) --base level was created on or after its creator
      AND b.blockchain = u.blockchain
    {% endif %}
    -- is the creator deterministic?
    left join {{ref('contracts_deterministic_contract_creators')}} as nd 
      ON nd.creator_address = b.creator_address
    
    WHERE b.to_iterate_creators=1 --only run contracts that we want to iterate through
)
{%- endfor %}

SELECT * FROM level{{max_levels - 1}}

)

, code_ranks AS ( --generate code deploy rank without ranking over all prior contracts (except for initial builds)
  WITH new_contracts AS (
  SELECT
    blockchain
    , contract_address
    , created_block_number
    , code
    , ROW_NUMBER() OVER (PARTITION BY code ORDER BY created_time ASC) AS code_deploy_rank_by_chain_intermediate

  FROM {{ref('contracts_' + chain + '_contract_creator_project_base_level') }}
  WHERE is_new_contract = 1
  )

  , existing_contracts_by_chain AS (
    SELECT
      blockchain
      , code
      , MAX_BY(code_deploy_rank_by_chain, code) AS max_code_deploy_rank_by_chain
    FROM {{ref('contracts_' + chain + '_contract_creator_project_base_level') }}
    WHERE is_new_contract = 0 AND code IS NOT NULL
    AND code IN (SELECT code from new_contracts)
    GROUP BY 1,2
  )


  SELECT 
  nc.blockchain, nc.contract_address, nc.code, nc.created_block_number
    , COALESCE(cbc.max_code_deploy_rank_by_chain,0) + nc.code_deploy_rank_by_chain_intermediate AS code_deploy_rank_by_chain
  FROM new_contracts nc 
    LEFT JOIN existing_contracts_by_chain cbc
      ON cbc.code = nc.code
      AND cbc.blockchain = nc.blockchain

)

,creator_contracts as (
  select 
    f.blockchain
    ,f.trace_creator_address
    ,f.creator_address
    ,f.deployer_address
    ,f.contract_address
    ,f.created_time
    ,f.creation_tx_hash
    ,f.created_block_number
    ,f.top_level_time
    ,f.top_level_tx_hash
    ,f.top_level_block_number
    ,f.top_level_tx_from
    ,f.top_level_tx_to
    ,f.top_level_tx_method_id
    ,f.created_tx_from
    ,f.created_tx_to
    ,f.created_tx_method_id
    ,f.created_tx_index
    ,f.code_bytelength
    ,f.code
    ,COALESCE(f.code_deploy_rank_by_chain, cr.code_deploy_rank_by_chain) AS code_deploy_rank_by_chain
  from (
    SELECT * FROM levels WHERE to_iterate_creators = 1 --get mapped contracts
    UNION ALL
    SELECT {{max_levels}} as level, * FROM {{ref('contracts_' + chain + '_contract_creator_project_base_level') }} WHERE to_iterate_creators = 0 --get legacy contracts
  ) f
  
  LEFT JOIN code_ranks cr --code ranks for new contracts
    ON cr.blockchain = f.blockchain
    AND cr.contract_address = f.contract_address
    AND cr.created_block_number = f.created_block_number
  
  where f.contract_address is not null
 )

  select 
    cc.blockchain
    ,cc.trace_creator_address
    ,cc.creator_address
    ,cc.deployer_address
    ,cc.contract_address
    ,cc.created_time
    ,'creator contracts' as source
    ,cc.top_level_time
    ,cc.creation_tx_hash
    ,cc.created_block_number
    ,cc.top_level_tx_hash
    ,cc.top_level_block_number
    ,cc.top_level_tx_from
    ,cc.top_level_tx_to
    ,cc.top_level_tx_method_id
    ,cc.created_tx_from
    ,cc.created_tx_to
    ,cc.created_tx_method_id
    ,cc.created_tx_index
    ,cc.code_bytelength
    ,cc.code_deploy_rank_by_chain
    ,cc.code
  from creator_contracts as cc 
  left join {{ source( chain , 'contracts') }} as oc 
    on cc.contract_address = oc.address 
  WHERE cc.blockchain = '{{chain}}'
  group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22

{% endmacro %}