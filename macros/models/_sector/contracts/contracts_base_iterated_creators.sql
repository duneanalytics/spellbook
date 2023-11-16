{% macro contracts_base_iterated_creators( chain ) %}

-- maybe split out contract naming mappings in to a separate thing
-- do token and name mappings at the end

-- set max number of levels to trace root contract, eventually figure out how to make this properly recursive
{% set max_levels = 5 %} --NOTE: If this is too low, this will make the "creator address" not accurate - pivot to use deployer_address if this is too poor.

with base_level AS (
SELECT
  '{{chain}}' AS blockchain
  ,trace_creator_address

  --map special contract creator types here
  ,CASE WHEN nd.creator_address IS NOT NULL THEN s.created_tx_from
    -- --Gnosis Safe Logic
    WHEN aa.contract_project = 'Gnosis Safe' THEN top_level_tx_to --smart wallet
    -- -- AA Wallet Logic
    -- WHEN aa.contract_project = 'ERC4337' THEN ( --smart wallet sender
    --     CASE WHEN bytearray_substring(t.data, 145,18) = 0x000000000000000000000000000000000000 THEN bytearray_substring(t.data, 49,20)
    --     ELSE bytearray_substring(t.data, 145,20) END
    --     )
    -- -- Else
    ELSE trace_creator_address
  END as creator_address

  ,trace_creator_address AS deployer_address -- deployer from the trace - does not iterate up
  ,contract_address
  ,created_time
  ,created_month
  ,created_block_number
  ,created_tx_hash
  ,top_level_time
  ,top_level_block_number
  ,top_level_tx_hash
  ,top_level_tx_from
  ,top_level_tx_to
  ,top_level_tx_method_id
  ,created_tx_from
  ,created_tx_to
  ,created_tx_method_id
  ,created_tx_index
  ,code
  ,code_deploy_rank_by_chain
  ,code_bytelength
      -- used to make sure we don't double map self-destruct contracts that are created multiple times. We'll opt to take the last one
  , ROW_NUMBER() OVER (PARTITION BY contract_address ORDER BY created_block_number DESC, created_tx_index DESC) AS reinit_rank


FROM {{ref('contracts_' + chain + '_base_starting_level') }} s
left join {{ref('contracts_deterministic_contract_creators')}} as nd 
      ON nd.creator_address = s.trace_creator_address
left join {{ ref('evm_smart_account_method_ids') }} aa 
      ON aa.method_id = s.created_tx_method_id
)

, levels as (
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
      ,b.created_month
      ,b.created_block_number
      ,b.created_tx_hash
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
      ,b.code_deploy_rank_by_chain
      ,b.to_iterate_creators --check if base needs to be iterated, keep the base option
      ,b.code

    {% if loop.first -%}
    from base_level as b
    left join base_level as u --get info about the contract that created this contract
      on b.creator_address = u.contract_address
      AND ( b.created_time >= u.created_time OR u.created_time IS NULL) --base level was created on or after its creator
      AND b.blockchain = u.blockchain
      AND u.reinit_rank = 1 --get most recent time the creator contract was created
    {% else -%}
    from level{{i-1}} as b
    left join base_level as u --get info about the contract that created this contract
      on b.creator_address = u.contract_address
      AND ( b.created_time >= u.created_time OR u.created_time IS NULL) --base level was created on or after its creator
      AND b.blockchain = u.blockchain
      AND u.reinit_rank = 1 --get most recent time the creator contract was created
    {% endif %}
    -- is the creator deterministic?
    left join {{ref('contracts_deterministic_contract_creators')}} as nd 
      ON nd.creator_address = b.creator_address

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

  FROM base_level
  WHERE is_new_contract = 1
  )

  , existing_contracts_by_chain AS (
    SELECT
      blockchain
      , code
      , MAX_BY(code_deploy_rank_by_chain, code) AS max_code_deploy_rank_by_chain
    FROM base_level
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
    ,f.created_month
    ,f.created_tx_hash
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
  from levels f
  
  LEFT JOIN code_ranks cr --code ranks for new contracts
    ON cr.blockchain = f.blockchain
    AND cr.contract_address = f.contract_address
    AND cr.created_block_number = f.created_block_number
  
  where f.contract_address is not null
 )

  select 
    blockchain
    ,trace_creator_address
    ,creator_address
    ,deployer_address
    ,u.contract_address
    ,created_time
    ,created_month
    ,'creator contracts' as source
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
    ,token_standard_erc20 --erc20 only - this only exists until we have an ERC20 Tokens table with ALL tokens

    FROM creator_contracts u
    left join (
            -- We have an all NFTs table, but don't yet hand an all ERC20s table
            SELECT contract_address, MIN(evt_block_number) AS min_block_number, 'erc20' as token_standard_erc20
            FROM {{source('erc20_' + chain, 'evt_transfer')}} r
            WHERE 1=1
            AND r.contract_address NOT IN (SELECT contract_address FROM {{ ref('tokens_' + chain + '_erc20')}} )
            group by 1
          ) ts 
  ON u.contract_address = ts.contract_address

  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24
{% endmacro %}