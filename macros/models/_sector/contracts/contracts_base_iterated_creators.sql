{% macro contracts_base_iterated_creators( chain ) %}


{% set column_list = [
    'blockchain', 'trace_creator_address', 'creator_address', 'deployer_address'
    ,'contract_address', 'created_time', 'created_month', 'created_block_number', 'created_tx_hash'
    ,'top_level_time', 'top_level_block_number', 'top_level_tx_hash', 'top_level_tx_from', 'top_level_tx_to', 'top_level_tx_method_id'
    ,'created_tx_from', 'created_tx_to', 'created_tx_method_id', 'created_tx_index'
    ,'code', 'code_bytelength', 'token_standard_erc20','code_deploy_rank_by_chain'
    ,'creator_address_lineage', 'tx_method_id_lineage'
  ] %}


-- maybe split out contract naming mappings in to a separate thing
-- do token and name mappings at the end

-- set max number of levels to trace root contract, eventually figure out how to make this properly recursive
{% set max_levels = 5 %} --NOTE: If this is too low, this will make the "creator address" not accurate - pivot to use deployer_address if this is too poor.

with base_level AS (
SELECT *
FROM (
SELECT *
  -- get code deployed rank
  , CASE WHEN is_new_contract = 0
    THEN code_deploy_rank_by_chain_intermediate
    ELSE lag(code_deploy_rank_by_chain_intermediate,1,0) OVER (PARTITION BY code ORDER BY code_deploy_rank_by_chain_intermediate DESC) + code_deploy_rank_by_chain_intermediate
    END AS code_deploy_rank_by_chain
  -- get lineage (or starting lineage)
  , COALESCE(creator_address_lineage_intermediate, ARRAY[creator_address]) AS creator_address_lineage
  , COALESCE(tx_method_id_lineage_intermediate, ARRAY[creator_address]) AS tx_method_id_lineage
  -- used to make sure we don't double map self-destruct contracts that are created multiple times. We'll opt to take the last one
  , ROW_NUMBER() OVER (PARTITION BY contract_address ORDER BY to_iterate_creators DESC, created_block_number DESC, created_tx_index DESC) AS reinit_rank
FROM (
  SELECT
    blockchain
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
    ,code_bytelength
    , NULL AS token_standard_erc20
    , ROW_NUMBER() OVER (PARTITION BY code ORDER BY created_time ASC, created_block_number ASC, created_tx_index ASC) AS code_deploy_rank_by_chain_intermediate
    , ARRAY[cast(NULL as varbinary)] AS creator_address_lineage_intermediate
    , ARRAY[cast(NULL as varbinary)] AS tx_method_id_lineage_intermediate
    , 1 AS to_iterate_creators
    , 1 AS is_new_contract

  FROM {{ref('contracts_' + chain + '_base_starting_level') }} s
  left join {{ref('contracts_deterministic_contract_creators')}} as nd 
        ON nd.creator_address = s.trace_creator_address
  left join (
            SELECT method_id, contract_project
            FROM {{ ref('base_evm_smart_account_method_ids') }}
            GROUP BY 1,2
          ) aa 
        ON aa.method_id = s.created_tx_method_id
  WHERE 
      1=1
      {% if is_incremental() %}
      AND {{ incremental_predicate('s.created_time') }}
      {% endif %}
  
  {% if is_incremental() %}

  UNION ALL

  SELECT
    blockchain
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
    ,code_bytelength
    , token_standard_erc20
    , code_deploy_rank_by_chain AS code_deploy_rank_by_chain_intermediate
    , creator_address_lineage AS creator_address_lineage_intermediate
    , tx_method_id_lineage AS tx_method_id_lineage_intermediate
    , CASE
        WHEN contains(creator_address_lineage, (SELECT creator_address FROM {{ref('contracts_deterministic_contract_creators')}} ) ) THEN 1--check deterministic creators
        WHEN contains(tx_method_id_lineage, (SELECT method_id FROM {{ref('base_evm_smart_account_method_ids')}} ) )
              -- AND (How do we know if this method_id needs to be remapped? Until then re-map everything)
              THEN 1 -- array contains smart account and creator = trace creator
        ELSE 0 END
      END AS to_iterate_creators
    , 0 AS is_new_contract

  FROM {{ this }} s
  left join {{ref('contracts_deterministic_contract_creators')}} as nd 
        ON nd.creator_address = s.trace_creator_address
  left join (
            SELECT method_id, contract_project
            FROM {{ ref('base_evm_smart_account_method_ids') }}
            GROUP BY 1,2
          ) aa 
        ON aa.method_id = s.created_tx_method_id
  WHERE 
      1=1
      AND (NOT {{ incremental_predicate('s.created_time') }} ) --don't pick up incrementals

  {% endif %}

) base
) filtered
WHERE reinit_rank = 1
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
      ,b.code
      ,b.token_standard_erc20
      , CASE WHEN u.creator_address IS NOT NULL THEN 
            b.creator_address_lineage || ARRAY[u.creator_address]
          ELSE b.creator_address_lineage
        END AS creator_address_lineage
      , CASE WHEN u.created_tx_method_id IS NOT NULL THEN 
            b.tx_method_id_lineage || ARRAY[u.created_tx_method_id]
          ELSE b.tx_method_id_lineage
        END AS tx_method_id_lineage
      , b.to_iterate_creators
      , b.is_new_contract

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
    WHERE b.to_iterate_creators = 1
)
{%- endfor %}


SELECT {{ column_list | join(', ') }}  FROM base_level WHERE to_iterate_creators = 0
UNION ALL
SELECT {{ column_list | join(', ') }}  FROM level{{max_levels - 1}}

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
    ,creator_address_lineage
    ,tx_method_id_lineage
    ,COALESCE(u.token_standard_erc20,ts.token_standard_erc20) AS token_standard_erc20 --erc20 only - this only exists until we have an ERC20 Tokens table with ALL tokens

    FROM levels u
    left join (
            -- We have an all NFTs table, but don't yet hand an all ERC20s table
            SELECT contract_address, MIN(evt_block_number) AS min_block_number, 'erc20' as token_standard_erc20
            FROM {{source('erc20_' + chain, 'evt_transfer')}} r
            WHERE 1=1
            AND r.contract_address NOT IN (SELECT contract_address FROM {{ ref('tokens_' + chain + '_erc20')}} )
            {% if is_incremental() %}
              AND {{ incremental_predicate('r.evt_block_time') }}
            {% endif %}
            group by 1
          ) ts 
  ON u.contract_address = ts.contract_address

  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26
{% endmacro %}