{% macro contracts_base_iterated_creators( chain, standard_name = 'erc', days_forward=365 ) %}


{% set column_list = [
     'blockchain', 'trace_creator_address', 'creator_address', 'trace_deployer_address'
    ,'contract_address', 'created_time', 'created_month', 'created_block_number', 'created_tx_hash'
    ,'top_level_contract_address','top_level_time', 'top_level_block_number', 'top_level_tx_hash', 'top_level_tx_from', 'top_level_tx_to', 'top_level_tx_method_id'
    ,'created_tx_from', 'created_tx_to', 'created_tx_method_id', 'created_tx_index'
    ,'code', 'code_bytelength', 'token_standard_erc20','code_deploy_rank_by_chain', 'is_self_destruct'
    ,'creator_address_lineage', 'tx_method_id_lineage'
  ] %}

-- maybe split out contract naming mappings in to a separate thing
-- do token and name mappings at the end

-- set max number of levels to trace root contract, eventually figure out how to make this properly recursive
{% set max_levels = 5 %} --NOTE: If this is too low, this will make the "creator address" not accurate - pivot to use trace_deployer_address if this is too poor.

WITH check_date AS (
  SELECT
  {% if is_incremental() %}
    MAX(created_time) AS base_time FROM {{this}}
  {% else %}
    MIN(time) AS base_time FROM {{ source( chain , 'blocks') }}
  {% endif %}
)

, deterministic_deployers AS (
    SELECT array_agg(creator_address) AS creator_address_array FROM {{ref('contracts_deterministic_contract_creators')}}
    )

, smart_account_methods AS (
    SELECT array_agg(method_id) AS method_id_array FROM {{ref('base_evm_smart_account_method_ids')}}
    )

, levels as (

  with base_level AS (
  SELECT b.*
    --map special contract creator types here
      ,CASE WHEN nd.creator_address IS NOT NULL THEN b.created_tx_from
        -- --Gnosis Safe Logic
        WHEN aa.contract_project = 'Gnosis Safe' THEN b.top_level_tx_to --smart wallet
        -- -- AA Wallet Logic - Commented out until we figure it out - this logic is wrong
        -- WHEN aa.contract_project = 'ERC4337' THEN ( --smart wallet sender
        --     CASE WHEN bytearray_substring(t.data, 145,18) = 0x000000000000000000000000000000000000 THEN bytearray_substring(t.data, 49,20)
        --     ELSE bytearray_substring(t.data, 145,20) END
        --     )
        -- -- Else
        ELSE creator_address_intermediate
      END as creator_address

    -- get code deployed rank
    , CASE WHEN is_new_contract = 0
        THEN code_deploy_rank_by_chain_intermediate
        ELSE lag(code_deploy_rank_by_chain_intermediate,1,0) OVER (PARTITION BY code ORDER BY code_deploy_rank_by_chain_intermediate DESC) + code_deploy_rank_by_chain_intermediate
      END AS code_deploy_rank_by_chain

    ,CASE WHEN sd.contract_address IS NOT NULL THEN true ELSE false END as is_self_destruct
    -- get lineage (or starting lineage)
    , creator_address_lineage_intermediate AS creator_address_lineage
    , tx_method_id_lineage_intermediate AS tx_method_id_lineage
    -- used to make sure we don't double map self-destruct contracts that are created multiple times. We'll opt to take the last one
    
  FROM (
    WITH new_contracts AS (
      SELECT
        blockchain
        ,trace_creator_address
        ,trace_creator_address AS creator_address_intermediate
        ,trace_creator_address AS trace_deployer_address -- deployer from the trace - does not iterate up
        ,contract_address
        ,created_time
        ,created_month
        ,created_block_number
        ,created_tx_hash
        ,contract_address AS top_level_contract_address
        ,created_time AS top_level_time
        ,created_block_number AS top_level_block_number
        ,created_tx_hash AS top_level_tx_hash
        ,created_tx_from as top_level_tx_from
        ,created_tx_to AS top_level_tx_to
        ,created_tx_method_id AS top_level_tx_method_id
        ,created_tx_from
        ,created_tx_to
        ,created_tx_method_id
        ,created_tx_index
        ,code
        ,code_bytelength
        , NULL AS token_standard_erc20
        , ROW_NUMBER() OVER (PARTITION BY code ORDER BY created_block_number ASC, created_tx_index ASC) AS code_deploy_rank_by_chain_intermediate
        , ARRAY[trace_creator_address] AS creator_address_lineage_intermediate
        , ARRAY[created_tx_method_id] AS tx_method_id_lineage_intermediate
        , 1 AS is_new_contract

      FROM {{ref('contracts_' + chain + '_base_starting_level') }} s, check_date cd
      WHERE 
          1=1

          AND {{ incremental_days_forward_predicate('s.created_time', 'cd.base_time', days_forward ) }}

    )

    {% if is_incremental() %}
    -- pre-generate the list of contracts we need to pull to help speed up the process
    
    -- Logic checked here: https://dune.com/queries/3210612
    , inc_contracts AS (
      SELECT contract_address
      FROM (
        ---- Select creators in this iteration
        SELECT nc.creator_address_intermediate as contract_address FROM new_contracts nc

        UNION --this was faster than union all'ing distincts
        ---- Select addresses from creator_address_lineage where contract_address matches creator_address in new_contracts
        SELECT lineage_address
        FROM {{this}} s
        CROSS JOIN UNNEST(s.creator_address_lineage) AS t(lineage_address)
        JOIN new_contracts nc ON s.contract_address = nc.creator_address_intermediate

        -- We don't need to select creators from prior iterations because we well reinitialize
        -- the incremental build on any updates to deterministic deployer & smart account methods
        -- Keeping the raw code below (commented out) in case we do need this however.

      ) a
      WHERE contract_address IS NOT NULL
    )
    {% endif %}

    SELECT * FROM new_contracts
    {% if is_incremental() %}

    UNION ALL

    SELECT
      s.blockchain
      ,s.trace_creator_address
      ,s.creator_address AS creator_address_intermediate
      ,s.trace_deployer_address AS trace_deployer_address
      ,s.contract_address
      ,s.created_time
      ,s.created_month
      ,s.created_block_number
      ,s.created_tx_hash
      ,s.top_level_contract_address
      ,s.top_level_time
      ,s.top_level_block_number
      ,s.top_level_tx_hash
      ,s.top_level_tx_from
      ,s.top_level_tx_to
      ,s.top_level_tx_method_id
      ,s.created_tx_from
      ,s.created_tx_to
      ,s.created_tx_method_id
      ,s.created_tx_index
      ,s.code
      ,s.code_bytelength
      ,s.token_standard_erc20
      ,s.code_deploy_rank_by_chain AS code_deploy_rank_by_chain_intermediate
      ,s.creator_address_lineage AS creator_address_lineage_intermediate
      ,s.tx_method_id_lineage AS tx_method_id_lineage_intermediate
      , 0 AS is_new_contract -- since we rebuild initial on static ref updates, we don't need to iterate on this.

    FROM {{ this }} s, check_date cd
    WHERE 
        1=1
        AND (NOT {{ incremental_days_forward_predicate('s.created_time', 'cd.base_time', days_forward ) }} ) --don't pick up incrementals
        AND s.contract_address IN (SELECT contract_address FROM inc_contracts) --is this a contract we need to iterate through
        AND s.contract_address NOT IN (SELECT contract_address FROM new_contracts) --exclude contract we reinitialize

    {% endif %}

  ) b
    left join {{ref('contracts_deterministic_contract_creators')}} as nd 
          ON nd.creator_address = b.creator_address_intermediate
    left join (
              SELECT method_id, contract_project
              FROM {{ ref('base_evm_smart_account_method_ids') }}
              GROUP BY 1,2
            ) aa 
          ON aa.method_id = b.created_tx_method_id
    left join {{ ref('contracts_'+ chain +'_find_self_destruct_contracts') }} as sd 
      on b.contract_address = sd.contract_address
      AND b.blockchain = sd.blockchain
      AND b.created_tx_hash = sd.created_tx_hash
      AND b.created_block_number = sd.created_block_number

  )
  -- starting from 0 
  -- u = next level up contract (i.e. the factory)
  -- b = base-level contract
  {% for i in range(max_levels) -%}

  ,level{{i}}
    as (
      select
        {{i}} as level 
        ,b.blockchain
        ,b.trace_creator_address -- get the original contract creator address
        ,case when nd.creator_address IS NOT NULL
          THEN b.created_tx_from --when deterministic creator, we take the tx sender
          ELSE coalesce(u.creator_address, b.creator_address)
        END as creator_address -- get the highest-level creator we know of
        ,b.creator_address AS trace_deployer_address -- deployer from the trace - does not iterate up
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
          then b.top_level_contract_address ELSE COALESCE(u.top_level_contract_address, b.top_level_contract_address ) END AS top_level_contract_address
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
        ,b.is_self_destruct
        ,b.code
        ,b.token_standard_erc20
        , CASE WHEN u.creator_address IS NOT NULL THEN 
              b.creator_address_lineage || u.creator_address
            ELSE b.creator_address_lineage
          END AS creator_address_lineage
        , CASE WHEN u.created_tx_method_id IS NOT NULL THEN 
              b.tx_method_id_lineage || u.created_tx_method_id
            ELSE b.tx_method_id_lineage
          END AS tx_method_id_lineage
        , b.is_new_contract
        , CASE WHEN u.contract_address IS NULL THEN 0 ELSE 1 END AS loop_again --if it's contract created, then 1 (we loop), else 0 (we're done)

      {% if loop.first -%}
      from base_level as b
      left join base_level as u --get info about the contract that created this contract
        on b.creator_address = u.contract_address
        AND b.blockchain = u.blockchain
      {% else -%}
      from level{{i-1}} as b
      left join base_level as u --get info about the contract that created this contract
        ON b.loop_again = 1 --don't search if we already hit the top
        AND b.creator_address = u.contract_address
        AND b.blockchain = u.blockchain
      {% endif %}
      -- is the creator deterministic?
      left join {{ref('contracts_deterministic_contract_creators')}} as nd 
        ON nd.creator_address = b.creator_address
      WHERE b.is_new_contract = 1
  )
  {%- endfor %}

  SELECT {{ column_list | join(', ') }}
  FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY contract_address, blockchain ORDER BY created_block_number DESC) AS init_rank
    FROM (
      SELECT {{ column_list | join(', ') }}  FROM base_level WHERE is_new_contract = 0
      UNION ALL
      SELECT {{ column_list | join(', ') }}  FROM level{{max_levels - 1}}
    ) uni
  ) filtered
  WHERE init_rank = 1

)

  select 
    blockchain
    ,trace_creator_address
    ,creator_address
    ,trace_deployer_address
    ,u.contract_address
    ,created_time
    ,created_month
    ,is_self_destruct
    ,'creator contracts' as source
    ,top_level_contract_address
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
            SELECT contract_address, MIN(evt_block_number) AS min_block_number, '{{standard_name}}' || '20' as token_standard_erc20
            FROM {{source('erc20_' + chain, 'evt_transfer')}} r, check_date cd
            WHERE 1=1
            AND r.contract_address NOT IN (SELECT contract_address FROM {{ source('tokens_' + chain, standard_name + '20')}} )

              AND {{ incremental_days_forward_predicate('r.evt_block_time', 'cd.base_time', days_forward ) }}

            group by 1
          ) ts 
  ON u.contract_address = ts.contract_address

  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28
  
{% endmacro %}