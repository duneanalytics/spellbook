{% macro contract_creator_project_mapping_by_chain( chain ) %}

-- maybe split out contract naming mappings in to a separate thing
-- do token and name mappings at the end

-- set max number of levels to trace root contract, eventually figure out how to make this properly recursive
{% set max_levels = 3 %} --NOTE: This will make the "creator address" not accurate, if the levels are too low - pivot to use deployer_address
-- set column names to loop through
{% set cols = [
     "trace_creator_address"
    ,"contract_project"
    ,"token_symbol"
    ,"contract_name"
    ,"creator_address"
    ,"deployer_address"
    ,"created_time"
    ,"created_block_number"
    ,"is_self_destruct"
    ,"creation_tx_hash"
    ,"top_level_tx_hash"
    ,"top_level_block_number"
    ,"top_level_time"
    ,"top_level_tx_from"
    ,"top_level_tx_to"
    ,"top_level_tx_method_id"
    ,"created_tx_from"
    ,"created_tx_to"
    ,"created_tx_method_id"
    ,"created_tx_index"
    ,"code_bytelength"
    ,"token_standard"
    ,"code"
    ,"code_deploy_rank_by_chain"
] %}

WITH unified_contract_sources AS (

  select 
    blockchain
    ,trace_creator_address
    ,ic.creator_address
    ,deployer_address
    ,contract_address
    ,COALESCE(al.contract_project, ald.contract_project, oc.namespace) AS contract_project
    ,oc.name as contract_name 
    ,created_time
    ,source
    ,top_level_time
    ,creation_tx_hash
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
    ,1 as map_rank
  from {{ref('contracts_' + chain + '_contract_creator_project_iterated_creators') }} ic
  -- map creator here
    LEFT JOIN {{ref('contracts_contract_creator_address_list')}} al 
      ON ic.creator_address = al.creator_address
    LEFT JOIN {{ref('contracts_contract_creator_address_list')}} ald
      ON ic.deployer_address = ald.creator_address
    left join {{ source( chain , 'contracts') }} as oc 
      ON l.contract_address = oc.address
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
      ,source
      ,created_time as top_level_time
      ,CAST(NULL AS varbinary) as top_level_tx_hash
      ,cast(NULL as bigint) as top_level_block_number
      ,CAST(NULL AS varbinary) as creation_tx_hash
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
    GROUP BY 1,2,3,4,5,6,7,8,9,10

  ---
  -- missing contracts
  ---

  union all
  
  select 
    '{{chain}}' AS blockchain
    ,COALESCE(ct."from",oc."from",0xdeaddeaddeaddeaddeaddeaddeaddeaddead0006) AS trace_creator_address
    ,COALESCE(ct."from",oc."from",0xdeaddeaddeaddeaddeaddeaddeaddeaddead0006) AS creator_address
    ,COALESCE(ct."from",oc."from",0xdeaddeaddeaddeaddeaddeaddeaddeaddead0006) AS deployer_address
    ,l.contract_address
    ,oc.namespace as contract_project 
    ,oc.name as contract_name 
    ,COALESCE(oc.created_at, MIN(block_time)) AS created_time
    ,'missing contracts' as source
    ,COALESCE(oc.created_at, MIN(block_time)) as top_level_time
    ,CAST(NULL AS varbinary) as top_level_tx_hash
    ,cast(NULL as bigint) as top_level_block_number
    ,CAST(NULL AS varbinary) as creation_tx_hash
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
      ON l.contract_address = oc.address
    left join {{ source( chain , 'contracts') }} as oc 
      ON l.contract_address = oc.address
  WHERE
    l.contract_address NOT IN (SELECT contract_address
                          FROM {{ref('contracts_' + chain + '_contract_creator_project_iterated_creators') }}
                          WHERE blockchain = '{{chain}}'
                          )
    AND l.contract_address NOT IN (SELECT contract_address FROM {{ ref('contracts_predeploys') }} WHERE pre.blockchain = '{{chain}}')
    
    {% if is_incremental() %} -- this filter will only be applied on an incremental run 
      and l.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}

  GROUP BY ct."from",oc."from", l.contract_address, oc.namespace, oc.name, oc.created_at, l.tx_index, oc.code
  
)

, get_contracts as (
  --grab the first non-null value for each, i.e. if we have the contract via both contract mapping and optimism.contracts
  select
    blockchain
    ,contract_address
    {% for col in cols %}
    ,(array_agg({{ col }}) filter (where {{ col }} is not NULL))[1] as {{ col }}
    {% endfor %}
  FROM (
  select 
    c.blockchain
    ,c.trace_creator_address
    ,c.contract_address
    ,t_mapped.symbol as token_symbol
    ,c.creator_address
    ,c.contract_project
    ,c.contract_name
    ,c.deployer_address
    ,c.created_time 

    ,c.creation_tx_hash
    ,c.created_block_number
    ,c.created_tx_from
    ,c.created_tx_to
    ,c.created_tx_method_id
    ,c.created_tx_index

    ,c.top_level_time
    ,c.top_level_tx_hash
    ,c.top_level_block_number
    ,c.top_level_tx_from
    ,c.top_level_tx_to
    ,c.top_level_tx_method_id

    ,c.code_bytelength
    ,COALESCE(t_mapped.token_standard, t_raw.token_standard, c.token_standard) AS token_standard
    ,c.code
    ,c.code_deploy_rank_by_chain
    ,MIN(c.map_rank) AS map_rank

  from unified_contract_sources as c 
  left join (
        select
          e.blockchain, e.contract_address, e.symbol, 'erc20' as token_standard
        FROM {{ ref('tokens_erc20')}} e --note: This doesn't yet contain all ERC20 tokens
        WHERE e.blockchain = '{{chain}}'
        GROUP BY 1,2,3,4
      UNION ALL
        select 
          t.blockchain ,t.contract_address ,t.name as symbol, standard AS token_standard
        from {{ ref('tokens_nft') }} as t
        WHERE t.blockchain = '{{chain}}'
        group by 1, 2, 3, 4
      ) as t_mapped
    on c.contract_address = t_mapped.contract_address
    AND c.blockchain = t_mapped.blockchain
  left join ( --ideally, we have an 'all tokens spell' to read from (pending Dune team?), until then:
          SELECT contract_address
            , MIN(min_block_number) AS min_block_number
            , MAX_BY(token_standard, LENGTH(token_standard)) AS token_standard
          FROM (
            SELECT contract_address, MIN(evt_block_number) AS min_block_number, 'erc1155' as token_standard
            FROM {{source('erc1155_' + chain, 'evt_transfersingle')}} r
            WHERE 1=1
            {% if is_incremental() %} -- this filter will only be applied on an incremental run 
            AND r.evt_block_time > NOW() - interval '7' day
            {% endif %}
            group by 1
          UNION ALL
            SELECT contract_address, MIN(evt_block_number) AS min_block_number, 'erc1155' as token_standard
            FROM {{source('erc1155_' + chain, 'evt_transferbatch')}} r
            WHERE 1=1
            {% if is_incremental() %} -- this filter will only be applied on an incremental run 
            AND r.evt_block_time > NOW() - interval '7' day
            {% endif %}
            group by 1
          UNION ALL
            SELECT contract_address, MIN(evt_block_number) AS min_block_number, 'erc721' as token_standard
            FROM {{source('erc721_' + chain, 'evt_transfer')}} r
            WHERE 1=1
            {% if is_incremental() %} -- this filter will only be applied on an incremental run 
            AND r.evt_block_time > NOW() - interval '7' day
            {% endif %}
            group by 1
          UNION ALL
            SELECT contract_address, MIN(evt_block_number) AS min_block_number, 'erc20' as token_standard
            FROM {{source('erc20_' + chain, 'evt_transfer')}} r
            WHERE 1=1
            {% if is_incremental() %} -- this filter will only be applied on an incremental run 
            AND r.evt_block_time > NOW() - interval '7' day
            {% endif %}
            group by 1
          ) ts 
          GROUP BY 1
        ) as t_raw
        on c.contract_address = t_raw.contract_address
        AND c.created_block_number <= t_raw.min_block_number
  group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26
  ORDER BY map_rank ASC NULLS LAST --order we pick
  ) a
  where contract_address is not NULL 
  group by 1,2
)


SELECT u.*,

  {% if is_incremental() %}
  CASE WHEN
    th.contract_address IS NULL -- did not exist
    -- check if a major field was updated
    OR u.contract_project<>th.contract_project
    OR u.token_symbol<>th.token_symbol
    OR u.contract_name<>u.contract_name
    OR u.creator_address<>u.creator_address
    OR u.code_deploy_rank_by_chain<>u.code_deploy_rank_by_chain
    OR th.token_standard<>u.token_standard
  THEN 1 ELSE 0 END
  {% else -%}
  1
  {% endif %}
  AS is_updated_in_last_run

FROM (
  SELECT
    created_month,
    blockchain,
    trace_creator_address,  contract_address
  , initcap(contract_project) AS contract_project
  , token_symbol
  , contract_name, creator_address, deployer_address, created_time
  , creation_tx_hash, created_block_number, created_tx_from
  , created_tx_to, created_tx_method_id, created_tx_index
  , top_level_time, top_level_tx_hash, top_level_block_number
  , top_level_tx_from, top_level_tx_to , top_level_tx_method_id
  , code_bytelength , token_standard 
  , code
  , code_deploy_rank_by_chain
  , is_eoa_deployed

  FROM (
    select 
      cast(DATE_TRUNC('month',c.created_time) as date) AS created_month
      ,c.blockchain
      ,c.trace_creator_address
      ,c.contract_address
      ,cast(
          replace(
          -- priority order: Override name, Mapped vs Dune, Raw/Actual names
            coalesce(
              co.contract_project
              ,dnm.mapped_name
              ,c.contract_project
            ),
          '_',
          ' '
      ) as varchar) as contract_project
      ,c.token_symbol
      ,cast( coalesce(co.contract_name, c.contract_name) as varchar) as contract_name
      ,c.creator_address
      ,c.deployer_address
      ,c.created_time
      ,CASE WHEN sd.is_self_destruct IS NOT NULL THEN true ELSE false END as is_self_destruct
      ,c.creation_tx_hash
      ,COALESCE(c.created_block_number,0) AS created_block_number
      ,c.created_tx_from
      ,c.created_tx_to
      ,c.created_tx_method_id
      ,c.created_tx_index

      ,c.top_level_time
      ,c.top_level_tx_hash
      ,c.top_level_block_number
      ,c.top_level_tx_from
      ,c.top_level_tx_to
      ,c.top_level_tx_method_id
      
      ,c.code_bytelength
      ,c.token_standard
      ,c.code
      ,c.code_deploy_rank_by_chain
      ,CASE WHEN c.trace_creator_address = c.created_tx_from THEN 1 ELSE 0 END AS is_eoa_deployed
      ,CASE WHEN c.top_level_tx_method_id in (SELECT method_id FROM {{ ref('evm_smart_account_method_ids') }}) THEN 1 ELSE 0 END AS is_smart_wallet_deployed
      ,CASE WHEN c.trace_creator_address in (SELECT creator_address from {{ref('contracts_deterministic_contract_creators')}} ) THEN 1 ELSE 0 END AS is_deterministic_deployer_deployed
    from get_contracts as c 
    left join {{ ref('contracts_project_name_mappings') }} as dnm -- fix names for decoded contracts
      on lower(c.contract_project) = lower(dnm.dune_name)
    left join {{ ref('contracts_contract_overrides') }} as co --override contract maps
      on c.contract_address = co.contract_address
    left join {{ ref('contracts_'+ chain +'_find_self_destruct_contracts') }} as sd 
      on c.contract_address = sd.contract_address
      AND c.blockchain = sd.blockchain
      and c.creation_tx_hash = sd.creation_tx_hash
      AND c.created_block_number = sd.created_block_number
  ) f
) u

{% if is_incremental() %}
left join {{this}} th -- see if this was updated or not
  ON th.contract_address = u.contract_address
  AND th.blockchain = u.blockchain
  AND th.created_block_number = u.created_block_number
  AND th.created_time = u.created_time
{% endif %}

{% endmacro %}