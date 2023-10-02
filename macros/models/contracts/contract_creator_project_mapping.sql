{% macro contract_creator_project_mapping_by_chain( chain ) %}


-- set max number of levels to trace root contract, eventually figure out how to make this properly recursive
{% set max_levels = 5 %}
-- set column names to loop through
{% set cols = [
     "trace_creator_address"
    ,"contract_project"
    ,"token_symbol"
    ,"contract_name"
    ,"creator_address"
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
WITH base_level as (

SELECT *
  FROM (
  select 
    blockchain
    ,trace_creator_address -- get the original contract creator address
    ,creator_address
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
    ,is_self_destruct
    ,token_standard
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
            ,ct."from" as creator_address
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
            ,coalesce(sd.contract_address is not NULL, false) as is_self_destruct
            ,NULL AS token_standard
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
      ,case when t.is_self_destruct then true
            when sd.contract_address is not NULL then true
            else false
      end as is_self_destruct
      ,token_standard
      , CASE
        WHEN nd.creator_address IS NOT NULL THEN 1
        ELSE 0 END AS to_iterate_creators
      , 0 AS is_new_contract
    from {{ this }} t
    left join {{ ref('contracts_'+ chain +'_find_self_destruct_contracts') }} as sd 
      on t.contract_address = sd.contract_address
      and t.creation_tx_hash = sd.creation_tx_hash
      and t.created_time = sd.created_time
      AND t.created_block_number = sd.created_block_number
      AND t.blockchain = sd.blockchain
      AND t.is_self_destruct = false --find new selfdestructs
      AND sd.destructed_time >= date_trunc('day', now() - interval '7' day) -- new self-destructs only

    -- If the creator becomes marked as deterministic, we want to re-run it.
    left join {{ref('contracts_deterministic_contract_creators')}} as nd 
      ON nd.creator_address = t.creator_address

    
    WHERE t.blockchain = '{{chain}}'

    {% endif %} -- incremental filter

  ) as x
  group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24
) y 
--Don't run the same contract twice (i.e. incremental and existing)
WHERE contract_order = 1
)

,tokens as (
  select
    e.blockchain
    ,e.contract_address
    ,e.symbol
    ,'erc20' as token_standard

  FROM {{ ref('tokens_erc20')}} e --note: This doesn't yet contain all ERC20 tokens
  WHERE e.blockchain = '{{chain}}'
  GROUP BY 1,2,3,4

  UNION ALL

  select 
    t.blockchain
    ,t.contract_address
    ,t.name as symbol
    , standard AS token_standard
  from {{ ref('tokens_nft') }} as t
  WHERE t.blockchain = '{{chain}}'
  group by 1, 2, 3, 4
)

-- starting from 0 
-- u = next level up contract (i.e. the factory)
-- b = base-level contract
{% for i in range(max_levels) -%}
,level{{i}} as (
    select
      {{i}} as level 
      ,b.blockchain
      ,b.trace_creator_address -- get the original contract creator address
      ,case when nd.creator_address IS NOT NULL
        THEN b.created_tx_from --when deterministic creator, we take the tx sender
        ELSE coalesce(u.creator_address, b.creator_address)
      END as creator_address -- get the highest-level creator we know of
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
      ,b.is_self_destruct
      ,b.token_standard
      ,b.code_deploy_rank_by_chain
      ,b.to_iterate_creators --check if base needs to be iterated, keep the base option
      ,b.code
      ,b.is_new_contract
      ,b.contract_order

    {% if loop.first -%}
    from base_level as b
    left join base_level as u --get info about the contract that created this contract
      on b.creator_address = u.contract_address
      AND ( b.created_time >= u.created_time OR u.created_time IS NULL) --base level was created on or after its creator
      AND b.blockchain = u.blockchain
    {% else -%}
    from level{{i-1}} as b
    left join base_level as u --get info about the contract that created this contract
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
    ,f.contract_address
    ,coalesce(cc.contract_project, cctr.contract_project) as contract_project 
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
    ,f.is_self_destruct
    ,f.token_standard
    ,f.code
    ,COALESCE(f.code_deploy_rank_by_chain, cr.code_deploy_rank_by_chain) AS code_deploy_rank_by_chain
  from (
    SELECT * FROM level{{max_levels - 1}} WHERE to_iterate_creators = 1 --get mapped contracts
    UNION ALL
    SELECT 5 as level, * FROM base_level WHERE to_iterate_creators = 0 --get legacy contracts
  ) f
  left join {{ ref('contracts_contract_creator_address_list') }} as cc 
    on f.creator_address = cc.creator_address
  left join {{ ref('contracts_contract_creator_address_list') }} as cctr
    on f.trace_creator_address = cctr.creator_address
  LEFT JOIN code_ranks cr --code ranks for new contracts
    ON cr.blockchain = f.blockchain
    AND cr.contract_address = f.contract_address
    AND cr.created_block_number = f.created_block_number
  
  where f.contract_address is not null
 )
,combine as (

  select 
    cc.blockchain
    ,cc.trace_creator_address
    ,cc.creator_address
    ,cc.contract_address
    ,coalesce(cc.contract_project, oc.namespace) as contract_project 
    ,oc.name as contract_name 
    ,cc.created_time
    ,coalesce(cc.is_self_destruct, false) as is_self_destruct
    ,cc.token_standard
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
    ,1 as map_rank
  from creator_contracts as cc 
  left join {{ source( chain , 'contracts') }} as oc 
    on cc.contract_address = oc.address 
  WHERE cc.blockchain = '{{chain}}'
  group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26
  
  union all
  -- missing contracts
  select 
    '{{chain}}' AS blockchain
    ,COALESCE(oc."from",0xdeaddeaddeaddeaddeaddeaddeaddeaddead0006) AS trace_creator_address
    ,COALESCE(oc."from",0xdeaddeaddeaddeaddeaddeaddeaddeaddead0006) AS creator_address
    ,l.contract_address
    ,oc.namespace as contract_project 
    ,oc.name as contract_name 
    ,COALESCE(oc.created_at, MIN(block_time)) AS created_time
    ,false as is_self_destruct
    ,NULL AS token_standard
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
    ,2 as map_rank
  from {{ source( chain , 'logs') }} as l
    left join {{ source( chain , 'contracts') }} as oc 
      ON l.contract_address = oc.address
  WHERE
    l.contract_address NOT IN (SELECT cc.contract_address FROM creator_contracts cc WHERE cc.blockchain = '{{chain}}')
    {% if is_incremental() %} -- this filter will only be applied on an incremental run 
      and l.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
  GROUP BY oc."from", l.contract_address, oc.namespace, oc.name, oc.created_at, l.tx_index, oc.code
  
---
-- predeploys
---
  union all
  
  select 
      blockchain
      ,trace_creator_address
      ,creator_address
      ,contract_address
      ,contract_project
      ,contract_name
      ,created_time
      ,is_self_destruct
      ,NULL AS token_standard
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
      ,3 as map_rank

    FROM {{ ref('contracts_predeploys') }} pre
    where 
    1=1
    and pre.blockchain = '{{chain}}'
    {% if is_incremental() %} -- this filter will only be applied on an incremental run 
    and 1=0 --do not run on incremental builds
    {% endif %}
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11

)

,get_contracts as (
  select *
  FROM (
  select 
    c.blockchain
    ,c.trace_creator_address
    ,c.contract_address
    ,c.contract_project
    ,t.symbol as token_symbol
    ,c.contract_name
    ,c.creator_address
    ,c.created_time 
    ,c.is_self_destruct

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
    ,COALESCE(t.token_standard, c.token_standard,
    -- to be replaced with all tokens table
      CASE 
      WHEN EXISTS (SELECT 1
                        FROM {{source('erc1155_' + chain, 'evt_transfersingle')}} r
                        WHERE c.contract_address = r.contract_address
                        AND r.evt_block_time >= c.created_time
                        {% if is_incremental() %} -- this filter will only be applied on an incremental run 
                        AND r.evt_block_time > NOW() - interval '7' day
                        {% endif %}
                        ) THEN 'erc1155'
      WHEN EXISTS (SELECT 1
                        FROM {{source('erc1155_' + chain, 'evt_transferbatch')}} r
                        WHERE c.contract_address = r.contract_address
                        AND r.evt_block_time >= c.created_time
                        {% if is_incremental() %} -- this filter will only be applied on an incremental run 
                        AND r.evt_block_time > NOW() - interval '7' day
                        {% endif %}
                        ) THEN 'erc1155'
      WHEN EXISTS (SELECT 1
                        FROM {{source('erc721_' + chain, 'evt_transfer')}} r
                        WHERE c.contract_address = r.contract_address
                        AND r.evt_block_time >= c.created_time
                        {% if is_incremental() %} -- this filter will only be applied on an incremental run 
                        AND r.evt_block_time > NOW() - interval '7' day
                        {% endif %}
                        ) THEN 'erc721'
      WHEN EXISTS (SELECT 1
                        FROM {{source('erc20_' + chain, 'evt_transfer')}} r
                        WHERE c.contract_address = r.contract_address
                        AND r.evt_block_time >= c.created_time
                        {% if is_incremental() %} -- this filter will only be applied on an incremental run 
                        AND r.evt_block_time > NOW() - interval '7' day
                        {% endif %}
                        ) THEN 'erc20'
      ELSE NULL END
      ) AS token_standard
    ,c.code
    ,c.code_deploy_rank_by_chain
    ,MIN(c.map_rank) AS map_rank

  from combine as c 
  left join tokens as t 
    on c.contract_address = t.contract_address
    AND c.blockchain = t.blockchain
  group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25
  ) a
  ORDER BY map_rank ASC NULLS LAST --order we pick
)
,cleanup as (
--grab the first non-null value for each, i.e. if we have the contract via both contract mapping and optimism.contracts
  select
    blockchain
    ,contract_address
    {% for col in cols %}
    ,(array_agg({{ col }}) filter (where {{ col }} is not NULL))[1] as {{ col }}
    {% endfor %}
  from get_contracts
  where contract_address is not NULL 
  group by 1,2
)


, updated_data AS (
  SELECT
    created_month,
    blockchain,
    trace_creator_address,  contract_address, 
    --initcap: https://jordanlamborn.medium.com/presto-sql-proper-case-initcap-how-to-capitalize-the-first-letter-of-each-word-in-presto-5fbac3f0154c
    (array_join((transform((split(lower(contract_project),' '))
      , x -> concat(upper(substr(x,1,1)),substr(x,2,length(x))))),' ',''))
      AS contract_project
    --
  , token_symbol
  , contract_name, creator_address, created_time
  , is_self_destruct, creation_tx_hash, created_block_number, created_tx_from
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
      ,coalesce(c.creator_address, CAST(NULL AS varbinary) ) as creator_address
      ,c.created_time
      ,coalesce(c.is_self_destruct, false) as is_self_destruct
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
    from cleanup as c 
    left join {{ ref('contracts_project_name_mappings') }} as dnm -- fix names for decoded contracts
      on lower(c.contract_project) = lower(dnm.dune_name)
    left join {{ ref('contracts_contract_overrides') }} as co --override contract maps
      on c.contract_address = co.contract_address
  ) f
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

FROM updated_data u
{% if is_incremental() %}
left join {{this}} th -- see if this was updated or not
  ON th.contract_address = u.contract_address
  AND th.blockchain = u.blockchain
  AND th.created_block_number = u.created_block_number
  AND th.created_time = u.created_time
{% endif %}

{% endmacro %}