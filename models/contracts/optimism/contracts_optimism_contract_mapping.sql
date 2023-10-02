 {{
  config(
        tags = ['dunesql'],
        alias = alias('contract_mapping'),
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key='contract_address',
        post_hook='{{ expose_spells(\'["optimism"]\',
                                    "sector",
                                    "contracts",
                                    \'["msilb7", "chuxin"]\') }}'
  )
}}

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
    ,"contract_factory"
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
    ,"code_deploy_rank"
] %}


with base_level as (
SELECT *
  FROM (
  select 
     trace_creator_address -- get the original contract creator address
    ,creator_address
    ,contract_factory
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
    ,ROW_NUMBER() OVER (PARTITION BY code ORDER BY created_block_number ASC, created_tx_index ASC) AS code_deploy_rank
    ,ROW_NUMBER() OVER (PARTITION BY contract_address ORDER BY created_time ASC ) AS contract_order -- to ensure no dupes
    ,to_iterate_creators

  from (
    select 
       ct."from" as trace_creator_address
      ,ct."from" as creator_address
      ,CAST(NULL AS varbinary) as contract_factory
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
      ,bytearray_length(ct.code) AS code_bytelength --toreplace with bytearray_length in dunesql
      ,coalesce(sd.contract_address is not NULL, false) as is_self_destruct
      ,1 AS to_iterate_creators
    from {{ source('optimism', 'creation_traces') }} as ct 
    inner join {{ source('optimism', 'transactions') }} as t 
      ON t.hash = ct.tx_hash
      AND t.block_time = ct.block_time
      AND t.block_number = ct.block_number
      {% if is_incremental() %}
      and t.block_time >= date_trunc('day', now() - interval '7' day)
      AND ct.block_time >= date_trunc('day', now() - interval '7' day)
      {% endif %}
    left join {{ ref('contracts_optimism_self_destruct_contracts') }} as sd 
      on ct.address = sd.contract_address
      and ct.tx_hash = sd.creation_tx_hash
      and ct.block_time = sd.created_time
      {% if is_incremental() %}
      and sd.created_time >= date_trunc('day', now() - interval '7' day)
      AND ct.block_time >= date_trunc('day', now() - interval '7' day)
      {% endif %}
    where 
      1=1
      {% if is_incremental() %}
      and ct.block_time >= date_trunc('day', now() - interval '7' day)

    -- to get existing history of contract mapping / only select those we want to re-run
    union all 

    select 
       t.trace_creator_address
      ,t.creator_address
      ,t.contract_creator_if_factory as contract_factory
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
      ,ct.code
      ,t.code_bytelength
      ,coalesce(sd.contract_address is not NULL, t.is_self_destruct, false) as is_self_destruct
      , CASE
        WHEN nd.creator_address IS NOT NULL THEN 1
        WHEN ct."from" != t.trace_creator_address THEN 1 -- weird data ingestion issue?
        WHEN t.trace_creator_address IS NULL THEN 1 -- weird data ingestion issue?
        ELSE 0 END AS to_iterate_creators
    from {{ this }} t
    left join {{ ref('contracts_optimism_self_destruct_contracts') }} as sd 
      on t.contract_address = sd.contract_address
      and t.creation_tx_hash = sd.creation_tx_hash
      and t.created_time = sd.created_time
      AND t.created_block_number = sd.created_block_number
    left join {{ source('optimism', 'creation_traces') }} as ct
      ON t.contract_address = ct.address
      AND t.created_time = ct.block_time
      AND t.created_block_number = ct.block_number
      AND t.creation_tx_hash = ct.tx_hash
      AND sd.contract_address IS NULL

    -- If the creator becomes marked as deterministic, we want to re-run it.
    left join {{ref('contracts_optimism_deterministic_contract_creators')}} as nd 
      ON nd.creator_address = t.creator_address

    -- Don't pull contracts that are in the incremental group (prevent dupes)
    WHERE t.contract_address NOT IN (
      SELECT address FROM {{ source('optimism', 'creation_traces') }} WHERE block_time >= date_trunc('day', now() - interval '7' day)
    )
      {% endif %} -- incremental filter
  ) as x
  group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 22, code
) y 
WHERE contract_order = 1
)

,tokens as (
  select 
    t.contract_address
    ,t.symbol
    ,'erc20' as token_standard
  from {{ ref('tokens_optimism_erc20') }} as t
  group by 1, 2, 3

  union all 

  select 
    t.contract_address
    ,t.name as symbol
    , standard AS token_standard
  from {{ ref('tokens_optimism_nft') }} as t
  group by 1, 2, 3
)

-- starting from 0 
-- u = next level up contract (i.e. the factory)
-- b = base-level contract
{% for i in range(max_levels) -%}
,level{{i}} as (
    select
      {{i}} as level 
      ,b.trace_creator_address -- get the original contract creator address
      ,
      case when nd.creator_address IS NOT NULL
        THEN b.created_tx_from --when deterministic creator, we take the tx sender
        ELSE coalesce(u.creator_address, b.creator_address)
      END as creator_address -- get the highest-level creator we know of
      {% if loop.first -%}
      ,case when u.creator_address is NULL then CAST(NULL as varbinary)
        else b.creator_address
      end as contract_factory -- if factory created, maintain the original factory
      {% else -%}
      ,b.contract_factory -- if factory created, maintain the original factory
      {% endif %}
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
      ,b.code_deploy_rank
      ,b.contract_order
      ,b.to_iterate_creators --check if base needs to be iterated, keep the base option

    {% if loop.first -%}
    from base_level as b
    left join base_level as u --get info about the contract that created this contract
      on b.creator_address = u.contract_address
      AND ( b.created_time >= u.created_time OR u.created_time IS NULL) --base level was created on or after its creator
    {% else -%}
    from level{{i-1}} as b
    left join base_level as u --get info about the contract that created this contract
      on b.creator_address = u.contract_address
      AND ( b.created_time >= u.created_time OR u.created_time IS NULL) --base level was created on or after its creator
    {% endif %}
    -- is the creator deterministic?
    left join {{ref('contracts_optimism_deterministic_contract_creators')}} as nd 
      ON nd.creator_address = b.creator_address
    
    WHERE b.to_iterate_creators=1 --only run contracts that we want to iterate through
)
{%- endfor %}

,creator_contracts as (
  select 
     f.trace_creator_address
    ,f.creator_address
    ,f.contract_factory
    ,f.contract_address
    ,coalesce(cc.contract_project, ccf.contract_project, cctr.contract_project) as contract_project 
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
    ,f.code_deploy_rank
  from (
    SELECT * FROM level{{max_levels - 1}} WHERE to_iterate_creators = 1 --get mapped contracts
    UNION ALL
    SELECT 5 as level, * FROM base_level WHERE to_iterate_creators = 0 --get legacy contracts
  ) f
  left join {{ ref('contracts_optimism_contract_creator_address_list') }} as cc 
    on f.creator_address = cc.creator_address
  left join {{ ref('contracts_optimism_contract_creator_address_list') }} as ccf
    on f.contract_factory = ccf.creator_address
  left join {{ ref('contracts_optimism_contract_creator_address_list') }} as cctr
    on f.trace_creator_address = cctr.creator_address
  
  where f.contract_address is not null
 )
,combine as (
  select 
    cc.trace_creator_address
    ,cc.creator_address
    ,cc.contract_factory
    ,cc.contract_address
    ,coalesce(cc.contract_project, oc.namespace) as contract_project 
    ,oc.name as contract_name 
    ,cc.created_time
    ,coalesce(cc.is_self_destruct, false) as is_self_destruct
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
    ,cc.code_deploy_rank
    ,1 as map_rank
  from creator_contracts as cc 
  left join {{ source('optimism', 'contracts') }} as oc 
    on cc.contract_address = oc.address 
  group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24
  
  union all
  -- missing contracts
  select 
     COALESCE(oc."from",0xdeaddeaddeaddeaddeaddeaddeaddeaddead0006) AS trace_creator_address
    ,COALESCE(oc."from",0xdeaddeaddeaddeaddeaddeaddeaddeaddead0006) AS creator_address
    ,CAST(NULL AS varbinary) as contract_factory
    ,l.contract_address
    ,oc.namespace as contract_project 
    ,oc.name as contract_name 
    ,COALESCE(oc.created_at, MIN(block_time)) AS created_time
    ,false as is_self_destruct
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
    ,1 as code_deploy_rank
    ,2 as map_rank
  from {{ source('optimism', 'logs') }} as l
    left join {{ source('optimism', 'contracts') }} as oc 
      ON l.contract_address = oc.address
  WHERE
    l.contract_address NOT IN (SELECT cc.contract_address FROM creator_contracts cc)
    {% if is_incremental() %} -- this filter will only be applied on an incremental run 
      and l.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
  GROUP BY oc."from", l.contract_address, oc.namespace, oc.name, oc.created_at, l.tx_index, oc.code

  union all
  -- predeploys
  select 
      trace_creator_address
      ,creator_address
      ,CAST(NULL AS varbinary) as contract_factory
      ,contract_address
      ,contract_project
      ,contract_name
      ,created_time
      ,is_self_destruct
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
      ,1 as code_deploy_rank
      ,3 as map_rank

    FROM {{ ref('contracts_optimism_predeploys') }} pre
    where 
    1=1
    {% if is_incremental() %} -- this filter will only be applied on an incremental run 
    and 1=0 --do not run on incremental builds
    {% endif %}
    GROUP BY 1,2,3,4,5,6,7,8,9

)

,get_contracts as (
  select *
  FROM (
  select 
    c.trace_creator_address
    ,c.contract_address
    ,c.contract_factory
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
    ,t.token_standard AS token_standard
    ,c.code_deploy_rank
    ,MIN(c.map_rank) AS map_rank

  from combine as c 
  left join tokens as t 
    on c.contract_address = t.contract_address
  group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24
  ) a
  ORDER BY map_rank ASC NULLS LAST --order we pick
)
,cleanup as (
--grab the first non-null value for each, i.e. if we have the contract via both contract mapping and optimism.contracts
  select
    contract_address
    {% for col in cols %}
    ,(array_agg({{ col }}) filter (where {{ col }} is not NULL))[1] as {{ col }}
    {% endfor %}
  from get_contracts
  where contract_address is not NULL 
  group by 1
)
SELECT
  trace_creator_address,  contract_address, 
  --initcap: https://jordanlamborn.medium.com/presto-sql-proper-case-initcap-how-to-capitalize-the-first-letter-of-each-word-in-presto-5fbac3f0154c
  (array_join((transform((split(lower(contract_project),' '))
    , x -> concat(upper(substr(x,1,1)),substr(x,2,length(x))))),' ',''))
    AS contract_project
  --
, token_symbol
, contract_name, creator_address, created_time, contract_creator_if_factory
, is_self_destruct, creation_tx_hash, created_block_number, created_tx_from
, created_tx_to, created_tx_method_id, created_tx_index
, top_level_time, top_level_tx_hash, top_level_block_number
, top_level_tx_from, top_level_tx_to , top_level_tx_method_id
, code_bytelength , token_standard , code_deploy_rank, is_eoa_deployed

FROM (
  select 
    COALESCE(c.trace_creator_address, CAST(NULL AS varbinary) ) AS trace_creator_address
    ,c.contract_address
    ,cast(
        replace(
        -- priority order: Override name, Mapped vs Dune, Raw/Actual names
          coalesce(
            co.contract_project
            ,dnm.mapped_name
            ,c.contract_project
            ,ovm1c.contract_project
          ),
        '_',
        ' '
    ) as varchar) as contract_project
    ,c.token_symbol
    ,cast( coalesce(co.contract_name, c.contract_name) as varchar) as contract_name
    ,coalesce(c.creator_address, ovm1c.creator_address, CAST(NULL AS varbinary) ) as creator_address
    ,coalesce(c.created_time
      -- , CAST(SUBSTRING(ovm1c.created_time, 1, POSITION('T' IN ovm1c.created_time) - 1) as timestamp)
        , from_iso8601_timestamp( ovm1c.created_time )
       ) as created_time
    ,coalesce(c.contract_factory, 
    {% if is_incremental() %}
      th.contract_creator_if_factory
      {% else -%}
      cast(NULL as varbinary)
    {% endif %}
    ) as contract_creator_if_factory
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
    ,c.code_deploy_rank
    ,CASE WHEN c.trace_creator_address = c.created_tx_from THEN 1 ELSE 0 END AS is_eoa_deployed

  from cleanup as c 
  left join {{ source('ovm1_optimism', 'contracts') }} as ovm1c
    on c.contract_address = ovm1c.contract_address --fill in any missing contract creators
  left join {{ ref('contracts_optimism_project_name_mappings') }} as dnm -- fix names for decoded contracts
    on lower(c.contract_project) = lower(dnm.dune_name)
  left join {{ ref('contracts_optimism_contract_overrides') }} as co --override contract maps
    on c.contract_address = co.contract_address
  {% if is_incremental() %} -- this filter will only be applied on an incremental run 
  left join {{ this }} th -- grab if the contract was previously picked up as factory created
    ON th.contract_address = c.contract_address
    AND th.created_block_number = c.created_block_number
  {% endif %}
) f
