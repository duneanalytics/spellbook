 {{
  config(
        alias='contract_mapping',
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
{% set max_levels = 10 %}
-- set column names to loop through
{% set cols = [
     "trace_creator_address"
    ,"contract_project"
    ,"token_symbol"
    ,"contract_name"
    ,"creator_address"
    ,"created_time"
    ,"contract_factory"
    ,"is_self_destruct"
    ,"creation_tx_hash"
] %}

with base_level as (
  select 
     creator_address AS trace_creator_address -- get the original contract creator address
    ,creator_address
    ,contract_factory
    ,contract_address
    ,created_time
    ,created_block_number
    ,creation_tx_hash
    ,top_level_tx_from
    ,top_level_tx_to
    ,top_level_tx_method_id
    ,created_tx_from
    ,created_tx_to
    ,created_tx_method_id
    ,is_self_destruct
  from (
    select 
      ct.from as creator_address
      ,CAST(NULL AS string) as contract_factory
      ,ct.address as contract_address
      ,ct.block_time as created_time
      ,ct.block_number as created_block_number
      ,ct.tx_hash as creation_tx_hash
      ,t.from AS top_level_tx_from
      ,t.to AS top_level_tx_to
      ,substring(t.data,1,10) AS top_level_tx_method_id
      ,t.from AS created_tx_from
      ,t.to AS created_tx_to
      ,substring(t.data,1,10) AS created_tx_method_id
      ,bytearray_length(ct.code) AS code_bytelength
      ,coalesce(sd.contract_address is not NULL, false) as is_self_destruct
    from {{ source('optimism', 'creation_traces') }} as ct 
    inner join {{ source('optimism', 'transactions') }} as t 
      ON t.hash = ct.tx_hash
      AND t.block_time = ct.created_time
      AND t.block_number = ct.block_number
      {% if is_incremental() %}
      and sd.created_time >= date_trunc('day', now() - interval '1 week')
      {% endif %}
    left join {{ ref('contracts_optimism_self_destruct_contracts') }} as sd 
      on ct.address = sd.contract_address
      and ct.tx_hash = sd.creation_tx_hash
      and ct.block_time = sd.created_time
      and ct.block_number = sd.block_number
      {% if is_incremental() %}
      and sd.created_time >= date_trunc('day', now() - interval '1 week')
      {% endif %}
    where 
      true
      {% if is_incremental() %}
      and ct.block_time >= date_trunc('day', now() - interval '1 week')

    -- to get existing history of contract mapping
    union all 

    select 
      creator_address
      ,contract_creator_if_factory as contract_factory
      ,contract_address
      ,created_time
      ,creation_tx_hash
      ,is_self_destruct
      ,top_level_tx_from
      ,top_level_tx_to
      ,top_level_tx_method_id
      ,created_tx_from
      ,created_tx_to
      ,created_tx_method_id
      ,code_bytelength
    from {{ this }}
      {% endif %} -- incremental filter
  ) as x
  group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
)

,tokens as (
  select 
    bl.contract_address
    ,t.symbol
  from base_level as bl 
  join {{ ref('tokens_optimism_erc20') }} as t
    on bl.contract_address = t.contract_address
  group by 1, 2

  union all 

  select 
    bl.contract_address
    ,t.name as symbol
  from base_level as bl 
  join {{ ref('tokens_optimism_nft') }} as t
    on bl.contract_address = t.contract_address
  group by 1, 2
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
      case when b.creator_address IN (SELECT creator_address FROM {{ref('contracts_optimism_nondeterministic_contract_creators')}})
        THEN b.tx_from --when non-deterministic creator, we take the tx sender
        ELSE coalesce(u.creator_address, b.creator_address)
      END as creator_address -- get the highest-level creator we know of
      {% if loop.first -%}
      ,case when u.creator_address is NULL then NULL
        else b.creator_address
      end as contract_factory -- if factory created, maintain the original factory
      {% else -%}
      ,b.contract_factory -- if factory created, maintain the original factory
      {% endif %}
      ,b.contract_address
      ,b.created_time
      ,b.creation_tx_hash
      ,b.created_block_number
      ,u.tx_from AS top_level_tx_from
      ,u.tx_to AS top_level_tx_to
      ,u.tx_method_id AS top_level_tx_method_id
      ,b.tx_from AS created_tx_from
      ,b.tx_to AS created_tx_to
      ,b.tx_method_id AS created_tx_method_id
      ,b.is_self_destruct
    {% if loop.first -%}
    from base_level as b
    left join base_level as u --get info about the contract that created this contract
      on b.creator_address = u.contract_address
      AND b.creator_address NOT IN -- don't map creators that we know are not deterministic
        (SELECT creator_address FROM {{ref('contracts_optimism_nondeterministic_contract_creators')}})
    {% else -%}
    from level{{i-1}} as b
    left join base_level as u --get info about the contract that created this contract
      on b.creator_address = u.contract_address
      AND b.creator_address NOT IN -- don't map creators that we know are not deterministic
        (SELECT creator_address FROM {{ref('contracts_optimism_nondeterministic_contract_creators')}})
    {% endif %}
)
{%- endfor %}

,creator_contracts as (
  select 
     f.trace_creator_address
    ,f.creator_address
    ,f.contract_factory
    ,f.contract_address
    ,coalesce(cc.contract_project, ccf.contract_project) as contract_project 
    ,f.created_time
    ,f.creation_tx_hash
    ,f.created_block_number
    ,f.top_level_tx_from
    ,f.top_level_tx_to
    ,f.top_level_tx_method_id
    ,f.created_tx_from
    ,f.created_tx_to
    ,f.created_tx_method_id
    ,f.is_self_destruct
  from level{{max_levels - 1}} as f
  left join {{ ref('contracts_optimism_contract_creator_address_list') }} as cc 
    on f.creator_address = cc.creator_address
  left join {{ ref('contracts_optimism_contract_creator_address_list') }} as ccf
    on f.contract_factory = ccf.creator_address
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
    ,cc.creation_tx_hash
    ,cc.created_block_number
    ,cc.top_level_tx_from
    ,cc.top_level_tx_to
    ,cc.top_level_tx_method_id
    ,cc.created_tx_from
    ,cc.created_tx_to
    ,cc.created_tx_method_id
  from creator_contracts as cc 
  left join {{ source('optimism', 'contracts') }} as oc 
    on cc.contract_address = oc.address 

  union all
  -- missing contracts
  select 
     COALESCE(oc.from,'0xdeaddeaddeaddeaddeaddeaddeaddeaddead0006') AS trace_creator_address
    ,COALESCE(oc.from,'0xdeaddeaddeaddeaddeaddeaddeaddeaddead0006') AS creator_address
    ,cast(NULL as string) as contract_factory
    ,l.contract_address
    ,oc.namespace as contract_project 
    ,oc.name as contract_name 
    ,COALESCE(oc.created_at, MIN(block_time)) AS created_time
    ,false as is_self_destruct
    ,'missing contracts' as source
    ,cast(NULL as string) as creation_tx_hash
    ,cast(NULL as bigint) as created_block_number
    ,cast(NULL as string) as top_level_tx_from
    ,cast(NULL as string) as top_level_tx_to
    ,cast(NULL as string) as top_level_tx_method_id
    ,cast(NULL as string) as created_tx_from
    ,cast(NULL as string) as created_tx_to
    ,cast(NULL as string) as created_tx_method_id
    
  from {{ source('optimism', 'logs') }} as l
    left join {{ source('optimism', 'contracts') }} as oc 
      ON l.contract_address = oc.address
  WHERE
    l.contract_address NOT IN (SELECT cc.contract_address FROM creator_contracts cc)
    {% if is_incremental() %} -- this filter will only be applied on an incremental run 
      and l.block_time >= date_trunc('day', now() - interval '1 week')
      and not exists (
          select 1 
          from {{ this }} as gc
          where 
            gc.contract_address = l.contract_address
        )
    {% endif %}
  GROUP BY oc.from, l.contract_address, oc.namespace, oc.name, oc.created_at

  union all
  -- ovm 1.0 contracts

  select 
     creator_address AS trace_creator_address
    ,creator_address
    ,cast(NULL as string) as contract_factory
    ,contract_address
    ,contract_project
    ,contract_name
    ,to_timestamp(created_time) as created_time
    ,false as is_self_destruct
    ,'ovm1 contracts' as source
    ,cast(NULL as string) as creation_tx_hash
    ,cast(NULL as bigint) as created_block_number
    ,cast(NULL as string) as top_level_tx_from
    ,cast(NULL as string) as top_level_tx_to
    ,cast(NULL as string) as top_level_tx_method_id
    ,cast(NULL as string) as created_tx_from
    ,cast(NULL as string) as created_tx_to
    ,cast(NULL as string) as created_tx_method_id
  from {{ source('ovm1_optimism', 'contracts') }} as c
  where 
    true
    {% if is_incremental() %} -- this filter will only be applied on an incremental run 
    and not exists (
      select 1
      from {{ this }} as gc
      where 
        gc.contract_address = c.contract_address
        and (
          (gc.contract_project = c.contract_project) or (gc.contract_project is NULL)
        )
    )
    {% endif %}
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10

  union all 
  --synthetix genesis contracts

  select 
     cast(NULL as string) as trace_creator_address
    ,cast(NULL as string) as creator_address
    ,cast(NULL as string) as contract_factory
    ,snx.contract_address
    ,'Synthetix' as contract_project
    ,contract_name
    ,to_timestamp('2021-07-06 00:00:00') as created_time
    ,false as is_self_destruct
    ,'synthetix contracts' as source
    ,cast(NULL as string) as creation_tx_hash
    ,cast(NULL as bigint) as created_block_number
    ,cast(NULL as string) as top_level_tx_from
    ,cast(NULL as string) as top_level_tx_to
    ,cast(NULL as string) as top_level_tx_method_id
    ,cast(NULL as string) as created_tx_from
    ,cast(NULL as string) as created_tx_to
    ,cast(NULL as string) as created_tx_method_id
  from {{ source('ovm1_optimism', 'synthetix_genesis_contracts') }} as snx
  where 
    true
    {% if is_incremental() %} -- this filter will only be applied on an incremental run 
    and not exists (
      select 1 
      from {{ this }} as gc
      where 
        gc.contract_address = snx.contract_address
        and gc.contract_project LIKE 'Synthetix%' --future proof in case this name changes
    )
    {% endif %}
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10

    union all 
  --uniswap pools from ovm1

  select 
     cast(NULL as string) as trace_creator_address
    ,cast(NULL as string) as creator_address
    ,cast(NULL as string) as contract_factory
    ,lower(newaddress) as contract_address
    ,'Uniswap' as contract_project
    ,'Pair' as contract_name
    ,to_timestamp('2021-11-11 00:00:00') as created_time
    ,false as is_self_destruct
    ,'ovm1 uniswap pools' as source
    ,cast(NULL as string) as creation_tx_hash
    ,cast(NULL as bigint) as created_block_number
    ,cast(NULL as string) as top_level_tx_from
    ,cast(NULL as string) as top_level_tx_to
    ,cast(NULL as string) as top_level_tx_method_id
    ,cast(NULL as string) as created_tx_from
    ,cast(NULL as string) as created_tx_to
    ,cast(NULL as string) as created_tx_method_id
  from {{ ref('uniswap_optimism_ovm1_pool_mapping') }} as uni
  where 
    true
    {% if is_incremental() %} -- this filter will only be applied on an incremental run 
    and not exists (
      select 1 
      from {{ this }} as gc
      where 
        gc.contract_address = lower(newaddress)
        and gc.contract_project LIKE 'Uniswap%' --future proof in case this name changes
    )
    {% endif %}
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
)
,get_contracts as (
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
    ,c.top_level_tx_from
    ,c.top_level_tx_to
    ,c.top_level_tx_method_id
    ,c.created_tx_from
    ,c.created_tx_to
    ,c.created_tx_method_id
  from combine as c 
  left join tokens as t 
    on c.contract_address = t.contract_address
  group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
)
,cleanup as (
--grab the first non-null value for each, i.e. if we have the contract via both contract mapping and optimism.contracts
  select
    contract_address
    {% for col in cols %}
    ,(array_agg({{ col }}) filter (where {{ col }} is not NULL))[0] as {{ col }}
    {% endfor %}
  from get_contracts
  where contract_address is not NULL 
  group by 1
)
select 
  c.trace_creator_address
  ,c.contract_address
  ,cast(initcap(
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
    )
   ) as varchar(250)) as contract_project
  ,c.token_symbol
  ,cast( coalesce(co.contract_name, c.contract_name) as varchar(250)) as contract_name
  ,coalesce(c.creator_address, ovm1c.creator_address) as creator_address
  ,coalesce(c.created_time, to_timestamp(ovm1c.created_time)) as created_time
  ,coalesce(c.contract_factory, 
  {% if is_incremental() %}
    th.contract_creator_if_factory
    {% else -%}
    NULL
  {% endif %}
  ) as contract_creator_if_factory
  ,coalesce(c.is_self_destruct, false) as is_self_destruct
  ,c.creation_tx_hash
  ,c.created_block_number
  ,c.top_level_tx_from
  ,c.top_level_tx_to
  ,c.top_level_tx_method_id
  ,c.created_tx_from
  ,c.created_tx_to
  ,c.created_tx_method_id
from cleanup as c 
left join {{ source('ovm1_optimism', 'contracts') }} as ovm1c
  on c.contract_address = ovm1c.contract_address --fill in any missing contract creators
left join {{ ref('contracts_optimism_project_name_mappings') }} as dnm -- fix names for decoded contracts
  on lower(c.contract_project) = lower(dnm.dune_name)
left join {{ ref('contracts_optimism_contract_overrides') }} as co --override contract maps
  on lower(c.contract_address) = lower(co.contract_address)
{% if is_incremental() %} -- this filter will only be applied on an incremental run 
left join {{ this }} th -- grab if the contract was previously picked up as factory created
  ON lower(th.contract_address) = lower(c.contract_address)
{% endif %}
