 {{
  config(
        schema = 'ovm_optimism', 
        alias='contract_mapping',
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key='contract_address',
        post_hook='{{ expose_spells(\'["optimism"]\',
                                    "sector",
                                    "addresses",
                                    \'["msilb7", "chuxinh"]\') }}'
  )
}}

-- set max number of levels to trace root contract
{% set max_levels = 5 %}
-- set column names to loop through
{% set cols = [
    "contract_project"
    ,"token_symbol"
    ,"contract_name"
    ,"creator_address"
    ,"created_time"
    ,"contract_factory"
    ,"is_self_destruct"
    ,"creation_tx_hash"
] %}
-- TODO CHUXIN: ask how the backfilling of creator_rows work
with base_level as (
  select 
    creator_address
    ,contract_factory
    ,contract_address
    ,created_time
    ,creation_tx_hash
  from (
    select 
      ct.`from` as creator_address
      ,NULL::string as contract_factory
      ,ct.address as contract_address
      ,ct.block_time as created_time
      ,ct.tx_hash as creation_tx_hash
-- TODO CHUXIN: creation_traces does not have trace_address
-- we might need to swtich back to look at traces?
    from {{ source('optimism', 'creation_traces') }} as ct 
    where 
      true
    {% if is_incremental() %} -- this filter will only be applied on an incremental run 
      and block_time >= date_trunc('day', now() - interval '1 week')

    -- to get existing history of contract mapping
    union all 
    -- TODO CHUXIN: ask what are the joins to creator_row about
    select 
      creator_address
      ,contract_creator_if_factory as contract_factory
      ,contract_address
      ,created_time
      ,creation_tx_hash
    from {{ this }}
    {% endif %}
  ) as x
  group by 1, 2, 3, 4, 5
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
{% for i in range(max_levels) -%}
,level{{i}} as (
    select
      {{i}} as level 
      ,coalesce(b1.creator_address, b.creator_address) as creator_address
      {% if loop.first -%}
      ,case
        when b1.creator_address is NULL then NULL
        else b.creator_address
      end as contract_factory
      {% else -%}
      ,case
        when b1.creator_address is NULL then b.contract_factory
        else b.creator_address
      end as contract_factory
      {% endif %}
      ,b.contract_address
      ,b.created_time
      ,b.creation_tx_hash
      -- ,b.trace_element
    {% if loop.first -%}
    from base_level as b
    left join base_level as b1
      on b.creator_address = b1.contract_address
    {% else -%}
    from level{{i-1}} as b
    left join base_level as b1
      on b.creator_address = b1.contract_address
    {% endif %}
)
{%- endfor %}

,creator_contracts as (
  select 
    f.creator_address
    ,f.contract_factory
    ,f.contract_address
    ,coalesce(cc.project, ccf.project) as project 
    ,f.created_time
    ,case 
      when exists (
        select 1 
        from {{ source('optimism', 'traces') }} as sd 
        where 
          f.tx_hash = sd.tx_hash
          -- and f.trace_element = sd.trace_address[1]
          and sd."type" = 'suicide'
          {% if is_incremental() %} -- this filter will only be applied on an incremental run 
          and sd.block_time >= date_trunc('day', now() - interval '1 week')
          {% endif %}
      ) then true 
      else false 
    end as is_self_destruct
    ,f.tx_hash as creation_tx_hash 
  from level{{max_levels}} as f
  left join {{ ref('contract_creator_address_list') }} as cc 
    on f.creator_address = cc.creator_address
  left join {{ ref('contract_creator_address_list') }} as ccf
    on f.contract_factory = ccf.creator_address
  where f.contract_address is not null
)
,combine as (
  select 
    cc.creator_address
    ,cc.contract_factory
    ,cc.contract_address
    ,coalesce(cc.project, oc.namespace) as contract_project 
    ,oc.name as contract_name 
    ,cc.created_time
    ,coalesce(cc.is_self_destruct, false) as is_self_destruct
    ,'creator contracts' as source
    ,cc.creation_tx_hash
  from creator_contracts as cc 
  left join {{ source('optimism', 'contracts') }} as oc 
    on cc.contract_address = oc.address 

  union all 

  -- CHUXIN: is this part necessary?
  select 
    NULL as creator_address
    ,NULL as contract_factory
    ,oc.address as contract_address
    ,oc.namespace as contract_project
    ,oc.name as contract_name
    ,oc.created_at as created_time
    ,coalesce(cc.is_self_destruct, false) as is_self_destruct
    ,'decoded contracts' as source
    ,cc.creation_tx_hash
  from {{ source('optimism', 'contracts') }} as oc 
  join creator_contracts as cc 
    on oc.address = cc.contract_address
  where 
    true
    {% if is_incremental() %} -- this filter will only be applied on an incremental run 
    and created_at >= date_trunc('day', now() - interval '1 week')
    {% endif %}

  union all
  -- ovm 1.0 contracts

  select 
    creator_address
    ,NULL as contract_factory
    ,contract_address
    ,contract_project
    ,contract_name
    ,to_timestamp(created_time) as created_time
    ,false as is_self_destruct
    ,'ovm1 contracts' as source
    NULL as creation_tx_hash
  from {{ source('ovm1_optimism', 'contracts') }} as c
  where 
    not exists (
      select 1
      from {{ this }} as gc
      where 
        gc.contract_address = c.contract_address
        and (
          (gc.contract_project = c.contract_project) or (gc.contract_project is NULL)
        )
    )
    or c.contract_address in (select contract_address from creator_contracts)
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9

  union all 
  --synthetix genesis contracts

  select 
    NULL as creator_address
    ,NULL as contract_factory
    ,snx.contract_address
    ,'Synthetix' as contract_project
    ,contract_name
    ,to_timestamp('2021-07-06 00:00:00') as created_time
    ,false as is_self_destruct
    ,'synthetix contracts' as source
    ,NULL as creation_tx_hash
  from {{ source('ovm1_optimism', 'synthetix_genesis_contracts') }} as snx
  where 
    not exists (
      select 1 
      from {{ this }} as gc
      where 
        gc.contract_address = snx.contract_address
        and gc.contract_project = 'Synthetix'
    )
    or snx.contract_address in (select contract_address from creator_contracts)
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9
)
,get_contracts as (
  select 
    c.contract_address
    ,c.contract_factory
    ,c.contract_project
    ,t.symbol as token_symbol
    ,c.contract_name
    ,c.creator_address
    ,c.created_time 
    ,c.is_self_destruct
    c.creation_tx_hash
  from combine as c 
  left join tokens as t 
    on c.contract_address = tokens.contract_address
  group by 1, 2, 3, 4, 5, 6, 7, 8, 9
)
,cleanup as (
--grab the first non-null value for each, i.e. if we have the contract via both contract mapping and optimism.contracts
  select 
    contract_address
    {% for col in cols %}
    (array_agg({{ col }}) filter (where {{ col }} is not NULL))[1] as {{ col }}
    {% if not loop.last %}
        ,
    {% endif %}
    {% endfor %}
  from get_contracts
  where contract_address is not NULL 
)
select 
  c.contract_address
  ,initcap(
      replace(
      -- priority order: Override name, Mapped vs Dune, Raw/Actual names
        coalesce(
          co.project
          ,dnm.maapped_name
          ,c.contract_project
          ,ovm1c.contract_project
        ),
      '-',
      ' '
    )
   ) as contract_project
  ,c.token_symbol
  ,coalesce(co.contract_name, c.contract_name) as contract_name
  ,coalesce(c.creator_address, ovm1c.creator_address) as creator_address
  ,coalesce(c.created_time, to_timestamp(ovm1c.created_time)) as created_time
  ,contract_factory as contract_creator_if_factory
  ,coalesce(is_self_destruct, false) as is_self_destruct
  ,creation_tx_hash
from cleanup as c 
left join {{ source('ovm1_optimism', 'contracts') }} as ovm1c
  on c.contract_address = ovm1c.contract_address --fill in any missing contract creators
left join {{ ref('project_name_mappings') }} as dnm -- fix names for decoded contracts
  on lower(c.contract_project) = lower(dnm.dune_name)
left join {{ ref('contract_overrides') }} as co --override contract maps
  on c.contract_address = co.contract_address
