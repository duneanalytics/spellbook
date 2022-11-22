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

with base_level as (
  select 
    creator_address
    ,contract_factory
    ,contract_address
    ,created_time
    ,creation_tx_hash
    ,is_self_destruct
  from (
    select 
      ct.`from` as creator_address
      ,NULL::string as contract_factory
      ,ct.address as contract_address
      ,ct.block_time as created_time
      ,ct.tx_hash as creation_tx_hash
      ,coalesce(sd.contract_address is not NULL, false) as is_self_destruct
    from {{ source('optimism', 'creation_traces') }} as ct 
    left join {{ ref('contracts_optimism_self_destruct_contracts') }} as sd 
      on ct.address = sd.contract_address
      and ct.tx_hash = sd.creation_tx_hash
      and ct.block_time = sd.created_time
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
    from {{ this }}
    {% endif %}
  ) as x
  group by 1, 2, 3, 4, 5, 6
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
      ,coalesce(u.creator_address, b.creator_address) as creator_address
      {% if loop.first -%}
      ,case
        when u.creator_address is NULL then NULL
        else b.creator_address
      end as contract_factory
      {% else -%}
      ,case
        when u.creator_address is NULL then b.contract_factory
        else b.creator_address
      end as contract_factory
      {% endif %}
      ,b.contract_address
      ,b.created_time
      ,b.creation_tx_hash
      ,b.is_self_destruct
    {% if loop.first -%}
    from base_level as b
    left join base_level as u
      on b.creator_address = u.contract_address
    {% else -%}
    from level{{i-1}} as b
    left join base_level as u
      on b.creator_address = u.contract_address
    {% endif %}
)
{%- endfor %}

,creator_contracts as (
  select 
    f.creator_address
    ,f.contract_factory
    ,f.contract_address
    ,coalesce(cc.contract_project, ccf.contract_project) as contract_project 
    ,f.created_time
    ,f.is_self_destruct
    ,f.creation_tx_hash
  from level{{max_levels - 1}} as f
  left join {{ ref('contracts_optimism_contract_creator_address_list') }} as cc 
    on f.creator_address = cc.creator_address
  left join {{ ref('contracts_optimism_contract_creator_address_list') }} as ccf
    on f.contract_factory = ccf.creator_address
  where f.contract_address is not null
 )
,combine as (
  select 
    cc.creator_address
    ,cc.contract_factory
    ,cc.contract_address
    ,coalesce(cc.contract_project, oc.namespace) as contract_project 
    ,oc.name as contract_name 
    ,cc.created_time
    ,coalesce(cc.is_self_destruct, false) as is_self_destruct
    ,'creator contracts' as source
    ,cc.creation_tx_hash
  from creator_contracts as cc 
  left join {{ source('optimism', 'contracts') }} as oc 
    on cc.contract_address = oc.address 

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
    ,NULL as creation_tx_hash
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
    true
    {% if is_incremental() %} -- this filter will only be applied on an incremental run 
    and not exists (
      select 1 
      from {{ this }} as gc
      where 
        gc.contract_address = snx.contract_address
        and gc.contract_project = 'Synthetix'
    )
    {% endif %}
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
    ,c.creation_tx_hash
  from combine as c 
  left join tokens as t 
    on c.contract_address = t.contract_address
  group by 1, 2, 3, 4, 5, 6, 7, 8, 9
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
  c.contract_address
  ,initcap(
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
   ) as contract_project
  ,c.token_symbol
  ,coalesce(co.contract_name, c.contract_name) as contract_name
  ,coalesce(c.creator_address, ovm1c.creator_address) as creator_address
  ,coalesce(c.created_time, to_timestamp(ovm1c.created_time)) as created_time
  ,c.contract_factory as contract_creator_if_factory
  ,coalesce(c.is_self_destruct, false) as is_self_destruct
  ,c.creation_tx_hash
from cleanup as c 
left join {{ source('ovm1_optimism', 'contracts') }} as ovm1c
  on c.contract_address = ovm1c.contract_address --fill in any missing contract creators
left join {{ ref('contracts_optimism_project_name_mappings') }} as dnm -- fix names for decoded contracts
  on lower(c.contract_project) = lower(dnm.dune_name)
left join {{ ref('contracts_optimism_contract_overrides') }} as co --override contract maps
  on lower(c.contract_address) = lower(co.contract_address)
