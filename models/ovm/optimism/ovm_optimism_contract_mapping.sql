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

with base_level as (
    select 
      ct.`from` as creator_address
      ,NULL::string as contract_factory
      ,ct.address as contract_address
      ,ct.block_time as created_time
      ,ct.tx_hash
    from {{ source('optimism', 'creation_traces') }} as ct 
    where 
      true
      {% if is_incremental() %} -- this filter will only be applied on an incremental run 
      and block_time >= date_trunc('day', now() - interval '1 week')
      {% endif %}

    -- to get existing history of contract mapping
    union all 

    select 
      creator_address
      ,contract_factory
      ,contract_address
      ,created_time
      ,tx_hash
    from {{ this }}
)
,tokens as (
  select 
    bl.contract_address
    ,t.symbol
  from base_level as bl 
  join {{ ref('tokens_optimism_erc20') }} as t
    on bl."contract_address" = t."contract_address"
  group by 1, 2

  union all 

  select 
    bl.contract_address
    ,t.project_name as symbol
  from base_level as bl 
  join {{ ref('tokens_optimism_nft') }} as t
    on bl."contract_address" = t."contract_address"
  group by 1, 2
)

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
      ,b.block_time
      ,b.tx_hash
      ,b.trace_element
    from base_level as b
    {% if loop.first -%}
    left join base_level as b1
      on b.creator_address = b1.contract_address
    {% else -%}
    left join level{{i-1}} as b1
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
,get_contracts as (
  select 
    cc.creator_address
    ,cc.contract_factory
    ,cc.contract_address
    ,coalesce(cc.project, oc.namespace) as project 
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
    ,oc.namespace as project
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
  from {{ source('optimism', 'contracts') }}

)