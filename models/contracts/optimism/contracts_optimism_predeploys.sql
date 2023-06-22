 {{
  config(
        alias='predeploys',
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
