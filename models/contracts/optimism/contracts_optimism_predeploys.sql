 {{
  config(
        
        alias = 'predeploys',
        post_hook='{{ expose_spells(\'["optimism"]\',
                                    "sector",
                                    "contracts",
                                    \'["msilb7", "chuxin"]\') }}'
  )
}}

{% set cols = [
     "trace_creator_address"
    ,"contract_project"
    ,"contract_name"
    ,"creator_address"
    ,"created_time"
    ,"contract_factory"
    ,"is_self_destruct"
    ,"creation_tx_hash"
    ,"source"
] %}

with get_contracts AS (
SELECT *, ROW_NUMBER() OVER (PARTITION BY contract_address ORDER BY pref_rnk ASC) AS c_rank
FROM (
  -- other predeploys
  select 
     cast(NULL as varbinary) as trace_creator_address
    ,cast(NULL as varbinary) as creator_address
    ,cast(NULL as varbinary) as contract_factory
    ,contract_address
    ,project_name as contract_project
    ,contract_name
    ,from_iso8601_timestamp( '2021-07-06' ) as created_time
    ,false as is_self_destruct
    ,'other predeploys' as source
    ,cast(NULL as varbinary) as creation_tx_hash
    , 1 as pref_rnk
  from {{ ref('contracts_optimism_system_predeploys') }} as c
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
  
  union all
  -- ovm 1.0 contracts

  select 
     creator_address AS trace_creator_address
    ,creator_address
    ,cast(NULL as varbinary) as contract_factory
    ,contract_address
    ,contract_project
    ,contract_name
    ,from_iso8601_timestamp( created_time ) AS created_time
    ,false as is_self_destruct
    ,'ovm1 contracts' as source
    ,cast(NULL as varbinary) as creation_tx_hash
    , 2 as pref_rnk
  from {{ source('ovm1_optimism', 'contracts') }} as c
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10,11

  union all 
  --synthetix genesis contracts

  select 
     cast(NULL as varbinary) as trace_creator_address
    ,cast(NULL as varbinary) as creator_address
    ,cast(NULL as varbinary) as contract_factory
    ,snx.contract_address
    ,'Synthetix' as contract_project
    ,contract_name
    ,from_iso8601_timestamp( '2021-07-06' ) as created_time
    ,false as is_self_destruct
    ,'synthetix contracts' as source
    ,cast(NULL as varbinary) as creation_tx_hash
    , 3 as pref_rnk
  from {{ source('ovm1_optimism', 'synthetix_genesis_contracts') }} as snx

    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10,11

    union all 
  --uniswap pools from ovm1

  select 
     cast(NULL as varbinary) as trace_creator_address
    ,cast(NULL as varbinary) as creator_address
    ,cast(NULL as varbinary) as contract_factory
    ,newaddress as contract_address
    ,'Uniswap' as contract_project
    ,'Pair' as contract_name
    ,from_iso8601_timestamp( '2021-11-11' ) as created_time
    ,false as is_self_destruct
    ,'ovm1 uniswap pools' as source
    ,cast(NULL as varbinary) as creation_tx_hash
    , 4 as pref_rnk
  from {{ ref('uniswap_optimism_ovm1_pool_mapping') }} as uni

    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
  ) a
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
  AND c_rank = 1 -- get first instance, no dupes
  group by 1
)

SELECT
  trace_creator_address,  contract_address, 
  --initcap: https://jordanlamborn.medium.com/presto-sql-proper-case-initcap-how-to-capitalize-the-first-letter-of-each-word-in-presto-5fbac3f0154c
  (array_join((transform((split(lower(contract_project),' '))
    , x -> concat(upper(substr(x,1,1)),substr(x,2,length(x))))),' ',''))
  AS contract_project
  --
, contract_name, creator_address, created_time, contract_creator_if_factory
, is_self_destruct, creation_tx_hash, source
FROM (
  select 
    c.trace_creator_address
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
      )
    as varchar) as contract_project
    ,cast( coalesce(co.contract_name, c.contract_name) as varchar) as contract_name
    ,coalesce(c.creator_address, ovm1c.creator_address) as creator_address
    ,coalesce(c.created_time, from_iso8601_timestamp(ovm1c.created_time) ) as created_time
    ,c.contract_factory as contract_creator_if_factory
    ,coalesce(c.is_self_destruct, false) as is_self_destruct
    ,c.creation_tx_hash
    ,c.source
  from cleanup as c 

  left join {{ source('ovm1_optimism', 'contracts') }} as ovm1c
    on c.contract_address = ovm1c.contract_address --fill in any missing contract creators
  left join {{ ref('contracts_optimism_project_name_mappings') }} as dnm -- fix names for decoded contracts
    on lower(c.contract_project) = lower(dnm.dune_name)
  left join {{ ref('contracts_optimism_contract_overrides') }} as co --override contract maps
    on c.contract_address = co.contract_address
) f
