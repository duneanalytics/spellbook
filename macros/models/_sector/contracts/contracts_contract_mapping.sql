{% macro contracts_contract_mapping( chain, standard_name = 'erc' ) %}

-- set column names to loop through
{% set cols = [
     "trace_creator_address"
    ,"contract_project"
    ,"token_symbol"
    ,"contract_name"
    ,"creator_address"
    ,"trace_deployer_address"
    ,"created_time"
    ,"created_month"
    ,"created_block_number"
    ,"created_tx_hash"
    ,"top_level_contract_address"
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
    ,"is_self_destruct"
] %}

WITH get_contracts as (
  --grab the first non-null value for each, i.e. if we have the contract via both contract mapping and optimism.contracts
  select
    blockchain
    ,contract_address
    {% for col in cols %}
    , (array_agg({{ col }} ORDER BY map_rank ASC NULLS LAST)
      filter (where {{ col }} is not NULL))[1]
    as {{ col }}
    {% endfor %}
  FROM (
  select 
    c.blockchain
    ,c.trace_creator_address
    ,c.contract_address
    ,coalesce(cc.contract_project, ccd.contract_project, cctr.contract_project, c.contract_project, oc.namespace) as contract_project
    ,COALESCE(c.contract_name, oc.name) AS contract_name
    ,t_mapped.symbol as token_symbol
    ,c.creator_address
    ,c.trace_deployer_address
    ,c.created_time 
    ,c.created_month

    
    ,c.created_tx_hash
    ,c.created_block_number
    ,c.created_tx_from
    ,c.created_tx_to
    ,c.created_tx_method_id
    ,c.created_tx_index

    ,c.top_level_contract_address
    ,c.top_level_time
    ,c.top_level_tx_hash
    ,c.top_level_block_number
    ,c.top_level_tx_from
    ,c.top_level_tx_to
    ,c.top_level_tx_method_id

    ,c.code_bytelength
    ,COALESCE(t_mapped.token_standard, c.token_standard) AS token_standard
    ,c.code
    ,c.code_deploy_rank_by_chain
    ,c.is_self_destruct
    ,map_rank
  from 
    (
      SELECT
      blockchain, trace_creator_address, contract_address, creator_address, trace_deployer_address,created_time, created_month
      ,created_tx_hash, created_block_number, created_tx_from, created_tx_to, created_tx_method_id, created_tx_index
      ,top_level_contract_address,top_level_time, top_level_tx_hash, top_level_block_number, top_level_tx_from, top_level_tx_to, top_level_tx_method_id
      ,code_bytelength, token_standard_erc20 AS token_standard, code, code_deploy_rank_by_chain, is_self_destruct
      , CAST(NULL as varchar) AS contract_project, cast(NULL as varchar) AS contract_name
      ,1 as map_rank
      FROM {{ ref('contracts_' + chain + '_base_iterated_creators') }} b

      UNION ALL 

      SELECT
      p.blockchain, trace_creator_address, contract_address, creator_address, creator_address AS trace_deployer_address, created_time, DATE_TRUNC('month',created_time) AS created_month
      ,created_tx_hash, 0 AS created_block_number, NULL AS created_tx_from, NULL AS created_tx_to, NULL AS created_tx_method_id, NULL AS created_tx_index
      ,contract_address AS top_level_contract_address,NULL AS top_level_time, NULL AS top_level_tx_hash, NULL AS top_level_block_number, NULL AS top_level_tx_from, NULL AS top_level_tx_to, NULL AS top_level_tx_method_id
      ,bytearray_length(oc.code) AS code_bytelength, NULL AS token_standard, oc.code, NULL AS code_deploy_rank_by_chain, NULL AS is_self_destruct
      , p.contract_project, p.contract_name
      ,2 as map_rank

      FROM {{ ref('contracts_predeploys')}} p
        LEFT JOIN {{ source(chain,'contracts')}} oc
          ON p.contract_address = oc.address
      WHERE p.blockchain = '{{chain}}'

    ) c
  left join {{ ref('contracts_contract_creator_address_list') }} as cc 
    on c.creator_address = cc.creator_address
  left join {{ ref('contracts_contract_creator_address_list') }} as ccd
    on c.trace_creator_address = ccd.creator_address
    AND cc.creator_address IS NULL
  left join {{ ref('contracts_contract_creator_address_list') }} as cctr
    on c.trace_deployer_address = cctr.creator_address
    AND ccd.creator_address IS NULL
  left join {{ source(chain,'contracts') }} oc
    ON c.contract_address = oc.address
  left join (
        select
          '{{chain}}' as blockchain, e.contract_address, e.symbol, 'erc20' as token_standard
        FROM {{ source('tokens_' + chain, standard_name + '20')}} e --note: This doesn't yet contain all ERC20 tokens
        -- WHERE e.blockchain = '{{chain}}'
        GROUP BY 1,2,3,4
      UNION ALL
        select 
          '{{chain}}' as bblockchain ,t.contract_address ,t.name as symbol, standard AS token_standard
        from {{ ref('tokens_' + chain + '_nft') }} as t --chain-specific NFT model
        -- WHERE t.blockchain = '{{chain}}'
        group by 1, 2, 3, 4
      ) as t_mapped
    on c.contract_address = t_mapped.contract_address
    AND c.blockchain = t_mapped.blockchain
  group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29
  ) a
  where contract_address is not NULL 
  group by 1,2
)


SELECT
    u.created_month,
    u.blockchain,
    u.trace_creator_address,  u.contract_address
  , u.contract_project
  , u.token_symbol
  , u.contract_name, u.creator_address, u.trace_deployer_address, u.created_time
  , u.is_self_destruct
  , u.created_tx_hash, u.created_block_number, u.created_tx_from
  , u.created_tx_to, u.created_tx_method_id, u.created_tx_index
  , u.top_level_contract_address, u.top_level_time, u.top_level_tx_hash, u.top_level_block_number
  , u.top_level_tx_from, u.top_level_tx_to , u.top_level_tx_method_id
  , u.code_bytelength
  , u.token_standard
  , u.code
  , u.code_deploy_rank_by_chain
  , u.is_eoa_deployed
  , u.is_smart_wallet_deployed
  , u.is_deterministic_deployer_deployed

FROM (
  SELECT
    created_month
  , blockchain
  , trace_creator_address, contract_address
  , initcap(contract_project) AS contract_project
  , token_symbol
  , contract_name, creator_address, trace_deployer_address, created_time
  , created_tx_hash, created_block_number, created_tx_from
  , created_tx_to, created_tx_method_id, created_tx_index
  , top_level_contract_address, top_level_time, top_level_tx_hash, top_level_block_number
  , top_level_tx_from, top_level_tx_to , top_level_tx_method_id
  , code_bytelength , token_standard 
  , code
  , code_deploy_rank_by_chain
  , is_self_destruct
  , is_eoa_deployed
  , is_smart_wallet_deployed
  , is_deterministic_deployer_deployed

  FROM (
    select 
      cast( created_month as date) AS created_month
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
              ,(CASE WHEN cdc.creator_address IS NOT NULL THEN 'Deterministic Deployer' ELSE NULL END)
            ),
          '_',
          ' '
      ) as varchar) as contract_project
      ,c.token_symbol
      ,cast( coalesce(co.contract_name, c.contract_name, cdc.creator_name) as varchar) as contract_name
      ,c.creator_address
      ,c.trace_deployer_address
      ,c.created_time
      ,c.is_self_destruct
      ,c.created_tx_hash
      ,COALESCE(c.created_block_number,0) AS created_block_number
      ,c.created_tx_from
      ,c.created_tx_to
      ,c.created_tx_method_id
      ,c.created_tx_index

      ,c.top_level_contract_address
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
    left join {{ ref('contracts_deterministic_contract_creators') }} as cdc --map deterministic deployers
      on c.contract_address = cdc.creator_address
  ) f
) u

{% endmacro %}