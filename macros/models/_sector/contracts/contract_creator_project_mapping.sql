{% macro contract_creator_project_mapping_by_chain( chain ) %}

-- set column names to loop through
{% set cols = [
     "trace_creator_address"
    ,"contract_project"
    ,"token_symbol"
    ,"contract_name"
    ,"creator_address"
    ,"deployer_address"
    ,"created_time"
    ,"created_month"
    ,"created_block_number"
    ,"created_tx_hash"
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
    ,t_mapped.symbol as token_symbol
    ,c.creator_address
    ,c.contract_project
    ,c.contract_name
    ,c.deployer_address
    ,c.created_time 
    ,c.created_month

    ,c.created_tx_hash
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
    ,COALESCE(t_mapped.token_standard, t_raw.token_standard) AS token_standard
    ,c.code
    ,c.code_deploy_rank_by_chain
    ,MIN(c.map_rank) AS map_rank

  from {{ ref('contracts_' + chain + '_contract_creator_project_intermediate_contracts') }} as c 
  left join (
        select
          '{{chain}}' as blockchain, e.contract_address, e.symbol, 'erc20' as token_standard
        FROM {{ ref('tokens_' + chain + '_erc20')}} e --note: This doesn't yet contain all ERC20 tokens
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
  left join ( --ideally, we have an 'all tokens spell' to read from (pending Dune team?), until then:
          SELECT contract_address
            , MIN(min_block_number) AS min_block_number
            , MAX_BY(token_standard, LENGTH(token_standard)) AS token_standard
          FROM (
            -- We have an all NFTs table, but don't yet hand an all ERC20s table
            SELECT contract_address, MIN(evt_block_number) AS min_block_number, 'erc20' as token_standard
            FROM {{source('erc20_' + chain, 'evt_transfer')}} r
            WHERE 1=1
            AND r.contract_address NOT IN (SELECT contract_address FROM {{ ref('tokens_erc20')}} WHERE  blockchain = '{{chain}}')
            {% if is_incremental() %} -- this filter will only be applied on an incremental run 
            AND {{ incremental_predicate('r.evt_block_time') }}
            {% endif %}
            group by 1
          ) ts 
          GROUP BY 1
        ) as t_raw
        on c.contract_address = t_raw.contract_address
        AND c.created_block_number <= t_raw.min_block_number
        AND t_mapped.contract_address IS NULL
  group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26
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
  , u.contract_name, u.creator_address, u.deployer_address, u.created_time
  , u.is_self_destruct
  , u.created_tx_hash, u.created_block_number, u.created_tx_from
  , u.created_tx_to, u.created_tx_method_id, u.created_tx_index
  , u.top_level_time, u.top_level_tx_hash, u.top_level_block_number
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
  , contract_name, creator_address, deployer_address, created_time
  , is_self_destruct
  , created_tx_hash, created_block_number, created_tx_from
  , created_tx_to, created_tx_method_id, created_tx_index
  , top_level_time, top_level_tx_hash, top_level_block_number
  , top_level_tx_from, top_level_tx_to , top_level_tx_method_id
  , code_bytelength , token_standard 
  , code
  , code_deploy_rank_by_chain
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
      ,c.deployer_address
      ,c.created_time
      ,CASE WHEN is_self_destruct = true then is_self_destruct ELSE
          (CASE WHEN sd.contract_address IS NOT NULL THEN true ELSE false END)
        END as is_self_destruct
      ,c.created_tx_hash
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
    left join {{ ref('contracts_deterministic_contract_creators') }} as cdc --map deterministic deployers
      on c.contract_address = cdc.creator_address
    left join {{ ref('contracts_'+ chain +'_find_self_destruct_contracts') }} as sd 
      on c.contract_address = sd.contract_address
      AND c.blockchain = sd.blockchain
      and c.created_tx_hash = sd.created_tx_hash
      AND c.created_block_number = sd.created_block_number
      {% if is_incremental() %} -- this filter will only be applied on an incremental run 
      AND {{ incremental_predicate('sd.created_block_time') }}
      {% endif %}
  ) f
) u

{% endmacro %}