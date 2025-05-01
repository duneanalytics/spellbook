{{
  config(     
        schema = 'contracts_scroll',
        alias = 'contract_mapping_static',
        materialized ='table',
        partition_by =['created_month']
  )
}}

-- Static version of contract mapping for Scroll
-- This is a temporary solution until the tokens_scroll schema is properly set up
SELECT
  cast('2023-01-01' as date) as created_month,
  'scroll' as blockchain,
  cast(NULL as varbinary) as trace_creator_address,
  cast('0x0000000000000000000000000000000000000000' as varbinary) as contract_address,
  'Scroll' as contract_project,
  'SCROLL' as token_symbol,
  'Scroll Token' as contract_name,
  cast(NULL as varbinary) as creator_address,
  cast(NULL as varbinary) as trace_deployer_address,
  current_timestamp as created_time,
  false as is_self_destruct,
  cast(NULL as varbinary) as created_tx_hash,
  0 as created_block_number,
  cast(NULL as varbinary) as created_tx_from,
  cast(NULL as varbinary) as created_tx_to,
  cast(NULL as varchar) as created_tx_method_id,
  cast(NULL as bigint) as created_tx_index,
  cast(NULL as varbinary) as top_level_contract_address,
  cast(NULL as timestamp) as top_level_time,
  cast(NULL as varbinary) as top_level_tx_hash,
  cast(NULL as bigint) as top_level_block_number,
  cast(NULL as varbinary) as top_level_tx_from,
  cast(NULL as varbinary) as top_level_tx_to,
  cast(NULL as varchar) as top_level_tx_method_id,
  0 as code_bytelength,
  'erc20' as token_standard,
  cast(NULL as varbinary) as code,
  cast(NULL as bigint) as code_deploy_rank_by_chain,
  0 as is_eoa_deployed,
  0 as is_smart_wallet_deployed,
  0 as is_deterministic_deployer_deployed

WHERE false -- Empty table but with the right schema 
