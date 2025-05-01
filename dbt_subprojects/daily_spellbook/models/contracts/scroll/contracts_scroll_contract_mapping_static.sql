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
  '2023-01-01'::date as created_month,
  'scroll' as blockchain,
  NULL as trace_creator_address,
  '0x0000000000000000000000000000000000000000'::varbinary as contract_address,
  'Scroll' as contract_project,
  'SCROLL' as token_symbol,
  'Scroll Token' as contract_name,
  NULL as creator_address,
  NULL as trace_deployer_address,
  current_timestamp as created_time,
  false as is_self_destruct,
  NULL as created_tx_hash,
  0 as created_block_number,
  NULL as created_tx_from,
  NULL as created_tx_to,
  NULL as created_tx_method_id,
  NULL as created_tx_index,
  NULL as top_level_contract_address,
  NULL as top_level_time,
  NULL as top_level_tx_hash,
  NULL as top_level_block_number,
  NULL as top_level_tx_from,
  NULL as top_level_tx_to,
  NULL as top_level_tx_method_id,
  0 as code_bytelength,
  'erc20' as token_standard,
  NULL as code,
  NULL as code_deploy_rank_by_chain,
  0 as is_eoa_deployed,
  0 as is_smart_wallet_deployed,
  0 as is_deterministic_deployer_deployed

WHERE false -- Empty table but with the right schema 
