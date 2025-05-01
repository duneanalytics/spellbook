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
-- We're selecting from another chain's contract mapping with LIMIT 0 to ensure type compatibility
-- and then replacing the blockchain name with 'scroll'

WITH base_data AS (
  SELECT *
  FROM {{ ref('contracts_zksync_contract_mapping') }}
  LIMIT 0
)

SELECT
  created_month,
  'scroll' as blockchain, -- Replace the blockchain name
  trace_creator_address,
  contract_address,
  contract_project,
  token_symbol,
  contract_name,
  creator_address,
  trace_deployer_address,
  created_time,
  is_self_destruct,
  created_tx_hash,
  created_block_number,
  created_tx_from,
  created_tx_to,
  created_tx_method_id,
  created_tx_index,
  top_level_contract_address,
  top_level_time,
  top_level_tx_hash,
  top_level_block_number,
  top_level_tx_from,
  top_level_tx_to,
  top_level_tx_method_id,
  code_bytelength,
  token_standard,
  code,
  code_deploy_rank_by_chain,
  is_eoa_deployed,
  is_smart_wallet_deployed,
  is_deterministic_deployer_deployed
FROM base_data 
