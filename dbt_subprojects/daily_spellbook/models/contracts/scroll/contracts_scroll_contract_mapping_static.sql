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
-- Start with a template with the correct data types
WITH sample_data AS (
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
      cast(0 as bigint) as created_block_number,
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
      cast(0 as bigint) as code_bytelength,
      cast('erc20' as varchar) as token_standard,
      cast(NULL as varbinary) as code,
      cast(NULL as bigint) as code_deploy_rank_by_chain,
      cast(0 as bigint) as is_eoa_deployed,
      cast(0 as bigint) as is_smart_wallet_deployed,
      cast(0 as bigint) as is_deterministic_deployer_deployed
)

-- Return an empty table with the correct structure
SELECT 
    created_month,
    blockchain,
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
FROM sample_data
WHERE 1=0 -- Empty table 
