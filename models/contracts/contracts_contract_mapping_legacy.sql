 {{
  config(
	tags=['legacy'],
        schema = 'contracts',
        alias = alias('contract_mapping', legacy_model=True)
  )
}}


SELECT
  1 
--   as trace_creator_address,  1 as contract_address, 

--   1 as contract_project
--   --
-- , 1 as token_symbol
-- , 1 as contract_name, 1 as creator_address, 1 as created_time, 1 as contract_creator_if_factory
-- , 1 as is_self_destruct, 1 as creation_tx_hash, 1 as created_block_number, 1 as created_tx_from
-- , 1 as created_tx_to, 1 as created_tx_method_id, 1 as created_tx_index
-- , 1 as top_level_time, 1 as top_level_tx_hash, 1 as top_level_block_number
-- , 1 as top_level_tx_from, 1 as top_level_tx_to , 1 as top_level_tx_method_id
-- , 1 as code_bytelength , 1 as token_standard , 1 as code_deploy_rank, 1 as is_eoa_deployed
