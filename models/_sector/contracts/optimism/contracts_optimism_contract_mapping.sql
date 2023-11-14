 {{
  config(
        schema = 'contracts_optimism',
        alias = 'contract_mapping',
        unique_key='contract_address',
        post_hook='{{ expose_spells(\'["optimism"]\',
                                    "sector",
                                    "contracts",
                                    \'["msilb7", "chuxin"]\') }}'
  )
}}

-- Keep this table alive for backwards compatability, to not break queries this is used in
-- But this will be a view of existing tables, and not require any additional builds

SELECT 
  trace_creator_address, contract_address, contract_project, token_symbol, contract_name
  , creator_address, created_time, creator_address as contract_creator_if_factory, is_self_destruct
  , created_tx_hash as creation_tx_hash, created_block_number, created_tx_from, created_tx_to, created_tx_method_id, created_tx_index
  , top_level_time, top_level_tx_hash, top_level_block_number, top_level_tx_from, top_level_tx_to, top_level_tx_method_id
  , code_bytelength, token_standard, code_deploy_rank_by_chain as code_deploy_rank, is_eoa_deployed

  FROM {{ ref('contracts_optimism_contract_creator_project_mapping')}}