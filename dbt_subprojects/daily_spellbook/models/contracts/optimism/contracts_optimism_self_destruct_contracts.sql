 {{
  config(
        schema = 'contracts_optimism',
        alias = 'self_destruct_contracts',
        unique_key='contract_address'
  )
}}


-- Keep this table alive for backwards compatability, to not break queries this is used in
-- But this will be a view of existing tables, and not require any additional builds

SELECT 
  created_time, created_block_number, created_tx_hash as creation_tx_hash
  , contract_address
  , array[cast( null as varbinary)] AS trace_element

  FROM {{ ref('contracts_optimism_find_self_destruct_contracts')}}
