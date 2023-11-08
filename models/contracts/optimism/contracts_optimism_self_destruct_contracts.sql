 {{
  config(
        
        alias = 'self_destruct_contracts',
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
  created_time, created_block_number, created_tx_hash as creation_tx_hash
  , contract_address, NULL AS trace_element

  FROM {{ ref('contracts_optimism_find_self_destruct_contracts')}}
