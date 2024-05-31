{{ 
  config(
    tags = ['static'],
    schema = 'contracts_optimism',
    alias = 'contract_creator_address_list',
    unique_key='creator_address',
    post_hook='{{ expose_spells(\'["optimism"]\',
                              "sector",
                              "contracts",
                              \'["msilb7", "chuxin"]\') }}'
    )  
}}

-- Keep this table alive for backwards compatability, to not break queries this is used in
-- But this will be a view of existing tables, and not require any additional builds

SELECT creator_address, contract_project FROM {{ ref('contracts_contract_creator_address_list')}}
  
