{{ 
  config(
    alias='contract_overrides',
    unique_key='contract_address',
    post_hook='{{ expose_spells(\'["optimism"]\',
                              "sector",
                              "contracts",
                              \'["msilb7", "chuxinh"]\') }}'
    ) 
}}

select 
  lower(contract_address) as contract_address
  ,contract_project
  ,contract_name
from 
    (values 
    ('0xc30141B657f4216252dc59Af2e7CdB9D8792e1B0', 'Socket', 'Socket Registry')
    ,('0x81b30ff521D1fEB67EDE32db726D95714eb00637', 'Optimistic Explorer', 'OptimisticExplorerNFT')
    ,('0x998EF16Ea4111094EB5eE72fC2c6f4e6E8647666', 'Quix', 'Seaport')
    ) as temp_table(contract_address, contract_project, contract_name)
