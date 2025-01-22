
{{ config(
        schema = 'evms',
        tags = ['static'],
        alias = 'base_evm_smart_account_method_ids'
        )
}}

 --If Null, make an entry for all chains
SELECT
    cast(NULL as varchar) AS blockchain, method_id, method_descriptor, contract_project
    FROM (values
         (0x6a761202, 'execTransaction', 'Gnosis Safe')
        ,(0x1fad948c, 'handleOps', 'Erc4337')
        ) a (method_id, method_descriptor, contract_project)
-- UNION ALL