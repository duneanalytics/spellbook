{{
    config(     
        schema = 'contracts_celo',
        alias = 'contract_mapping',
        materialized ='table',
        partition_by =['created_month']
    )
}}

{{
    contracts_contract_mapping(
        chain = 'celo'
    )
}}