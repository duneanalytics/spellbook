{{
    config(     
        schema = 'contracts_celo',
        alias = 'contract_mapping',
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key = ['blockchain', 'contract_address'],
        partition_by =['created_month']
    )
}}

{{contracts_contract_mapping(
    chain = 'celo'
)}}