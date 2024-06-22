{{
    config(
        schema = 'contracts_celo',
        alias = 'base_starting_level',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'contract_address'],
        partition_by = ['created_month']
    )
}}

{{
    contracts_base_starting_level(
        chain = 'celo'
    )
}}
