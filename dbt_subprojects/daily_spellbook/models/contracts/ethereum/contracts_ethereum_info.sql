{{
  config(
        schema = 'contracts_ethereum',
        alias = 'info',
        materialized = 'incremental',
        file_format = 'delta',
        partition_by = ['creation_block_month'],
        unique_key = ['blockchain', 'contract_address'],
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.creation_block_time')]
  )
}}

{{
    contracts_info(blockchain='ethereum')
}}