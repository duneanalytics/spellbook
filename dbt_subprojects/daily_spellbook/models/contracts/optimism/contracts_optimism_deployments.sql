{{
  config(
        schema = 'contracts_optimism',
        alias = 'deployments',
        materialized = 'incremental',
        file_format = 'delta',
        partition_by = ['creation_block_month'],
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'contract_address', 'creation_tx_hash'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.creation_block_time')]
  )
}}

{{
    contracts_deployments(blockchain='optimism')
}}
