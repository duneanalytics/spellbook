{{
  config(
        schema = 'contracts_hyperevm',
        alias = 'deployments',
        materialized = 'incremental',
        file_format = 'delta',
        partition_by = ['deployment_block_month'],
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'contract_address', 'deployment_tx_hash'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.deployment_block_time')]
  )
}}

{{
    contracts_deployments(blockchain='hyperevm')
}}
