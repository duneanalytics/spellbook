{{
  config(
    schema = 'contracts_arc',
    alias = 'deployments',
    materialized = 'incremental',
    file_format = 'delta',
    partition_by = ['deployment_block_month'],
    incremental_strategy = 'merge',
    unique_key = ['blockchain', 'contract_address', 'deployment_tx_hash', 'deployment_block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.deployment_block_time')]
  )
}}

{{ contracts_deployments(blockchain='arc') }}
