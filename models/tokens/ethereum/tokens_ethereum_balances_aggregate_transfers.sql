{{ config(
        alias = 'balances_transfers_aggregate',
        tags=['dunesql'],
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['block_number', 'tx_index', 'token_standard', 'token_address', 'wallet_address'],
        pre_hook='{{ enforce_join_distribution("PARTITIONED") }}'
        )
}}

{{balances_transfers_aggregate(
    blockchain='ethereum',
    transfers_base = ref('tokens_ethereum_transfers_base'),
)}}
