{{ config(
        alias = alias('balances_base'),
        tags=['dunesql'],
        partition_by = ['block_date'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['block_number', 'tx_index', 'evt_index', 'trace_address'],
        pre_hook='{{ enforce_join_distribution("PARTITIONED") }}'
        )
}}

{{balances_base(
    blockchain='ethereum',
    transfers_base = ref('tokens_ethereum_transfers_base'),
)}}
