{{ config(
        alias = 'balances_base_erc20',
        tags=['dunesql'],
        partition_by = ['block_date'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['block_number', 'tx_index', 'token_address', 'wallet_address'],
        pre_hook='{{ enforce_join_distribution("PARTITIONED") }}'
        )
}}

select * from (
{{balances_base(
    blockchain='ethereum',
    transfers_base = ref('tokens_ethereum_transfers_base'),
    token_standard = 'erc20',
)}}
)
