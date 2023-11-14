{{ config(
        alias = 'base_balances_erc20',
        tags=['dunesql'],
        partition_by = ['block_date'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['block_number', 'tx_index', 'token_address', 'wallet_address'],
        )
}}

select * from (
{{balances_base(
    blockchain='ethereum',
    transfers_base = ref('tokens_ethereum_transfers_base'),
    token_standard = 'erc20',
)}}
)
