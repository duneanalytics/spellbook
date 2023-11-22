{{config(
    schema = 'tokens_zksync',
    alias = 'base_transfers',
    partition_by = ['token_standard', 'block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['unique_key'],
)
}}

{{transfers_base(
    blockchain='zksync',
    traces = source('zksync','traces'),
    transactions = source('zksync','transactions'),
    erc20_transfers = source('erc20_zksync','evt_transfer'),
    native_contract_address = '0x000000000000000000000000000000000000800a'
)
}}
