{{config(
    schema = 'tokens_base',
    alias = 'base_transfers',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['block_date','unique_key'],
)
}}

{{transfers_base(
    blockchain='base',
    traces = source('base','traces'),
    transactions = source('base','transactions'),
    erc20_transfers = source('erc20_base','evt_transfer'),
    native_contract_address = null
)
}}
