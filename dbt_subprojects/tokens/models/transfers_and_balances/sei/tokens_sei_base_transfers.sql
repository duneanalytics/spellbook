{{config(
    schema = 'tokens_sei',
    alias = 'base_transfers',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['block_date','unique_key'],
)
}}

{{transfers_base(
    blockchain='sei',
    traces = source('sei','traces'),
    transactions = source('sei','transactions'),
    erc20_transfers = source('erc20_sei','evt_transfer'),
    native_contract_address = null
)
}}
