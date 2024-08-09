{{config(
    schema = 'tokens_mantle',
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
    blockchain='mantle',
    traces = source('mantle','traces'),
    transactions = source('mantle','transactions'),
    erc20_transfers = source('erc20_mantle','evt_transfer'),
    native_contract_address = null
)
}}
