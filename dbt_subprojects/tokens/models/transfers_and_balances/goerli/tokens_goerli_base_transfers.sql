{{config(
    schema = 'tokens_goerli',
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
    blockchain='goerli',
    traces = source('goerli','traces'),
    transactions = source('goerli','transactions'),
    erc20_transfers = source('erc20_goerli','evt_transfer'),
    native_contract_address = null
)
}}
