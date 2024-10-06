{{config(
    schema = 'tokens_bob',
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
    blockchain='bob',
    traces = source('bob','traces'),
    transactions = source('bob','transactions'),
    erc20_transfers = source('erc20_bob','evt_transfer'),
    native_contract_address = null
)
}}
