{{config(
    schema = 'tokens_avalanche_c',
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
    blockchain='avalanche_c',
    traces = source('avalanche_c','traces'),
    transactions = source('avalanche_c','transactions'),
    erc20_transfers = source('erc20_avalanche_c','evt_transfer'),
    native_contract_address = null
)
}}
