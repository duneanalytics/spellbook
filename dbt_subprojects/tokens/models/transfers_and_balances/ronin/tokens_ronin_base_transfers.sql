{{config(
    schema = 'tokens_ronin',
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
    blockchain='ronin',
    traces = source('ronin','traces'),
    transactions = source('ronin','transactions'),
    erc20_transfers = source('erc20_ronin','evt_transfer'),
    native_contract_address = null
)
}}
