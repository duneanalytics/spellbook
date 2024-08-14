{{config(
    schema = 'tokens_blast',
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
    blockchain='blast',
    traces = source('blast','traces'),
    transactions = source('blast','transactions'),
    erc20_transfers = source('erc20_blast','evt_transfer'),
    native_contract_address = null
)
}}
