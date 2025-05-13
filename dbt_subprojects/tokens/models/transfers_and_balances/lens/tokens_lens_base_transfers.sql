{{config(
    schema = 'tokens_lens',
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
    blockchain='lens',
    traces = source('lens','traces'),
    transactions = source('lens','transactions'),
    erc20_transfers = source('erc20_lens','evt_Transfer'),
    native_contract_address = '0x000000000000000000000000000000000000800a',
    include_traces = false
)
}} 