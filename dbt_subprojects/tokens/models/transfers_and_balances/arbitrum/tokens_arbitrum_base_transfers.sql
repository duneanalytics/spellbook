{{config(
    schema = 'tokens_arbitrum',
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
    blockchain='arbitrum',
    traces = source('arbitrum','traces'),
    transactions = source('arbitrum','transactions'),
    erc20_transfers = source('erc20_arbitrum','evt_Transfer')
)
}}
