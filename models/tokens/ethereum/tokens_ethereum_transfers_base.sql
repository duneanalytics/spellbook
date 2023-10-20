{{ config(
        alias = alias('transfers_base'),
        tags=['dunesql'],
        partition_by = ['block_date'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['block_number', 'tx_index', 'evt_index', 'trace_address'],
        )
}}

{{transfers_base(
    blockchain='ethereum',
    traces = source('ethereum','traces'),
    transactions = source('ethereum','transactions'),
    erc20_transfers = source('erc20_ethereum','evt_Transfer'),
    wrapped_token_deposit = source('zeroex_ethereum', 'weth9_evt_deposit'),
    wrapped_token_withdrawal = source('zeroex_ethereum', 'weth9_evt_withdrawal'),
)}}
