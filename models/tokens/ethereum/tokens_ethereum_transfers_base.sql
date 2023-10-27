{{ config(
        alias = 'transfers_base',
        tags=['dunesql'],
        partition_by = ['token_standard', 'block_date'],
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
    -- TODO: use variable here
    native_contract_address = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee',
    wrapped_token_deposit = source('zeroex_ethereum', 'weth9_evt_deposit'),
    wrapped_token_withdrawal = source('zeroex_ethereum', 'weth9_evt_withdrawal'),
)}}
