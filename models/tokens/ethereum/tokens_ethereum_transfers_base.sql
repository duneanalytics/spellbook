{{ config(
        alias = alias('transfers_base'),
        tags=['dunesql'],
        partition_by = ['block_date'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['block_number', 'tx_index', 'evt_index', 'trace_address'],
        )
}}

{{transfers_base(
    blockchain='ethereum',
    traces = source('ethereum','traces'),
    transactions = source('ethereum','transactions'),
    erc20_transfers = source('erc20_ethereum','evt_Transfer'),
)}}
