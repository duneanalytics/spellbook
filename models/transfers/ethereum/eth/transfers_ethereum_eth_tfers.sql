{{ config(
    tags=['dunesql'],
    schema = 'transfers_ethereum_eth',
    materialized = 'incremental',
    partition_by = ['block_month'],
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['transfer_type', 'tx_hash', 'trace_address', 'wallet_address', 'block_time'], 
    alias = alias('eth_tfers'),
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "sector",
                                    "transfers",
                                    \'["Henrystats"]\') }}') 
}}

{{
    transfers_native(
        blockchain = 'ethereum',
        traces = source('ethereum', 'traces'),
        transactions = source('ethereum', 'transactions'),
        native_token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
        genesis_balances = ref('genesis_balances'),
        staking_withdrawals = source('ethereum', 'withdrawals'),
        contract_creation_deposit = true
    )
}}
