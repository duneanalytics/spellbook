{{ config(
    tags=['dunesql'],
    schema = 'transfers_arbitrum',
    materialized = 'incremental',
    partition_by = ['block_month'],
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['transfer_type', 'evt_tx_hash', 'evt_index', 'wallet_address'], 
    alias = alias('erc20'),
    post_hook='{{ expose_spells(\'["arbitrum"]\',
                                    "sector",
                                    "transfers",
                                    \'["Henrystats"]\') }}') 
}}

{{
    transfers_erc20(
        blockchain = 'arbitrum',
        erc20_evt_transfer = source('erc20_arbitrum', 'evt_transfer')
    )
}}