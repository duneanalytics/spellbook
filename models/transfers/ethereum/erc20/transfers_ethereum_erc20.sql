{{ config(
    schema = 'transfers_ethereum',
    tags=['dunesql'],
    materialized = 'incremental',
    partition_by = ['block_month'],
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['transfer_type', 'evt_tx_hash', 'evt_index', 'wallet_address'], 
    alias = alias('erc20'),
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "sector",
                                    "transfers",
                                    \'["soispoke", "dot2dotseurat", "Henrystats"]\') }}'
    )
}}


{{
    transfers_erc20(
        blockchain = 'ethereum',
        erc20_evt_transfer = source('erc20_ethereum', 'evt_transfer'),
        wrapped_token_deposit = source('zeroex_ethereum', 'weth9_evt_deposit'),
        wrapped_token_withdrawal = source('zeroex_ethereum', 'weth9_evt_withdrawal')
    )
}}
