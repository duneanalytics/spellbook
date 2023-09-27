{{ config(
    tags=['dunesql'],
    schema = 'transfers_bnb',
    materialized = 'incremental',
    partition_by = ['block_month'],
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['transfer_type', 'evt_tx_hash', 'evt_index', 'wallet_address'], 
    alias = alias('bep20'),
    post_hook='{{ expose_spells(\'["bnb"]\',
                                    "sector",
                                    "transfers",
                                    \'["soispoke", "dot2dotseurat", "tschubotz", "Henrystats"]\') }}'
    )
}}

{{
    transfers_erc20(
        blockchain = 'bnb',
        erc20_evt_transfer = source('erc20_bnb', 'evt_transfer'),
        wrapped_token_deposit = source('bnb_bnb', 'WBNB_evt_Deposit'),
        wrapped_token_withdrawal = source('bnb_bnb', 'WBNB_evt_Withdrawal')
    )
}}
