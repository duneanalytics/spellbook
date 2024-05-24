{{ config(
    
    materialized = 'incremental',
    partition_by = ['block_month'],
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['transfer_type', 'evt_tx_hash', 'evt_index', 'wallet_address'], 
    alias = 'erc20_v2',
    post_hook='{{ expose_spells(\'["gnosis"]\',
                                    "sector",
                                    "transfers",
                                    \'["soispoke", "dot2dotseurat", "tschubotz", "hosuke", "Henrystats", "hdser"]\') }}'
    )
}}


{{
    transfers_erc20(
        blockchain = 'gnosis',
        erc20_evt_transfer = source('erc20_gnosis', 'evt_transfer'),
        wrapped_token_deposit = source('wxdai_gnosis', 'WXDAI_evt_Deposit'),
        wrapped_token_withdrawal = source('wxdai_gnosis', 'WXDAI_evt_withdrawal')
    )
}}
