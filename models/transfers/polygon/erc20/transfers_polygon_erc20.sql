{{ config(
    tags=['dunesql'],
    materialized = 'incremental',
    partition_by = ['block_month'],
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['transfer_type', 'evt_tx_hash', 'evt_index', 'wallet_address'], 
    alias = alias('erc20'),
    post_hook='{{ expose_spells(\'["polygon"]\',
                                    "sector",
                                    "transfers",
                                    \'["soispoke", "dot2dotseurat", "tschubotz", "hosuke", "Henrystats"]\') }}'
    )
}}


{{
    transfers_erc20(
        blockchain = 'polygon',
        erc20_evt_transfer = source('erc20_polygon', 'evt_transfer'),
        wrapped_token_deposit = source('mahadao_polygon', 'wmatic_evt_deposit'),
        wrapped_token_withdrawal = source('mahadao_polygon', 'wmatic_evt_withdrawal')
    )
}}

