{{ config(
    tags=['dunesql'],
    materialized='view', 
    alias = alias('erc20'),
    post_hook='{{ expose_spells(\'["optimism"]\',
                                    "sector",
                                    "transfers",
                                    \'["soispoke", "dot2dotseurat", "tschubotz", "Henrystats"]\') }}') }}

WITH 

erc20_transfers  as (
        SELECT 
            'receive' as transfer_type, 
            evt_tx_hash,
            evt_index, 
            evt_block_time,
            to as wallet_address, 
            contract_address as token_address,
            value as amount_raw
        FROM 
        {{ source('erc20_optimism', 'evt_transfer') }}

        UNION ALL 

        SELECT 
            'send' as transfer_type, 
            evt_tx_hash,
            evt_index, 
            evt_block_time,
            from as wallet_address, 
            contract_address as token_address,
            -value as amount_raw
        FROM 
        {{ source('erc20_optimism', 'evt_transfer') }}
),


weth_events as (
        SELECT 
            'weth_deposit' as transfer_type, 
            evt_tx_hash, 
            evt_index, 
            evt_block_time,
            dst as wallet_address, 
            contract_address as token_address, 
            wad as amount_raw
        FROM 
        {{ source('weth_optimism', 'weth9_evt_deposit') }}

        UNION ALL 

        SELECT 
            'weth_withdraw' as transfer_type, 
            evt_tx_hash, 
            evt_index, 
            evt_block_time,
            src as wallet_address, 
            contract_address as token_address, 
            -wad as amount_raw
        FROM 
        {{ source('weth_optimism', 'weth9_evt_withdrawal') }}
)

SELECT 'optimism' as blockchain, * FROM erc20_transfers

UNION ALL 

SELECT 'optimism' as blockchain, * FROM weth_events
