{{ config(
    
    materialized = 'incremental',
    partition_by = ['block_month'],
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['transfer_type', 'evt_tx_hash', 'evt_index', 'wallet_address'], 
    alias = 'erc20',
    post_hook='{{ expose_spells(\'["gnosis"]\',
                                    "sector",
                                    "transfers",
                                    \'["hdser"]\') }}'
    )
}}


WITH

erc20_transfers  as (
        SELECT 
            IF("from" = 0x0000000000000000000000000000000000000000, 'mint', 'receive') as transfer_type, 
            evt_tx_hash,
            evt_index, 
            evt_block_time,
            to as wallet_address, 
            "from" AS counterparty,
            contract_address as token_address,
            TRY_CAST(value as INT256) as amount_raw
        FROM 
        {{ source('erc20_gnosis', 'evt_transfer') }}
        

        UNION ALL 

        SELECT 
            IF(to = 0x0000000000000000000000000000000000000000, 'burn', 'send') as transfer_type, 
            evt_tx_hash,
            evt_index, 
            evt_block_time,
            "from" as wallet_address, 
            to AS counterparty,
            contract_address as token_address,
            -TRY_CAST(value as INT256) as amount_raw
        FROM 
        {{ source('erc20_gnosis', 'evt_transfer') }}
        
)


, wrapped_token_events as (
        SELECT 
            'deposit' as transfer_type, 
            evt_tx_hash, 
            evt_index, 
            evt_block_time,
            dst as wallet_address, 
            NULL as counterparty,
            contract_address as token_address, 
            TRY_CAST(wad as INT256) as amount_raw
        FROM 
        {{ source('wxdai_gnosis', 'WXDAI_evt_Deposit') }}
        

        UNION ALL 

        SELECT 
            'withdraw' as transfer_type, 
            evt_tx_hash, 
            evt_index, 
            evt_block_time,
            src as wallet_address, 
            NULL as counterparty,
            contract_address as token_address, 
            -TRY_CAST(wad as INT256) as amount_raw
        FROM 
        {{ source('wxdai_gnosis', 'WXDAI_evt_withdrawal') }}
        
)


SELECT
    'gnosis' as blockchain, 
    transfer_type,
    evt_tx_hash, 
    evt_index,
    evt_block_time,
    CAST(date_trunc('month', evt_block_time) as date) as block_month,
    wallet_address, 
    counterparty,
    token_address, 
    amount_raw
FROM 
erc20_transfers


UNION ALL 

SELECT 
    'gnosis' as blockchain, 
    transfer_type,
    evt_tx_hash, 
    evt_index,
    evt_block_time,
    CAST(date_trunc('month', evt_block_time) as date) as block_month,
    wallet_address, 
    counterparty,
    token_address, 
    amount_raw
FROM 
wrapped_token_events