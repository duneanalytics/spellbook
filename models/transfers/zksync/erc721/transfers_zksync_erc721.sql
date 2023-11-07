{{ config(
    
    materialized = 'incremental',
    partition_by = ['block_month'],
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['transfer_type', 'evt_tx_hash', 'evt_index', 'wallet_address'], 
    alias = 'erc721',
    post_hook='{{ expose_spells(\'["zksync"]\',
                                    "sector",
                                    "transfers",
                                    \'["lgingerich"]\') }}') }}

WITH 

erc721_transfers  as (
        SELECT 
            'receive' as transfer_type, 
            evt_tx_hash,
            evt_index, 
            evt_block_time,
            to as wallet_address, 
            contract_address as token_address,
            tokenId,
            1 as amount
        FROM 
        {{ source('erc721_zksync', 'evt_transfer') }}
        {% if is_incremental() %}
            AND {{ incremental_predicate('evt_block_time') }}
        {% endif %}

    
        UNION ALL 

        SELECT 
            'send' as transfer_type, 
            evt_tx_hash,
            evt_index, 
            evt_block_time,
            "from" as wallet_address, 
            contract_address as token_address,
            tokenId,
            -1 as amount
        FROM 
        {{ source('erc721_zksync', 'evt_transfer') }}
        {% if is_incremental() %}
            AND {{ incremental_predicate('evt_block_time') }}
        {% endif %}
)

SELECT
    'zksync' as blockchain, 
    transfer_type,
    evt_tx_hash, 
    evt_index,
    evt_block_time,
    CAST(date_trunc('month', evt_block_time) as date) as block_month,
    wallet_address, 
    token_address, 
    tokenId,
    amount
FROM 
erc721_transfers