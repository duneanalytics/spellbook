{{ config(
    
    materialized = 'incremental',
    partition_by = ['block_month'],
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['transfer_type', 'evt_tx_hash', 'evt_index', 'wallet_address'], 
    alias = 'eth',
    post_hook='{{ expose_spells(\'["zksync"]\',
                                    "sector",
                                    "transfers",
                                    \'["lgingerich"]\') }}') }}


WITH 

eth_transfers  as (
        SELECT 
            'receive' as transfer_type, 
            evt_tx_hash,
            evt_index, 
            evt_block_time,
            to as wallet_address, 
            '0x000000000000000000000000000000000000800a' as token_address,
            CAST(value as double) as amount_raw,
            CAST(value as double) / 1e18 as amount_decimal
        FROM 
        {{ source('erc20_zksync', 'evt_transfer') }}
            -- A transfer event is emitted for ETH. Filter for that here:
            WHERE contract_address = 0x000000000000000000000000000000000000800a
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
            '0x000000000000000000000000000000000000800a' as token_address,
            -CAST(value as double) as amount_raw,
            -CAST(value as double) / 1e18 as amount_decimal
        FROM 
        {{ source('erc20_zksync', 'evt_transfer') }}
            -- A transfer event is emitted for ETH. Filter for that here:
            WHERE contract_address = 0x000000000000000000000000000000000000800a
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
    amount_raw,
    amount_decimal
FROM 
eth_transfers