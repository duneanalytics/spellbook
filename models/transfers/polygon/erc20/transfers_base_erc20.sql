{{ config(
    tags=['dunesql'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['transfer_type', 'evt_tx_hash', 'evt_index', 'wallet_address'], 
    alias = alias('erc20'),
    post_hook='{{ expose_spells(\'["base"]\',
                                    "sector",
                                    "transfers",
                                    \'["Henrystats"]\') }}') }}

WITH 

erc20_transfers  as (
        SELECT 
            'receive' as transfer_type, 
            evt_tx_hash,
            evt_index, 
            evt_block_time,
            to as wallet_address, 
            contract_address as token_address,
            CAST(value as double) as amount_raw
        FROM 
        {{ source('erc20_base', 'evt_transfer') }}
        {% if is_incremental() %}
            WHERE evt_block_time >= date_trunc('day', now() - interval '7' Day)
        {% endif %}

        UNION ALL 

        SELECT 
            'send' as transfer_type, 
            evt_tx_hash,
            evt_index, 
            evt_block_time,
            "from" as wallet_address, 
            contract_address as token_address,
            -CAST(value as double) as amount_raw
        FROM 
        {{ source('erc20_base', 'evt_transfer') }}
        {% if is_incremental() %}
            WHERE evt_block_time >= date_trunc('day', now() - interval '7' Day)
        {% endif %}
),


weth_events as (
        SELECT 
            'weth_deposit' as transfer_type, 
            tx_hash as evt_tx_hash, 
            CAST(index as BIGINT) as evt_index, 
            block_time as evt_block_time,
            bytearray_substring(topic1,13,20) as wallet_address, 
            0x4200000000000000000000000000000000000006 as token_address, 
            CAST(bytearray_to_uint256(data) as DOUBLE) as amount_raw
        FROM 
        {{ source('base', 'logs') }}
        WHERE contract_address = 0x4200000000000000000000000000000000000006
        AND topic0 = 0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c --deposit
        {% if is_incremental() %} -- this filter will only be applied on an incremental run
        AND block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}

        UNION ALL 

        SELECT 
            'weth_withdraw' as transfer_type, 
            tx_hash as evt_tx_hash, 
            CAST(index as BIGINT) as evt_index, 
            block_time as evt_block_time,
            bytearray_substring(topic1,13,20) as wallet_address, 
            0x4200000000000000000000000000000000000006 as token_address, 
            -CAST(bytearray_to_uint256(data) as DOUBLE) as amount_raw
        FROM 
        {{ source('base', 'logs') }}
        WHERE contract_address = 0x4200000000000000000000000000000000000006
        AND topic0 = 0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65 --deposit
        {% if is_incremental() %} -- this filter will only be applied on an incremental run
        AND block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
)

SELECT
    'base' as blockchain, 
    transfer_type,
    evt_tx_hash, 
    evt_index,
    evt_block_time,
    wallet_address, 
    token_address, 
    amount_raw
FROM 
erc20_transfers

UNION ALL 

SELECT 
    'base' as blockchain, 
    transfer_type,
    evt_tx_hash, 
    evt_index,
    evt_block_time,
    wallet_address, 
    token_address, 
    amount_raw
FROM 
weth_events
