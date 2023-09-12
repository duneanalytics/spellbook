{{ config(
    tags=['dunesql'],
    materialized = 'incremental',
    partition_by = ['block_month'],
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['transfer_type', 'tx_hash', 'trace_address', 'wallet_address', 'block_time'], 
    alias = alias('eth'),
    post_hook='{{ expose_spells(\'["arbitrum"]\',
                                    "sector",
                                    "transfers",
                                    \'["Henrystats"]\') }}') }}

WITH 

eth_transfers  as (
        SELECT 
            'receive' as transfer_type, 
            tx_hash,
            trace_address, 
            block_time,
            to as wallet_address, 
            0xDeadDeAddeAddEAddeadDEaDDEAdDeaDDeAD0000 as token_address,
            CAST(value as double) as amount_raw
        FROM 
        {{ source('arbitrum', 'traces') }}
        WHERE (call_type NOT IN ('delegatecall', 'callcode', 'staticcall') OR call_type IS NULL)
        AND success
        AND CAST(value as double) > 0
        AND to IS NOT NULL 
        {% if is_incremental() %}
            AND block_time >= date_trunc('day', now() - interval '3' Day)
        {% endif %}

        UNION ALL 

        SELECT 
            'send' as transfer_type, 
            tx_hash,
            trace_address, 
            block_time,
            "from" as wallet_address, 
            0xDeadDeAddeAddEAddeadDEaDDEAdDeaDDeAD0000 as token_address,
            -CAST(value as double) as amount_raw
        FROM 
        {{ source('arbitrum', 'traces') }}
        WHERE (call_type NOT IN ('delegatecall', 'callcode', 'staticcall') OR call_type IS NULL)
        AND success
        AND CAST(value as double) > 0
        AND "from" IS NOT NULL 
        {% if is_incremental() %}
            AND block_time >= date_trunc('day', now() - interval '3' Day)
        {% endif %}
),

gas_fee as (
    SELECT 
        'gas_fee' as transfer_type,
        hash as tx_hash, 
        array[index] as trace_address, 
        block_time, 
        "from" as wallet_address, 
        0xDeadDeAddeAddEAddeadDEaDDEAdDeaDDeAD0000 as token_address, 
        CAST(gas_price as double) * CAST(gas_used as double) as amount_raw
    FROM 
    {{ source('arbitrum', 'transactions') }}
    {% if not is_incremental() %}
    WHERE CONCAT(CAST(hash as VARCHAR), CAST(block_number as VARCHAR)) != '0xf135954c7b2a17c094f917fff69aa215fa9af86443e55f167e701e39afa5ff0f15458950' -- this is weirdly duplicated on arbitrum.transactions table with a different block_number
    {% endif %}
    {% if is_incremental() %}
        WHERE block_time >= date_trunc('day', now() - interval '3' Day)
    {% endif %}
)

SELECT
    'arbitrum' as blockchain, 
    transfer_type,
    tx_hash, 
    trace_address,
    block_time,
    CAST(date_trunc('month', block_time) as date) as block_month,
    wallet_address, 
    token_address, 
    amount_raw
FROM 
eth_transfers

UNION ALL 

SELECT 
    'arbitrum' as blockchain, 
    transfer_type,
    tx_hash, 
    trace_address,
    block_time,
    CAST(date_trunc('month', block_time) as date) as block_month,
    wallet_address, 
    token_address, 
    -amount_raw
FROM 
gas_fee
