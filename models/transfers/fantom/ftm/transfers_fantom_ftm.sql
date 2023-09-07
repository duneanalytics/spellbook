{{ config(
    tags=['dunesql'],
    materialized = 'incremental',
    partition_by = ['block_month'],
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['transfer_type', 'tx_hash', 'trace_address', 'wallet_address', 'block_time'], 
    alias = alias('ftm'),
    post_hook='{{ expose_spells(\'["fantom"]\',
                                    "sector",
                                    "transfers",
                                    \'["Henrystats"]\') }}') }}

WITH 

ftm_transfers  as (
        SELECT 
            'receive' as transfer_type, 
            tx_hash,
            trace_address, 
            block_time,
            to as wallet_address, 
            0x21be370D5312f44cB42ce377BC9b8a0cEF1A4C83 as token_address,
            CAST(value as double) as amount_raw
        FROM 
        {{ source('fantom', 'traces') }}
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
            0x21be370D5312f44cB42ce377BC9b8a0cEF1A4C83 as token_address,
            -CAST(value as double) as amount_raw
        FROM 
        {{ source('fantom', 'traces') }}
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
        0x21be370D5312f44cB42ce377BC9b8a0cEF1A4C83 as token_address, 
        -(CASE 
            WHEN CAST(gas_price  as double) = 0 THEN 0
            ELSE (CAST(gas_used as DOUBLE) * CAST(gas_price as DOUBLE))
        END) as amount_raw
    FROM 
    {{ source('fantom', 'transactions') }}
    {% if is_incremental() %}
        WHERE block_time >= date_trunc('day', now() - interval '3' Day)
    {% endif %}
)

SELECT
    'fantom' as blockchain, 
    transfer_type,
    tx_hash, 
    trace_address,
    block_time,
    CAST(date_trunc('month', block_time) as date) as block_month,
    wallet_address, 
    token_address, 
    amount_raw
FROM 
ftm_transfers

UNION ALL 

SELECT 
    'fantom' as blockchain, 
    transfer_type,
    tx_hash, 
    trace_address,
    block_time,
    CAST(date_trunc('month', block_time) as date) as block_month,
    wallet_address, 
    token_address, 
    amount_raw
FROM 
gas_fee
