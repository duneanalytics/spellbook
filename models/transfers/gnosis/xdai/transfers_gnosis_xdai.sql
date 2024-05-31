{{ config(
    
    materialized = 'incremental',
    partition_by = ['block_month'],
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['transfer_type', 'tx_hash', 'trace_address', 'wallet_address', 'block_time'], 
    alias = 'xdai',
    post_hook='{{ expose_spells(\'["gnosis"]\',
                                    "sector",
                                    "transfers",
                                    \'["hdser"]\') }}') }}

WITH 

xdai_transfers  as (
    SELECT 
        'receive' as transfer_type, 
        tx_hash,
        trace_address, 
        block_time,
        block_number,
        COALESCE(to,address) as wallet_address, 
        0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee as token_address,
        TRY_CAST(value as INT256) as amount_raw
    FROM 
    {{ source('gnosis', 'traces') }}
    WHERE (call_type NOT IN ('delegatecall', 'callcode', 'staticcall') OR call_type IS NULL )
    AND success
    AND TRY_CAST(value as INT256) > 0
    AND tx_hash IS NOT NULL
    {% if is_incremental() %}
        AND block_time >= date_trunc('day', now() - interval '3' Day)
    {% endif %}

    UNION ALL 

    SELECT 
        'send' as transfer_type, 
        tx_hash,
        trace_address, 
        block_time,
        block_number,
        "from" as wallet_address, 
        0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee as token_address,
        -TRY_CAST(value as INT256) as amount_raw
    FROM 
        {{ source('gnosis', 'traces') }} 
    WHERE (call_type NOT IN ('delegatecall', 'callcode', 'staticcall') OR call_type IS NULL)
    AND success
    AND TRY_CAST(value as INT256) > 0
    AND tx_hash IS NOT NULL
    {% if is_incremental() %}
        AND t1.block_time >= date_trunc('day', now() - interval '3' Day)
    {% endif %}
),


gas_fee as (
    SELECT 
        'gas_fee' as transfer_type,
        hash as tx_hash, 
        array[index] as trace_address, 
        block_time, 
        block_number,
        "from" as wallet_address, 
        0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee as token_address, 
        - TRY_CAST(gas_used as INT256) * TRY_CAST(gas_price as INT256) as amount_raw
    FROM 
    {{ source('gnosis', 'transactions') }}
    {% if is_incremental() %}
        WHERE block_time >= date_trunc('day', now() - interval '3' Day)
    {% endif %}
),

gas_fee_rewards as (
    SELECT 
        'gas_fee_reward' as transfer_type,
        t1.hash as tx_hash, 
        array[t1.index] as trace_address, 
        t1.block_time, 
        t1.block_number,
        t2.miner as wallet_address, 
        0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee as token_address, 
        IF(TRY_CAST(t1.gas_price as INT256) = 0,
            CAST(0 AS INT256),
            TRY_CAST(t1.gas_used as INT256) * (
                TRY_CAST(t1.gas_price as INT256) - TRY_CAST(COALESCE(t2.base_fee_per_gas,0) as INT256)
            )
            ) as amount_raw
    FROM 
        {{ source('gnosis', 'transactions') }} t1
    INNER JOIN
        {{ source('gnosis', 'blocks') }} t2
        ON
        t2.number = t1.block_number
    {% if is_incremental() %}
        WHERE t1.block_time >= date_trunc('day', now() - interval '3' Day)
    {% endif %}
),

block_reward AS (
    SELECT 
        'block_reward' as transfer_type,
        evt_tx_hash AS tx_hash, 
        array[evt_index] as trace_address, 
        evt_block_time AS block_time,
        evt_block_number AS block_number, 
        receiver AS wallet_address,
        0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee as token_address, 
        TRY_CAST(amount as INT256) as amount_raw
    FROM 
        {{ source('xdai_gnosis', 'RewardByBlock_evt_AddedReceiver') }}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc('day', now() - interval '3' Day)
    {% endif %}

    UNION ALL

    SELECT 
        'block_reward' as transfer_type,
        evt_tx_hash AS tx_hash, 
        array[evt_index] as trace_address, 
        evt_block_time AS block_time,
        evt_block_number AS block_number, 
        receiver AS wallet_address,
        0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee as token_address, 
        TRY_CAST(amount as INT256) as amount_raw
    FROM 
        {{ source('xdai_gnosis', 'BlockRewardAuRa_evt_AddedReceiver') }}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc('day', now() - interval '3' Day)
    {% endif %}
)

SELECT
    'gnosis' as blockchain, 
    transfer_type,
    tx_hash, 
    trace_address,
    block_time,
    block_number,
    CAST(date_trunc('month', block_time) as date) as block_month,
    wallet_address, 
    token_address, 
    amount_raw
FROM 
xdai_transfers


UNION ALL 

SELECT 
    'gnosis' as blockchain, 
    transfer_type,
    tx_hash, 
    trace_address,
    block_time,
    block_number,
    CAST(date_trunc('month', block_time) as date) as block_month,
    wallet_address, 
    token_address, 
    amount_raw
FROM 
gas_fee

UNION ALL 

SELECT 
    'gnosis' as blockchain, 
    transfer_type,
    tx_hash, 
    trace_address,
    block_time,
    block_number,
    CAST(date_trunc('month', block_time) as date) as block_month,
    wallet_address, 
    token_address, 
    amount_raw
FROM 
gas_fee_rewards

UNION ALL 

SELECT 
    'gnosis' as blockchain, 
    transfer_type,
    tx_hash, 
    trace_address,
    block_time,
    block_number,
    CAST(date_trunc('month', block_time) as date) as block_month,
    wallet_address, 
    token_address, 
    amount_raw
FROM 
block_reward
