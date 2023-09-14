{{ config(
    tags=['dunesql'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['transfer_type', 'tx_hash', 'trace_address', 'wallet_address', 'block_time'], 
    alias = alias('eth_tfers'),
    post_hook='{{ expose_spells(\'["base"]\',
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
        {{ source('base', 'traces') }}
        WHERE (call_type NOT IN ('delegatecall', 'callcode', 'staticcall') OR call_type IS NULL)
        AND success
        AND CAST(value as double) > 0
        AND to IS NOT NULL 
        {% if is_incremental() %}
            AND block_time >= date_trunc('day', now() - interval '7' Day)
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
        {{ source('base', 'traces') }}
        WHERE (call_type NOT IN ('delegatecall', 'callcode', 'staticcall') OR call_type IS NULL)
        AND success
        AND CAST(value as double) > 0
        AND "from" IS NOT NULL 
        {% if is_incremental() %}
            AND block_time >= date_trunc('day', now() - interval '7' Day)
        {% endif %}
),

eth_deposits_events as (
    SELECT 
        'deposits' as transfer_type, 
        evt_tx_hash as tx_hash, 
        evt_block_time as block_time, 
        to as wallet_address, 
        DENSE_RANK() OVER (PARTITION BY evt_tx_hash ORDER BY evt_index ASC) as tx_rank
    FROM 
    {{ source('bridgebase_ethereum', 'OptimismPortal_evt_TransactionDeposited') }}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc('day', now() - interval '7' Day)
    {% endif %}
), 

eth_deposits_traces as (
    SELECT 
        tx_hash, 
        value, 
        trace_address,
        DENSE_RANK() OVER (PARTITION BY tx_hash ORDER BY tx_index ASC) as tx_rank 
    FROM 
    {{ source('ethereum', 'traces') }}
    WHERE to = 0x49048044d57e1c92a77f79988d21fa8faf74e97e
    AND CAST(value as DOUBLE) > 0 
    {% if is_incremental() %}
    AND block_time >= date_trunc('day', now() - interval '7' Day)
    {% endif %}
), 

-- eth deposits do not show in traces 
eth_deposits as (
    SELECT 
        ee.transfer_type, 
        ee.tx_hash, 
        et.trace_address, 
        ee.block_time, 
        ee.wallet_address, 
        0xDeadDeAddeAddEAddeadDEaDDEAdDeaDDeAD0000 as token_address, 
        CAST(et.value as double) as amount_raw 
    FROM 
    eth_deposits_events ee 
    INNER JOIN 
    eth_deposits_traces et 
        ON ee.tx_hash = et.tx_hash
        AND ee.tx_rank = et.tx_rank 
),

gas_fee as (
    SELECT 
        'gas_fee' as transfer_type,
        hash as tx_hash, 
        array[index] as trace_address, 
        block_time, 
        "from" as wallet_address, 
        0xDeadDeAddeAddEAddeadDEaDDEAdDeaDDeAD0000 as token_address, 
        -(CASE 
            WHEN gas_price = cast(0 as UINT256) THEN 0
            ELSE (CAST(gas_used as DOUBLE) * CAST(gas_price as DOUBLE)) + (CAST(l1_fee as DOUBLE))
        END) as amount_raw
    FROM 
    {{ source('base', 'transactions') }}
    {% if is_incremental() %}
        WHERE block_time >= date_trunc('day', now() - interval '7' Day)
    {% endif %}
)

SELECT
    'base' as blockchain, 
    transfer_type,
    tx_hash, 
    trace_address,
    block_time,
    wallet_address, 
    token_address, 
    amount_raw
FROM 
eth_transfers

UNION ALL 

SELECT 
    'base' as blockchain, 
    transfer_type,
    tx_hash, 
    trace_address,
    block_time,
    wallet_address, 
    token_address, 
    amount_raw
FROM 
eth_deposits

UNION ALL 

SELECT 
    'base' as blockchain, 
    transfer_type,
    tx_hash, 
    trace_address,
    block_time,
    wallet_address, 
    token_address, 
    amount_raw
FROM 
gas_fee
