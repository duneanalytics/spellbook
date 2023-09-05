{{ config(
    tags=['dunesql'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['transfer_type', 'tx_hash', 'trace_address', 'wallet_address', 'block_time'], 
    alias = alias('eth_tfers'),
    post_hook='{{ expose_spells(\'["optimism"]\',
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
        {{ source('optimism', 'traces') }}
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
        {{ source('optimism', 'traces') }}
        WHERE (call_type NOT IN ('delegatecall', 'callcode', 'staticcall') OR call_type IS NULL)
        AND success
        AND CAST(value as double) > 0
        AND "from" IS NOT NULL 
        {% if is_incremental() %}
            AND block_time >= date_trunc('day', now() - interval '7' Day)
        {% endif %}
),

 --ETH Transfers from deposits and withdrawals are ERC20 transfers of the 'deadeadead' ETH token. These do not appear in traces.
erc20_eth_transfers  as (
        SELECT 
            'deposits' as transfer_type, 
            evt_tx_hash as tx_hash,
            array[evt_index] as trace_address, 
            evt_block_time as block_time,
            to as wallet_address, 
            contract_address as token_address,
            CAST(value as double) as amount_raw
        FROM 
        {{ source('erc20_optimism', 'evt_transfer') }}
        WHERE contract_address = 0xDeadDeAddeAddEAddeadDEaDDEAdDeaDDeAD0000
        {% if is_incremental() %}
            AND evt_block_time >= date_trunc('day', now() - interval '7' Day)
        {% endif %}

        UNION ALL 

        SELECT 
            'withdrawals' as transfer_type, 
            evt_tx_hash as tx_hash,
            array[evt_index] as trace_address, 
            evt_block_time as block_time,
            "from" as wallet_address, 
            contract_address as token_address,
            -CAST(value as double) as amount_raw
        FROM 
        {{ source('erc20_optimism', 'evt_transfer') }}
        WHERE contract_address = 0xDeadDeAddeAddEAddeadDEaDDEAdDeaDDeAD0000
        {% if is_incremental() %}
            AND evt_block_time >= date_trunc('day', now() - interval '7' Day)
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
        CASE 
            WHEN gas_price = cast(0 as UINT256) THEN 0
            ELSE CAST(gas_used as DOUBLE) * CAST(gas_price as DOUBLE)/1e18 + CAST(l1_fee as DOUBLE) /1e18
        END as amount_raw
    FROM 
    {{ source('optimism', 'transactions') }}
    {% if is_incremental() %}
        WHERE block_time >= date_trunc('day', now() - interval '7' Day)
    {% endif %}
)

SELECT
    'optimism' as blockchain, 
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
    'optimism' as blockchain, 
    transfer_type,
    tx_hash, 
    trace_address,
    block_time,
    wallet_address, 
    token_address, 
    amount_raw
FROM 
erc20_eth_transfers

UNION ALL 

SELECT 
    'optimism' as blockchain, 
    transfer_type,
    tx_hash, 
    trace_address,
    block_time,
    wallet_address, 
    token_address, 
    -amount_raw
FROM 
gas_fee
