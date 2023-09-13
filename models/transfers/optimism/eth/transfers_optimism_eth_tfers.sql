{{ config(
    tags=['dunesql'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    partition_by = ['block_month'],
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
    (
    {% if not is_incremental() %}
        SELECT 
            tx_hash, 
            trace_address, 
            block_time, 
            to, 
            value, 
            call_type, 
            success
        FROM 
        {{ source('optimism_legacy_ovm1', 'traces') }}

        UNION ALL 

        SELECT 
            tx_hash, 
            trace_address, 
            block_time, 
            to, 
            value, 
            call_type, 
            success
        FROM 
        {{ source('optimism', 'traces') }}
    {% endif %}
    {% if is_incremental() %}
        SELECT 
            * 
        FROM 
        {{ source('optimism', 'traces') }}
        WHERE block_time >= date_trunc('day', now() - interval '3' Day)
    {% endif %}
    ) x 
        WHERE (call_type NOT IN ('delegatecall', 'callcode', 'staticcall') OR call_type IS NULL)
        AND success
        AND CAST(value as double) > 0
        AND to IS NOT NULL 

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
    (
    {% if not is_incremental() %}
        SELECT 
            tx_hash, 
            trace_address, 
            block_time, 
            "from", 
            value, 
            call_type, 
            success
        FROM 
        {{ source('optimism_legacy_ovm1', 'traces') }}

        UNION ALL 

        SELECT 
            tx_hash, 
            trace_address, 
            block_time, 
            "from", 
            value, 
            call_type, 
            success
        FROM 
        {{ source('optimism', 'traces') }}
    {% endif %}
    {% if is_incremental() %}
        SELECT 
            * 
        FROM 
        {{ source('optimism', 'traces') }}
        WHERE block_time >= date_trunc('day', now() - interval '3' Day)
    {% endif %}
    ) x 
        WHERE (call_type NOT IN ('delegatecall', 'callcode', 'staticcall') OR call_type IS NULL)
        AND success
        AND CAST(value as double) > 0
        AND "from" IS NOT NULL 
),

 --ETH Transfers from deposits and withdrawals are ERC20 transfers of the 'deadeadead' ETH token. These do not appear in traces.
erc20_eth_transfers  as (
        SELECT 
            'deposits' as transfer_type, 
            evt_tx_hash as tx_hash,
            array[evt_index] as trace_address, 
            evt_block_time as block_time,
            to as wallet_address, 
            0xDeadDeAddeAddEAddeadDEaDDEAdDeaDDeAD0000 as token_address,
            CAST(value as double) as amount_raw
        FROM 
            (
            {% if not is_incremental() %}
                SELECT 
                    * 
                FROM 
                {{ source('erc20_optimism', 'evt_transfer') }}
                WHERE contract_address = 0xDeadDeAddeAddEAddeadDEaDDEAdDeaDDeAD0000

                UNION ALL 

                SELECT 
                    * 
                FROM 
                {{ source('erc20_optimism_legacy_ovm1', 'evt_transfer') }}
                WHERE contract_address = 0x4200000000000000000000000000000000000006
            {% endif %}
            {% if is_incremental() %}
                SELECT 
                    * 
                FROM 
                {{ source('erc20_optimism', 'evt_transfer') }}
                WHERE evt_block_time >= date_trunc('day', now() - interval '3' Day)
                AND contract_address = 0xDeadDeAddeAddEAddeadDEaDDEAdDeaDDeAD0000
            {% endif %}
            ) x 
        UNION ALL 

        SELECT 
            'withdrawals' as transfer_type, 
            evt_tx_hash as tx_hash,
            array[evt_index] as trace_address, 
            evt_block_time as block_time,
            "from" as wallet_address, 
            0xDeadDeAddeAddEAddeadDEaDDEAdDeaDDeAD0000 as token_address,
            -CAST(value as double) as amount_raw
        FROM 
            (
            {% if not is_incremental() %}
                SELECT 
                    * 
                FROM 
                {{ source('erc20_optimism', 'evt_transfer') }}
                WHERE contract_address = 0xDeadDeAddeAddEAddeadDEaDDEAdDeaDDeAD0000

                UNION ALL 

                SELECT 
                    * 
                FROM 
                {{ source('erc20_optimism_legacy_ovm1', 'evt_transfer') }}
                WHERE contract_address = 0x4200000000000000000000000000000000000006
            {% endif %}
            {% if is_incremental() %}
                SELECT 
                    * 
                FROM 
                {{ source('erc20_optimism', 'evt_transfer') }}
                WHERE evt_block_time >= date_trunc('day', now() - interval '3' Day)
                AND contract_address = 0xDeadDeAddeAddEAddeadDEaDDEAdDeaDDeAD0000
            {% endif %}
            ) x 
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
    (
    {% if not is_incremental() %}
        SELECT 
            hash, 
            index, 
            block_time, 
            "from", 
            CAST(gas_price as UINT256) as gas_price, 
            gas_used, 
            CAST(0 as BIGINT) as l1_fee 
        FROM 
        {{ source('optimism_legacy_ovm1', 'transactions') }}

        UNION ALL 

        SELECT 
            hash, 
            index,
            block_time, 
            "from", 
            gas_price, 
            gas_used, 
            l1_fee 
        FROM 
        {{ source('optimism', 'transactions') }}
    {% endif %}
    {% if is_incremental() %}
        SELECT 
            * 
        FROM 
        {{ source('optimism', 'transactions') }}
        WHERE block_time >= date_trunc('day', now() - interval '3' Day)
    {% endif %}
    ) x 
)

SELECT
    'optimism' as blockchain, 
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
    'optimism' as blockchain, 
    transfer_type,
    tx_hash, 
    trace_address,
    block_time,
    CAST(date_trunc('month', block_time) as date) as block_month, 
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
    CAST(date_trunc('month', block_time) as date) as block_month, 
    wallet_address, 
    token_address, 
    amount_raw
FROM 
gas_fee
