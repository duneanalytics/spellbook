{{ config(
    
    materialized = 'incremental',
    partition_by = ['block_month'],
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['transfer_type', 'tx_hash', 'trace_address', 'wallet_address', 'block_time'], 
    alias = 'xdai_v2',
    post_hook='{{ expose_spells(\'["gnosis"]\',
                                    "sector",
                                    "transfers",
                                    \'["hdser"]\') }}') }}

WITH 


type_create AS (
    SELECT 
        tx_hash
        ,address
    FROM 
       {{ source('gnosis', 'traces') }}
    WHERE
        type = 'create'
        AND
        success
        {% if is_incremental() %}
        AND block_time >= date_trunc('day', now() - interval '3' Day)
        {% endif %}
),

type_suicide AS (
    SELECT 
        tx_hash
        ,trace_address
        ,refund_address
    FROM 
        {{ source('gnosis', 'traces') }}
    WHERE
        type = 'suicide'
        AND
        success
        {% if is_incremental() %}
        AND block_time >= date_trunc('day', now() - interval '3' Day)
        {% endif %}
),
 
same_tx_create_suicide_events AS (
    SELECT 
        t1.tx_hash
        ,t2.trace_address
        ,t1.address
        ,t2.refund_address
    FROM 
        type_create t1
    INNER JOIN  
        type_suicide t2
        ON
        t2.tx_hash = t1.tx_hash
),

xdai_transfers  as (
        SELECT 
            'receive' as transfer_type, 
            tx_hash,
            trace_address, 
            block_time,
            COALESCE(to,address) as wallet_address, 
            0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee as token_address,
            TRY_CAST(value as INT256) as amount_raw
        FROM 
        {{ source('gnosis', 'traces') }}
        WHERE (call_type NOT IN ('delegatecall', 'callcode', 'staticcall') OR call_type IS NULL )
        AND success
        AND TRY_CAST(value as INT256) > 0
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

xdai_transfers_create_suicide_events  as (
    SELECT 
        t1.tx_hash,
        t2.address, 
        t2.refund_address,
        SUM(amount_raw) AS amount_raw
    FROM
        xdai_transfers t1
    INNER JOIN
        same_tx_create_suicide_events t2
        ON
        t2.tx_hash = t1.tx_hash
        AND
        t2.address = t1.wallet_address
    GROUP BY 
        1,2,3
),

xdai_transfers_from_same_tx_create_suicide_events  as (
    SELECT 
        'send' AS transfer_type, 
        t1.tx_hash,
        NULL AS trace_address, 
        t1.block_time,
        t2.address AS wallet_address, 
        t1.token_address,
        -t2.amount_raw AS amount_raw
    FROM
        xdai_transfers t1
    RIGHT JOIN
        xdai_transfers_create_suicide_events t2
        ON
        t2.tx_hash = t1.tx_hash
    GROUP BY
        1,2,3,4,5,6,7


    UNION ALL

    SELECT 
        'receive' AS transfer_type, 
        t1.tx_hash,
        NULL AS trace_address, 
        t1.block_time,
        t2.refund_address AS wallet_address, 
        t1.token_address,
        t2.amount_raw AS amount_raw
    FROM
        xdai_transfers t1
    RIGHT JOIN
        xdai_transfers_create_suicide_events t2
        ON
        t2.tx_hash = t1.tx_hash
    GROUP BY
        1,2,3,4,5,6,7
),

gas_fee as (
    SELECT 
        'gas_fee' as transfer_type,
        hash as tx_hash, 
        array[index] as trace_address, 
        block_time, 
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
        t2.miner as wallet_address, 
        0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee as token_address, 
        TRY_CAST(t1.gas_used as INT256) * (TRY_CAST(t1.gas_price as INT256) - TRY_CAST(COALESCE(t2.base_fee_per_gas,0) as INT256)) as amount_raw
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
        receiver AS wallet_address,
        0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee as token_address, 
        TRY_CAST(amount as INT256) as amount_raw
    FROM 
        {{ source('xdai_gnosis', 'BlockRewardAuRa_evt_AddedReceiver') }}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc('day', now() - interval '3' Day)
    {% endif %}
),

bridged AS (
    SELECT 
        'bridged' as transfer_type,
        evt_tx_hash AS tx_hash, 
        array[evt_index] as trace_address, 
        evt_block_time AS block_time, 
        recipient AS wallet_address,
        0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee as token_address, 
        TRY_CAST(value as INT256) as amount_raw
    FROM 
        {{ source('xdai_bridge_gnosis', 'HomeBridgeErcToNative_evt_UserRequestForSignature') }}
    {% if is_incremental() %}
        WHERE block_time >= date_trunc('day', now() - interval '3' Day)
    {% endif %}

)

SELECT
    'gnosis' as blockchain, 
    transfer_type,
    tx_hash, 
    trace_address,
    block_time,
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
    CAST(date_trunc('month', block_time) as date) as block_month,
    wallet_address, 
    token_address, 
    amount_raw
FROM 
xdai_transfers_from_same_tx_create_suicide_events

UNION ALL 

SELECT 
    'gnosis' as blockchain, 
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

UNION ALL 

SELECT 
    'gnosis' as blockchain, 
    transfer_type,
    tx_hash, 
    trace_address,
    block_time,
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
    CAST(date_trunc('month', block_time) as date) as block_month,
    wallet_address, 
    token_address, 
    amount_raw
FROM 
block_reward
