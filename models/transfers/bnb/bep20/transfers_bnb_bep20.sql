{{ config(
    tags=['dunesql'],
    materialized = 'incremental',
    partition_by = ['block_month'],
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['transfer_type', 'evt_tx_hash', 'evt_index', 'wallet_address'], 
    alias = alias('erc20'),
    post_hook='{{ expose_spells(\'["bnb"]\',
                                    "sector",
                                    "transfers",
                                    \'["soispoke", "dot2dotseurat", "tschubotz", "Henrystats"]\') }}'
    )
}}
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
        {{ source('erc20_bnb', 'evt_transfer') }}
        {% if is_incremental() %}
            WHERE evt_block_time >= date_trunc('day', now() - interval '3' Day)
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
        {{ source('erc20_bnb', 'evt_transfer') }}
        {% if is_incremental() %}
            WHERE evt_block_time >= date_trunc('day', now() - interval '3' Day)
        {% endif %}
),

wbnb_events as (
        SELECT 
            'deposit' as transfer_type, 
            evt_tx_hash, 
            evt_index, 
            evt_block_time,
            dst as wallet_address, 
            contract_address as token_address, 
            CAST(wad as double)as amount_raw
        FROM 
        {{ source('bnb_bnb', 'WBNB_evt_Deposit') }}
        {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc('day', now() - interval '3' Day)
        {% endif %}

        UNION ALL 

        SELECT 
            'withdraw' as transfer_type, 
            evt_tx_hash, 
            evt_index, 
            evt_block_time,
            src as wallet_address, 
            contract_address as token_address, 
            -CAST(wad as double)as amount_raw
        FROM 
        {{ source('bnb_bnb', 'WBNB_evt_Withdrawal') }}
        {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc('day', now() - interval '3' Day)
        {% endif %}
)

, prices as (
    SELECT * 
    
    FROM {{ source('prices', 'usd') }}
    WHERE blockchain = 'bnb'
    {% if is_incremental() %}
        AND minute >= date_trunc('day', now() - interval '3' day)
    {% endif %}
)

, all_transfer as (
    SELECT
        'bnb' as blockchain, 
        transfer_type,
        evt_tx_hash, 
        evt_index,
        evt_block_time,
        CAST(date_trunc('month', evt_block_time) as date) as block_month,
        wallet_address, 
        token_address, 
        amount_raw
    FROM 
    erc20_transfers

    UNION ALL 

    SELECT 
        'bnb' as blockchain, 
        transfer_type,
        evt_tx_hash, 
        evt_index,
        evt_block_time,
        CAST(date_trunc('month', evt_block_time) as date) as block_month,
        wallet_address, 
        token_address, 
        amount_raw
    FROM 
    wbnb_events
)

SELECT t.blockchain,
    t.transfer_type,
    t.evt_tx_hash,
    t.evt_index,
    t.evt_block_time
    t.block_month,
    t.wallet_address,
    t.token_address,
    t.amount_raw,
    -1 * t.amount_raw / power(10, t.decimals) * p.price as amount_transfer_usd
FROM all_transfer t
LEFT JOIN prices p
    ON date_trunc('minute', t.block_time) = p.minute
    AND t.token_address = p.contract_address
