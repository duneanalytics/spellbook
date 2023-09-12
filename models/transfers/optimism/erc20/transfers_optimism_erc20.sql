{{ config(
    tags=['dunesql'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    partition_by = ['block_month'],
    unique_key = ['transfer_type', 'evt_tx_hash', 'evt_index', 'wallet_address'], 
    alias = alias('erc20'),
    post_hook='{{ expose_spells(\'["optimism"]\',
                                    "sector",
                                    "transfers",
                                    \'["soispoke", "dot2dotseurat", "tschubotz", "Henrystats"]\') }}') }}

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
            (
            {% if not is_incremental() %}
                SELECT 
                    * 
                FROM 
                {{ source('erc20_optimism', 'evt_transfer') }}

                UNION ALL 

                SELECT 
                    * 
                FROM 
                {{ source('erc20_optimism_legacy_ovm1', 'evt_transfer') }}
            {% endif %}
            {% if is_incremental() %}
                SELECT 
                    * 
                FROM 
                {{ source('erc20_optimism', 'evt_transfer') }}
                WHERE evt_block_time >= date_trunc('day', now() - interval '3' Day)0
            {% endif %}
            ) x 

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
            (
            {% if not is_incremental() %}
                SELECT 
                    * 
                FROM 
                {{ source('erc20_optimism', 'evt_transfer') }}

                UNION ALL 

                SELECT 
                    * 
                FROM 
                {{ source('erc20_optimism_legacy_ovm1', 'evt_transfer') }}
            {% endif %}
            {% if is_incremental() %}
                SELECT 
                    * 
                FROM 
                {{ source('erc20_optimism', 'evt_transfer') }}
                WHERE evt_block_time >= date_trunc('day', now() - interval '3' Day)0
            {% endif %}
            ) x 
),


weth_events as (
        SELECT 
            'weth_deposit' as transfer_type, 
            evt_tx_hash, 
            evt_index, 
            evt_block_time,
            dst as wallet_address, 
            contract_address as token_address, 
            CAST(wad as double) as amount_raw
        FROM 
        {{ source('weth_optimism', 'weth9_evt_deposit') }}
        {% if is_incremental() %}
            WHERE evt_block_time >= date_trunc('day', now() - interval '3' Day)
        {% endif %}

        UNION ALL 

        SELECT 
            'weth_withdraw' as transfer_type, 
            evt_tx_hash, 
            evt_index, 
            evt_block_time,
            src as wallet_address, 
            contract_address as token_address, 
            -CAST(wad as double) as amount_raw
        FROM 
        {{ source('weth_optimism', 'weth9_evt_withdrawal') }}
        {% if is_incremental() %}
            WHERE evt_block_time >= date_trunc('day', now() - interval '3' Day)
        {% endif %}
)

SELECT
    'optimism' as blockchain, 
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
    'optimism' as blockchain, 
    transfer_type,
    evt_tx_hash, 
    evt_index,
    evt_block_time,
    CAST(date_trunc('month', evt_block_time) as date) as block_month, 
    wallet_address, 
    token_address, 
    amount_raw
FROM 
weth_events
