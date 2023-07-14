{{ config(
	tags=['legacy'],
	
    alias = alias('erc20', legacy_model=True),
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['transfer_type', 'evt_tx_hash', 'evt_index', 'wallet_address'],
    post_hook='{{ expose_spells(\'["fantom"]\',
                                    "sector",
                                    "transfers",
                                    \'["soispoke", "dot2dotseurat", "tschubotz", "hosuke"]\') }}'
    )
}}

with
    sent_transfers as (
        select 
            'send'as transfer_type,
            evt_tx_hash,
            evt_index,
            et.to as wallet_address,
            contract_address as token_address,
            evt_block_time,
            value as amount_raw
        from
            {{ source('erc20_fantom', 'evt_transfer') }} et
        {% if is_incremental() %}
            where evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    ),
    received_transfers as (
        select
            'receive'as transfer_type,
            evt_tx_hash,
            evt_index,
            et.from as wallet_address,
            contract_address as token_address,
            evt_block_time,
            '-' || CAST(value AS VARCHAR(100)) as amount_raw
        from
            {{ source('erc20_fantom', 'evt_transfer') }} et
        {% if is_incremental() %}
            where evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    )

-- There is no need to add wrapped FTM deposits / withdrawals since wrapped FTM on fantom triggers transfer events for both.
    
select 
    transfer_type,
    'fantom' as blockchain, 
    evt_tx_hash,
    evt_index,
    wallet_address,
    token_address,
    evt_block_time,
    CAST(amount_raw AS VARCHAR(100)) as amount_raw
from sent_transfers

union

select 
    transfer_type,
    'fantom' as blockchain, 
    evt_tx_hash,
    evt_index,
    wallet_address,
    token_address,
    evt_block_time, 
    CAST(amount_raw AS VARCHAR(100)) as amount_raw
from received_transfers
