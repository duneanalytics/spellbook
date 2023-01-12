{{ config(materialized='view', alias='ftm',
        post_hook='{{ expose_spells(\'["fantom"]\',
                                    "sector",
                                    "transfers",
                                    \'["Henrystats"]\') }}') }}

with
    received_transfers as (
        select
            'receive' || '-' || tx_hash || '-' || CAST(tx_index AS VARCHAR(100)) || '-' || CAST(trace_address AS VARCHAR(100)) || '-' ||  `to` as unique_transfer_id,
            `to` as wallet_address,
            block_time as evt_block_time, -- consistency in schema 
            CAST(value as decimal(38,0)) as amount_raw
        from
            {{ source('fantom', 'traces') }}
        WHERE (LOWER(call_type) NOT IN ('delegatecall', 'callcode', 'staticcall') or call_type IS NULL)
        AND success = true 
        AND CAST(value as decimal(38,0)) > 0 
    )

    ,
    sent_transfers as (
        select
            'send' || '-' || tx_hash || '-' || CAST(tx_index AS VARCHAR(100)) || '-' || CAST(trace_address AS VARCHAR(100)) || '-' ||  `from` as unique_transfer_id,
            `from` as wallet_address,
            block_time as evt_block_time,
            -1 * CAST(value as decimal(38,0)) as amount_raw 
        from
            {{ source('fantom', 'traces') }}
        WHERE (LOWER(call_type) NOT IN ('delegatecall', 'callcode', 'staticcall') or call_type IS NULL)
        AND success = true 
        AND CAST(value as decimal(38,0)) > 0 
    )
  
    ,
    gas_fees as (
        select 
            'gas' || '-' || hash || '-' || CAST(index AS VARCHAR(100)) || '-' || `from` as unique_transfer_id, 
            `from` as wallet_address, 
            block_time as evt_block_time,
            -1 * ((gas_price * gas_used)) as amount_raw
        from 
            {{ source('fantom', 'transactions') }}
    )

select unique_transfer_id, 'fantom' as blockchain, wallet_address, evt_block_time, amount_raw
from sent_transfers
union
select unique_transfer_id, 'fantom' as blockchain, wallet_address, evt_block_time, amount_raw
from received_transfers
union
select unique_transfer_id, 'fantom' as blockchain, wallet_address, evt_block_time, amount_raw
from gas_fees