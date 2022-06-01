{{ config(materialized='view', alias='erc20') }}

with
    sent_transfers as (
        select
            evt_tx_hash || '-' || evt_index || '-' || to as unique_tx_id,
            to as wallet_address,
            contract_address as token_address,
            evt_block_time,
            value as amount_raw
        from
            {{ source('erc20_ethereum', 'evt_transfer') }}
    )

    ,
    received_transfers as (
        select evt_tx_hash || '-' || evt_index || '-' || to as unique_tx_id,
        from
            as wallet_address,
            contract_address as token_address,
            evt_block_time, 
            - value as amount_raw
        from
            {{ source('erc20_ethereum', 'evt_transfer') }}
    )

    ,
    deposited_weth as (
        select
            evt_tx_hash || '-' || evt_index as unique_tx_id,
            dst as wallet_address,
            contract_address as token_address,
            evt_block_time,
            wad as amount_raw
        from
            {{ source('zeroex_ethereum', 'weth9_evt_deposit') }}
    )

    ,
    withdrawn_weth as (
        select
            evt_tx_hash || '-' || evt_index as unique_tx_id,
            src as wallet_address,
            contract_address as token_address,
            evt_block_time,
            - wad as amount_raw
        from
            {{ source('zeroex_ethereum', 'weth9_evt_withdrawal') }}
    )
    
select 'ethereum' as blockchain, wallet_address, token_address, evt_block_time, amount_raw
from sent_transfers
union all
select 'ethereum' as blockchain, wallet_address, token_address, evt_block_time, amount_raw
from received_transfers
