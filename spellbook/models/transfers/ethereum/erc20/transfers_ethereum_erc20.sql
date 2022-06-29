{{ config(materialized='view', alias='erc20') }}

with
    sent_transfers as (
        select
            'send' || '-' || evt_tx_hash || '-' || evt_index || '-' || `to` as unique_transfer_id,
            `to` as wallet_address,
            contract_address as token_address,
            evt_block_time,
            value as amount_raw
        from
            {{ source('erc20_ethereum', 'evt_transfer') }}
    )

    ,
    received_transfers as (
        select
        'receive' || '-' || evt_tx_hash || '-' || evt_index || '-' || `from` as unique_transfer_id,
        `from` as wallet_address,
        contract_address as token_address,
        evt_block_time,
        - value as amount_raw
        from
            {{ source('erc20_ethereum', 'evt_transfer') }}
    )

    ,
    deposited_weth as (
        select
            'deposit' || '-' || evt_tx_hash || '-' || evt_index || '-' || dst as unique_transfer_id,
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
            'withdrawn' || '-' || evt_tx_hash || '-' || evt_index || '-' || src as unique_transfer_id,
            src as wallet_address,
            contract_address as token_address,
            evt_block_time,
            - wad as amount_raw
        from
            {{ source('zeroex_ethereum', 'weth9_evt_withdrawal') }}
    )
    
select unique_transfer_id, 'ethereum' as blockchain, wallet_address, token_address, evt_block_time, amount_raw
from sent_transfers
union
select unique_transfer_id, 'ethereum' as blockchain, wallet_address, token_address, evt_block_time, amount_raw
from received_transfers
union
select unique_transfer_id, 'ethereum' as blockchain, wallet_address, token_address, evt_block_time, amount_raw
from deposited_weth
union
select unique_transfer_id, 'ethereum' as blockchain, wallet_address, token_address, evt_block_time, amount_raw
from withdrawn_weth