{{ config(materialized='view', alias='erc721') }}

with
    sent_transfers as (
        select 'send' || '-' ||  evt_tx_hash || '-' || evt_index || '-' || `to` as unique_tx_id,
            to as wallet_address,
            contract_address as token_address,
            evt_block_time,
            tokenId,
            1 as amount
        from
            {{ source('erc721_ethereum', 'evt_transfer') }}
    )

    ,
    received_transfers as (
        select 'receive' || '-' || evt_tx_hash || '-' || evt_index || '-' || `from` as unique_tx_id,
            from as wallet_address,
            contract_address as token_address,
            evt_block_time,
            tokenId,
            -1 as amount
        from
            {{ source('erc721_ethereum', 'evt_transfer') }}
    )
    
select 'ethereum' as blockchain, wallet_address, token_address, evt_block_time, tokenId, amount, unique_tx_id
from sent_transfers
union
select 'ethereum' as blockchain, wallet_address, token_address, evt_block_time, tokenId, amount, unique_tx_id
from received_transfers