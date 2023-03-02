{{ config(materialized='view', alias='erc721') }}

with
    received_transfers as (
        select 'receive' || '-' ||  evt_tx_hash || '-' || evt_index || '-' || `to` as unique_tx_id,
            to as wallet_address,
            contract_address as token_address,
            evt_block_time,
            evt_index,
            tokenId,
            1 as amount
        from
            {{ source('erc721_ethereum', 'evt_transfer') }}
        where contract_address = lower('0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D')
    )

    ,
    sent_transfers as (
        select 'send' || '-' || evt_tx_hash || '-' || evt_index || '-' || `from` as unique_tx_id,
            from as wallet_address,
            contract_address as token_address,
            evt_block_time,
            evt_index,
            tokenId,
            -1 as amount
        from
            {{ source('erc721_ethereum', 'evt_transfer') }}
        where contract_address = lower('0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D')
    )
    
select 'ethereum' as blockchain, wallet_address, token_address, evt_block_time, tokenId, amount, unique_tx_id
from received_transfers
union
select 'ethereum' as blockchain, wallet_address, token_address, evt_block_time, tokenId, amount, unique_tx_id
from sent_transfers

