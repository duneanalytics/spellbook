{{ config(materialized='view', alias='erc20',
        post_hook='{{ expose_spells(\'["fantom"]\',
                                    "sector",
                                    "transfers",
                                    \'["Henrystats"]\') }}') }}

with
    sent_transfers as (
        select
            'send' || '-' || evt_tx_hash || '-' || CAST(evt_index AS VARCHAR(100)) || '-' || `to` as unique_transfer_id,
            `to` as wallet_address,
            contract_address as token_address,
            evt_block_time,
            value as amount_raw
        from
            {{ source('erc20_fantom', 'evt_transfer') }}
    )

    ,
    received_transfers as (
        select
        'receive' || '-' || evt_tx_hash || '-' || CAST(evt_index AS VARCHAR(100)) || '-' || `from` as unique_transfer_id,
        `from` as wallet_address,
        contract_address as token_address,
        evt_block_time,
        '-' || value as amount_raw
        from
            {{ source('erc20_fantom', 'evt_transfer') }}
    )

select unique_transfer_id, 'fantom' as blockchain, wallet_address, token_address, evt_block_time, amount_raw
from sent_transfers
union
select unique_transfer_id, 'fantom' as blockchain, wallet_address, token_address, evt_block_time, amount_raw
from received_transfers