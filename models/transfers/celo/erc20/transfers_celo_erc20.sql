{{ config(materialized='view', alias='erc20',
        post_hook='{{ expose_spells(\'["celo"]\',
                                    "sector",
                                    "transfers",
                                    \'[""danielpartida"]\') }}') }}

with
    sent_transfers as (
        select
            CAST('send' AS VARCHAR(4)) || CAST('-' AS VARCHAR(1)) || CAST(evt_tx_hash AS VARCHAR(100)) || CAST('-' AS VARCHAR(1)) || CAST(evt_index AS VARCHAR(100)) || CAST('-' AS VARCHAR(1)) || CAST(`to` AS VARCHAR(100)) as unique_transfer_id,
            `to` as wallet_address,
            contract_address as token_address,
            evt_block_time,
            value as amount_raw
        from
            {{ source('erc20_celo', 'evt_transfer') }}
    )

    ,
    received_transfers as (
        select
        CAST('receive' AS VARCHAR(7)) || CAST('-' AS VARCHAR(1)) || CAST(evt_tx_hash AS VARCHAR(100)) || CAST('-' AS VARCHAR(1)) || CAST(evt_index AS VARCHAR(100)) || CAST('-' AS VARCHAR(1)) || CAST(`from` AS VARCHAR(100)) as unique_transfer_id,
        `from` as wallet_address,
        contract_address as token_address,
        evt_block_time,
        '-' || CAST(value AS VARCHAR(100)) as amount_raw
        from
            {{ source('erc20_celo', 'evt_transfer') }}
    )

select unique_transfer_id, 'celo' as blockchain, wallet_address, token_address, evt_block_time, CAST(amount_raw AS VARCHAR(100)) as amount_raw
from sent_transfers
union
select unique_transfer_id, 'celo' as blockchain, wallet_address, token_address, evt_block_time, CAST(amount_raw AS VARCHAR(100)) as amount_raw
from received_transfers