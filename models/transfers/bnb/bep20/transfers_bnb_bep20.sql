{{ config(materialized='view', alias='bep20',
        post_hook='{{ expose_spells(\'["bnb"]\',
                                    "sector",
                                    "transfers",
                                    \'["soispoke", "dot2dotseurat", "tschubotz"]\') }}') }}

with
    sent_transfers as (
        select
            CAST('send' AS VARCHAR(4)) || CAST('-' AS VARCHAR(1)) || CAST(evt_tx_hash AS VARCHAR(100)) || CAST('-' AS VARCHAR(1)) || CAST(evt_index AS VARCHAR(100)) || CAST('-' AS VARCHAR(1)) || CAST(`to` AS VARCHAR(100)) as unique_transfer_id,
            `to` as wallet_address,
            contract_address as token_address,
            evt_block_time,
            value as amount_raw
        from
            {{ source('erc20_bnb', 'evt_transfer') }}
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
            {{ source('erc20_bnb', 'evt_transfer') }}
    )

    ,
    deposited_bnb as (
        select
            CAST('deposit' AS VARCHAR(7)) || CAST('-' AS VARCHAR(1)) || CAST(evt_tx_hash AS VARCHAR(100)) || CAST('-' AS VARCHAR(1)) || CAST(evt_index AS VARCHAR(100)) || CAST('-' AS VARCHAR(1)) || CAST(dst AS VARCHAR(100)) as unique_transfer_id,
            dst as wallet_address,
            contract_address as token_address,
            evt_block_time,
            wad as amount_raw
        from
            {{ source('bnb_bnb', 'WBNB_evt_Deposit') }}
    )

    ,
    withdrawn_bnb as (
        select
            CAST('withdrawn' AS VARCHAR(9)) || CAST('-' AS VARCHAR(1)) || CAST(evt_tx_hash AS VARCHAR(100)) || CAST('-' AS VARCHAR(1)) || CAST(evt_index AS VARCHAR(100)) || CAST('-' AS VARCHAR(1)) || CAST(src AS VARCHAR(100)) as unique_transfer_id,
            src as wallet_address,
            contract_address as token_address,
            evt_block_time,
            '-' || CAST(wad AS VARCHAR(100)) as amount_raw
        from
            {{ source('bnb_bnb', 'WBNB_evt_Withdrawal') }}
    )
    
select unique_transfer_id, 'bnb' as blockchain, wallet_address, token_address, evt_block_time, CAST(amount_raw AS VARCHAR(100)) as amount_raw
from sent_transfers
union
select unique_transfer_id, 'bnb' as blockchain, wallet_address, token_address, evt_block_time, CAST(amount_raw AS VARCHAR(100)) as amount_raw
from received_transfers
union
select unique_transfer_id, 'bnb' as blockchain, wallet_address, token_address, evt_block_time, CAST(amount_raw AS VARCHAR(100)) as amount_raw
from deposited_bnb
union
select unique_transfer_id, 'bnb' as blockchain, wallet_address, token_address, evt_block_time, CAST(amount_raw AS VARCHAR(100)) as amount_raw
from withdrawn_bnb
