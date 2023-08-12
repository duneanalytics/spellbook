{{ config(
    tags = ['dunesql'],
    materialized='view', alias = alias('erc20'),
        post_hook='{{ expose_spells(\'["celo"]\',
                                    "sector",
                                    "transfers",
                                    \'["soispoke", "dot2dotseurat", "tschubotz", "tomfutago"]\') }}') }}

with
    sent_transfers as (
        select
            'send-' || cast(evt_tx_hash as varchar(100)) || '-' || cast (evt_index as varchar(100)) || '-' || CAST(to AS VARCHAR(100)) as unique_transfer_id,
            to as wallet_address,
            contract_address as token_address,
            evt_block_time,
            value as amount_raw
        from
            {{ source('erc20_celo', 'evt_transfer') }}
    )

    ,
    received_transfers as (
        select
        'receive-' || cast(evt_tx_hash as varchar(100)) || '-' || cast (evt_index as varchar(100)) || '-' || CAST("from" AS VARCHAR(100)) as unique_transfer_id,
        "from" as wallet_address,
        contract_address as token_address,
        evt_block_time,
        '-' || CAST(value AS VARCHAR(100)) as amount_raw
        from
            {{ source('erc20_celo', 'evt_transfer') }}
    )

    ,
    deposited_wcelo as (
        select
            'deposit-' || cast(evt_tx_hash as varchar(100)) || '-' || cast (evt_index as varchar(100)) || '-' ||  CAST(dst AS VARCHAR(100)) as unique_transfer_id,
            dst as wallet_address,
            contract_address as token_address,
            evt_block_time,
            wad as amount_raw
        from
            {{ source('celo_celo', 'wcelo_evt_deposit') }}
    )

    ,
    withdrawn_wcelo as (
        select
            'withdraw-' || cast(evt_tx_hash as varchar(100)) || '-' || cast (evt_index as varchar(100)) || '-' ||  CAST(src AS VARCHAR(100)) as unique_transfer_id,
            src as wallet_address,
            contract_address as token_address,
            evt_block_time,
            '-' || CAST(wad AS VARCHAR(100)) as amount_raw
        from
            {{ source('celo_celo', 'wcelo_evt_withdrawal') }}
    )
    
select unique_transfer_id, 'celo' as blockchain, wallet_address, token_address, evt_block_time, CAST(amount_raw AS VARCHAR(100)) as amount_raw
from sent_transfers
union
select unique_transfer_id, 'celo' as blockchain, wallet_address, token_address, evt_block_time, CAST(amount_raw AS VARCHAR(100)) as amount_raw
from received_transfers
union
select unique_transfer_id, 'celo' as blockchain, wallet_address, token_address, evt_block_time, CAST(amount_raw AS VARCHAR(100)) as amount_raw
from deposited_wcelo
union
select unique_transfer_id, 'celo' as blockchain, wallet_address, token_address, evt_block_time, CAST(amount_raw AS VARCHAR(100)) as amount_raw
from withdrawn_wcelo
