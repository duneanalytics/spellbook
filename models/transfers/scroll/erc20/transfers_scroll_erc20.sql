{{ config(
tags=['prod_exclude'],materialized='view', alias = 'erc20',
        post_hook='{{ expose_spells(\'["scroll"]\',
                                    "sector",
                                    "transfers",
                                    \'["soispoke", "dot2dotseurat", "tschubotz", "0xRob"]\') }}') }}
/*
    note: this spell has not been migrated to dunesql, therefore is only a view on spark
        please migrate to dunesql to ensure up-to-date logic & data
*/
with
    sent_transfers as (
        select
            'send-' || cast(evt_tx_hash as varchar(100)) || '-' || cast (evt_index as varchar(100)) || '-' || CAST(to AS VARCHAR(100)) as unique_transfer_id,
            to as wallet_address,
            contract_address as token_address,
            evt_block_time,
            value as amount_raw
        from
            {{ source('erc20_scroll', 'evt_transfer') }}
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
            {{ source('erc20_scroll', 'evt_transfer') }}
    )

select unique_transfer_id, 'scroll' as blockchain, wallet_address, token_address, evt_block_time, CAST(amount_raw AS VARCHAR(100)) as amount_raw
from sent_transfers
union
select unique_transfer_id, 'scroll' as blockchain, wallet_address, token_address, evt_block_time, CAST(amount_raw AS VARCHAR(100)) as amount_raw
from received_transfers
