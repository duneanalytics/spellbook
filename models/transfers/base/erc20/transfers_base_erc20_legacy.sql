{{ config(
	tags=['legacy'],
	materialized='view', 
    schema = 'base_transfers',
    alias = alias('erc20', legacy_model=True),
        post_hook='{{ expose_spells(\'["base"]\',
                                    "sector",
                                    "transfers",
                                    \'["soispoke", "dot2dotseurat", "tschubotz"]\') }}') }}

with
    sent_transfers as (
        select
            'send-' || cast(evt_tx_hash as varchar(100)) || '-' || cast (evt_index as varchar(100)) || '-' || CAST(to AS VARCHAR(100)) as unique_transfer_id,
            to as wallet_address,
            contract_address as token_address,
            evt_block_time,
            value as amount_raw
        from
            {{ source('erc20_base', 'evt_transfer') }}
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
            {{ source('erc20_base', 'evt_transfer') }}
    )

    ,
    deposited_weth as (
        select
            'deposit-' || cast(tx_hash as varchar(100)) || '-' || cast (index as varchar(100)) || '-' ||  CAST(dst AS VARCHAR(100)) as unique_transfer_id,
            bytearray_substring(topic2,13,20) as wallet_address,
            contract_address as token_address,
            block_time as evt_block_time,
            cast( bytearray_to_uint256(data) as double) as amount_raw
        from
            {{ source('base', 'logs') }}
            WHERE contract_address = '0x4200000000000000000000000000000000000006'
            AND topic1 = '0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c' --deposit
    )

    ,
    withdrawn_weth as (
        select
            'withdraw-' || cast(tx_hash as varchar(100)) || '-' || cast (index as varchar(100)) || '-' ||  CAST(src AS VARCHAR(100)) as unique_transfer_id,
            bytearray_substring(topic2,13,20) as wallet_address,
            contract_address as token_address,
            block_time as evt_block_time,
            (-1)* cast( bytearray_to_uint256(data) as double) as amount_raw
        from
            {{ source('base', 'logs') }}
            WHERE contract_address = '0x4200000000000000000000000000000000000006'
            AND topic1 = '0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65' --withdrawal
    )
    
select unique_transfer_id, 'base' as blockchain, wallet_address, token_address, evt_block_time, CAST(amount_raw AS VARCHAR(100)) as amount_raw
from sent_transfers
union
select unique_transfer_id, 'base' as blockchain, wallet_address, token_address, evt_block_time, CAST(amount_raw AS VARCHAR(100)) as amount_raw
from received_transfers
union
select unique_transfer_id, 'base' as blockchain, wallet_address, token_address, evt_block_time, CAST(amount_raw AS VARCHAR(100)) as amount_raw
from deposited_weth
union
select unique_transfer_id, 'base' as blockchain, wallet_address, token_address, evt_block_time, CAST(amount_raw AS VARCHAR(100)) as amount_raw
from withdrawn_weth
