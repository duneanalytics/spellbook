{{ config(
	tags=['legacy'],
	alias = alias('erc20', legacy_model=True),
    post_hook='{{ expose_spells(\'["optimism"]\',
                                "sector",
                                "transfers",
                                \'["soispoke", "dot2dotseurat", "tschubotz"]\') }}'
    ) 
}}

with
    sent_transfers as (
        select
            'send-' || cast(evt_tx_hash as varchar(100)) || '-' || cast (evt_index as varchar(100)) || '-' || CAST(to AS VARCHAR(100)) as unique_transfer_id,
            to as wallet_address,
            contract_address as token_address,
            evt_block_time,
            value as amount_raw
        from
            {{ source('erc20_optimism', 'evt_transfer') }}
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
            {{ source('erc20_optimism', 'evt_transfer') }}
    )

    ,
    deposited_weth as (
        select
            'deposit-' || cast(evt_tx_hash as varchar(100)) || '-' || cast (evt_index as varchar(100)) || '-' ||  CAST(dst AS VARCHAR(100)) as unique_transfer_id,
            dst as wallet_address,
            contract_address as token_address,
            evt_block_time,
            wad as amount_raw
        from
            {{ source('weth_optimism', 'weth9_evt_deposit') }}
    )

    ,
    withdrawn_weth as (
        select
            'withdraw-' || cast(evt_tx_hash as varchar(100)) || '-' || cast (evt_index as varchar(100)) || '-' ||  CAST(src AS VARCHAR(100)) as unique_transfer_id,
            src as wallet_address,
            contract_address as token_address,
            evt_block_time,
            '-' || CAST(wad AS VARCHAR(100)) as amount_raw
        from
            {{ source('weth_optimism', 'weth9_evt_withdrawal') }}
    )

select unique_transfer_id, 'optimism' as blockchain, wallet_address, token_address, evt_block_time, CAST(amount_raw AS VARCHAR(100)) as amount_raw
from sent_transfers
union all
select unique_transfer_id, 'optimism' as blockchain, wallet_address, token_address, evt_block_time, CAST(amount_raw AS VARCHAR(100)) as amount_raw
from received_transfers
union all
select unique_transfer_id, 'optimism' as blockchain, wallet_address, token_address, evt_block_time, CAST(amount_raw AS VARCHAR(100)) as amount_raw
from deposited_weth
union all
select unique_transfer_id, 'optimism' as blockchain, wallet_address, token_address, evt_block_time, CAST(amount_raw AS VARCHAR(100)) as amount_raw
from withdrawn_weth
