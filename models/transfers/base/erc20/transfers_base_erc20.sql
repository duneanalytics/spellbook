{{ config(materialized='view', 
    schema = 'base_transfers',
    alias = alias('erc20'),
    tags = ['dunesql'],
    materialized ='incremental',
    file_format ='delta',
    incremental_strategy='merge',
    unique_key='unique_transfer_id',
        post_hook='{{ expose_spells(\'["base"]\',
                                    "sector",
                                    "transfers",
                                    \'["soispoke", "dot2dotseurat", "tschubotz"]\') }}') }}

with
    sent_transfers as (
        select
            'send-' || cast(evt_tx_hash as varchar) || '-' || cast (evt_index as varchar) || '-' || CAST(to AS varchar) as unique_transfer_id,
            to as wallet_address,
            contract_address as token_address,
            evt_block_time,
            cast(value as double) as amount_raw
        from
            {{ source('erc20_base', 'evt_transfer') }}
            where 1=1
            {% if is_incremental() %} -- this filter will only be applied on an incremental run
            and evt_block_time >= date_trunc('day', now() - interval '7' day)
            {% endif %}
    )

    ,
    received_transfers as (
        select
        'receive-' || cast(evt_tx_hash as varchar) || '-' || cast (evt_index as varchar) || '-' || CAST("from" AS varchar) as unique_transfer_id,
        "from" as wallet_address,
        contract_address as token_address,
        evt_block_time,
        (-1) * CAST(value AS double) as amount_raw
        from
            {{ source('erc20_base', 'evt_transfer') }}
            where 1=1
            {% if is_incremental() %} -- this filter will only be applied on an incremental run
            and evt_block_time >= date_trunc('day', now() - interval '7' day)
            {% endif %}

    )

    ,
    deposited_weth as (
        select
            'deposit-' || cast(evt_tx_hash as varchar) || '-' || cast (evt_index as varchar) || '-' ||  CAST(dst AS varchar) as unique_transfer_id,
            dst as wallet_address,
            contract_address as token_address,
            evt_block_time,
            cast(wad as double) as amount_raw
        from
            {{ source('weth_base', 'weth9_evt_deposit') }}
            where 1=1
            {% if is_incremental() %} -- this filter will only be applied on an incremental run
            and evt_block_time >= date_trunc('day', now() - interval '7' day)
            {% endif %}
    )

    ,
    withdrawn_weth as (
        select
            'withdraw-' || cast(evt_tx_hash as varchar) || '-' || cast (evt_index as varchar) || '-' ||  CAST(src AS varchar) as unique_transfer_id,
            src as wallet_address,
            contract_address as token_address,
            evt_block_time,
            (-1) * CAST(wad AS double) as amount_raw
        from
            {{ source('weth_base', 'weth9_evt_withdrawal') }}
            where 1=1
            {% if is_incremental() %} -- this filter will only be applied on an incremental run
            and evt_block_time >= date_trunc('day', now() - interval '7' day)
            {% endif %}
    )
    
select unique_transfer_id, 'base' as blockchain, wallet_address, token_address, evt_block_time, CAST(amount_raw AS varchar) as amount_raw
from sent_transfers
union
select unique_transfer_id, 'base' as blockchain, wallet_address, token_address, evt_block_time, CAST(amount_raw AS varchar) as amount_raw
from received_transfers
union
select unique_transfer_id, 'base' as blockchain, wallet_address, token_address, evt_block_time, CAST(amount_raw AS varchar) as amount_raw
from deposited_weth
union
select unique_transfer_id, 'base' as blockchain, wallet_address, token_address, evt_block_time, CAST(amount_raw AS varchar) as amount_raw
from withdrawn_weth
