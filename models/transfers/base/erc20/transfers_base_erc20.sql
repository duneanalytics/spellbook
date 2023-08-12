{{ config(
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
            'deposit-' || cast(tx_hash as varchar) || '-' || cast (index as varchar) || '-' ||  CAST(bytearray_substring(topic1,13,20) AS varchar) as unique_transfer_id,
            bytearray_substring(topic1,13,20) as wallet_address,
            contract_address as token_address,
            block_time as evt_block_time,
            cast( bytearray_to_uint256(data) as double) as amount_raw
        from
            {{ source('base', 'logs') }}
            WHERE contract_address = 0x4200000000000000000000000000000000000006
            AND topic0 = 0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c --deposit
            {% if is_incremental() %} -- this filter will only be applied on an incremental run
            and block_time >= date_trunc('day', now() - interval '7' day)
            {% endif %}
    )

    ,
    withdrawn_weth as (
        select
            'withdraw-' || cast(tx_hash as varchar) || '-' || cast (index as varchar) || '-' ||  CAST(bytearray_substring(topic1,13,20) AS varchar) as unique_transfer_id,
            bytearray_substring(topic1,13,20) as wallet_address,
            contract_address as token_address,
            block_time as evt_block_time,
            (-1)* cast( bytearray_to_uint256(data) as double) as amount_raw
        from
            {{ source('base', 'logs') }}
            WHERE contract_address = 0x4200000000000000000000000000000000000006
            AND topic0 = 0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65 --withdrawal
            {% if is_incremental() %} -- this filter will only be applied on an incremental run
            and block_time >= date_trunc('day', now() - interval '7' day)
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