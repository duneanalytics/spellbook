{{ config(
    alias=alias('erc20'),
    tags=['dunesql'],
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    unique_key='unique_transfer_id',
    post_hook='{{ expose_spells(\'["celo"]\',
                                "sector",
                                "transfers",
                                \'["soispoke", "dot2dotseurat", "tschubotz", "tomfutago"]\') }}') }}

with
    sent_transfers as (
        select
            'send-' || cast(evt_tx_hash as varchar) || '-' || cast(evt_index as varchar) || '-' || cast("to" as varchar) as unique_transfer_id,
            to as wallet_address,
            contract_address as token_address,
            evt_block_time,
            cast(value as double) as amount_raw
        from
            {{ source('erc20_celo', 'evt_transfer') }}
        where 1=1
            {% if is_incremental() %} -- this filter will only be applied on an incremental run
            and evt_block_time >= date_trunc('day', now() - interval '7' day)
            {% endif %}
    ),
    
    received_transfers as (
        select
            'receive-' || cast(evt_tx_hash as varchar) || '-' || cast (evt_index as varchar) || '-' || cast("from" as varchar) as unique_transfer_id,
            "from" as wallet_address,
            contract_address as token_address,
            evt_block_time,
            (-1) * cast(value as double) as amount_raw
        from
            {{ source('erc20_celo', 'evt_transfer') }}
        where 1=1
            {% if is_incremental() %} -- this filter will only be applied on an incremental run
            and evt_block_time >= date_trunc('day', now() - interval '7' day)
            {% endif %}

    ),

    deposited_wcelo as (
        select
            'deposit-' || cast(tx_hash as varchar) || '-' || cast (index as varchar) || '-' ||  cast(bytearray_substring(topic1,13,20) as varchar) as unique_transfer_id,
            bytearray_substring(topic1,13,20) as wallet_address,
            contract_address as token_address,
            block_time as evt_block_time,
            cast(bytearray_to_uint256(data) as double) as amount_raw
        from
            {{ source('celo', 'logs') }}
        where contract_address = 0x3Ad443d769A07f287806874F8E5405cE3Ac902b9 --Wrapped Celo
            and topic0 = 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef --deposit
            and bytearray_substring(topic1,13,20) <> 0x0000000000000000000000000000000000000000
            {% if is_incremental() %} -- this filter will only be applied on an incremental run
            and block_time >= date_trunc('day', now() - interval '7' day)
            {% endif %}
    ),

    withdrawn_wcelo as (
        select
            'withdraw-' || cast(tx_hash as varchar) || '-' || cast (index as varchar) || '-' ||  cast(bytearray_substring(topic1,13,20) as varchar) as unique_transfer_id,
            bytearray_substring(topic1,13,20) as wallet_address,
            contract_address as token_address,
            block_time as evt_block_time,
            (-1) * cast(bytearray_to_uint256(data) as double) as amount_raw
        from
            {{ source('celo', 'logs') }}
        where contract_address = 0x3Ad443d769A07f287806874F8E5405cE3Ac902b9 --Wrapped Celo
            and topic0 = 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef --withdrawal
            and bytearray_substring(topic1,13,20) = 0x0000000000000000000000000000000000000000
            {% if is_incremental() %} -- this filter will only be applied on an incremental run
            and block_time >= date_trunc('day', now() - interval '7' day)
            {% endif %}
    )
    
select unique_transfer_id, 'celo' as blockchain, wallet_address, token_address, evt_block_time, cast(amount_raw as varchar(100)) as amount_raw
from sent_transfers
union
select unique_transfer_id, 'celo' as blockchain, wallet_address, token_address, evt_block_time, cast(amount_raw as varchar(100)) as amount_raw
from received_transfers
union
select unique_transfer_id, 'celo' as blockchain, wallet_address, token_address, evt_block_time, cast(amount_raw as varchar(100)) as amount_raw
from deposited_wcelo
union
select unique_transfer_id, 'celo' as blockchain, wallet_address, token_address, evt_block_time, cast(amount_raw as varchar(100)) as amount_raw
from withdrawn_wcelo
