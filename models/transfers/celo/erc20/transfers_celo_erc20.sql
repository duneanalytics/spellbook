{{ 
    config(
        
        alias = 'erc20',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'transfer_type', 'evt_index', 'wallet_address'],
        post_hook='{{ expose_spells(\'["celo"]\',
                                    "sector",
                                    "transfers",
                                    \'["soispoke", "dot2dotseurat", "tschubotz", "tomfutago"]\') }}'
    )
}}

with
    sent_transfers as (
        select
            'sent' as transfer_type,
            to as wallet_address,
            contract_address as token_address,
            evt_block_time as block_time,
            cast(date_trunc('month', evt_block_time) as date) as block_month,
            cast(value as double) as amount_raw,
            evt_index,
            evt_tx_hash as tx_hash
        from
            {{ source('erc20_celo', 'evt_transfer') }}
        where 1=1
            {% if is_incremental() %} -- this filter will only be applied on an incremental run
            and evt_block_time >= date_trunc('day', now() - interval '7' day)
            {% endif %}
    ),
    
    received_transfers as (
        select
            'received' as transfer_type,
            "from" as wallet_address,
            contract_address as token_address,
            evt_block_time as block_time,
            cast(date_trunc('month', evt_block_time) as date) as block_month,
            (-1) * cast(value as double) as amount_raw,
            evt_index,
            evt_tx_hash as tx_hash
        from
            {{ source('erc20_celo', 'evt_transfer') }}
        where 1=1
            {% if is_incremental() %} -- this filter will only be applied on an incremental run
            and evt_block_time >= date_trunc('day', now() - interval '7' day)
            {% endif %}

    )
    
    /*,
    -- Wrapped Celo looks to work differently than WETH - commenting this section out for now
    deposited_wcelo as (
        select
            bytearray_substring(topic1,13,20) as wallet_address,
            contract_address as token_address,
            block_time,
            date_trunc('month', block_time) as block_month,
            cast(bytearray_to_uint256(data) as double) as amount_raw,
            index,
            tx_hash
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
            bytearray_substring(topic1,13,20) as wallet_address,
            contract_address as token_address,
            block_time,
            date_trunc('month', block_time) as block_month,
            (-1) * cast(bytearray_to_uint256(data) as double) as amount_raw,
            index,
            tx_hash
        from
            {{ source('celo', 'logs') }}
        where contract_address = 0x3Ad443d769A07f287806874F8E5405cE3Ac902b9 --Wrapped Celo
            and topic0 = 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef --withdrawal
            and bytearray_substring(topic1,13,20) = 0x0000000000000000000000000000000000000000
            {% if is_incremental() %} -- this filter will only be applied on an incremental run
            and block_time >= date_trunc('day', now() - interval '7' day)
            {% endif %}
    )
    */
    
select 'celo' as blockchain, transfer_type, wallet_address, token_address, block_time, block_month, amount_raw, evt_index, tx_hash
from sent_transfers
union
select 'celo' as blockchain, transfer_type, wallet_address, token_address, block_time, block_month, amount_raw, evt_index, tx_hash
from received_transfers

/*
union
select 'celo' as blockchain, wallet_address, token_address, block_time, block_month, amount_raw, index, tx_hash
from deposited_wcelo
union
select 'celo' as blockchain, wallet_address, token_address, block_time, block_month, amount_raw, index, tx_hash
from withdrawn_wcelo
*/
