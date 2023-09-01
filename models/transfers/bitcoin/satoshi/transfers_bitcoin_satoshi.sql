{{ config(
        materialized='incremental',
        alias = alias('satoshi'),
        tags = ['dunesql'],
        unique_key = ['type', 'tx_id', 'index', 'block_height', 'wallet_address'],
        post_hook='{{ expose_spells(\'["bitcoin"]\',
                                    "sector",
                                    "transfers",
                                    \'["longnhbkhn", "hosuke"]\') }}') }}
with 
    input_transfers as (
        select
            'input' as type,
            tx_id,
            index,
            address as wallet_address,
            block_time,
            block_date,
            block_height,
            -1 * value as amount_raw
        from
            {{ source('bitcoin', 'inputs') }} 
        where address is not null
        {% if is_incremental() %}
        and block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
    )
    , 
    output_transfers as (
        select
            'output' as type,
            tx_id,
            index,
            address as wallet_address,
            block_time,
            block_date,
            block_height,
            value as amount_raw
        from
            {{ source('bitcoin', 'outputs') }} 
        where address is not null
        {% if is_incremental() %}
        and block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
    )

select type, tx_id, index, 'bitcoin' as blockchain, wallet_address, block_time, block_date, block_height, amount_raw
from input_transfers
union
select type, tx_id, index, 'bitcoin' as blockchain, wallet_address, block_time, block_date, block_height, amount_raw
from output_transfers