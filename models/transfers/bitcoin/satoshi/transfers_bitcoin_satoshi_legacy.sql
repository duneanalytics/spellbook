{{ config(materialized='view', alias = alias('satoshi', legacy_model=True),
        tags = ['legacy'],
        file_format = 'delta',
        unique_key = ['type', 'tx_id', 'index', 'wallet_address'],
        post_hook='{{ expose_spells(\'["bitcoin"]\',
                                    "sector",
                                    "transfers",
                                    \'["longnhbkhn"]\') }}') }}
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

select last(type) as type, 
    tx_id, index, 'bitcoin' as blockchain, 
    last(wallet_address) as wallet_address, 
    last(block_time) as block_time, 
    last(block_date) as block_date, 
    last(block_height) as block_height, 
    last(amount_raw) as amount_raw
from input_transfers group by tx_id, index
union
select last(type) as type, 
    tx_id, index, 'bitcoin' as blockchain, 
    last(wallet_address) as wallet_address, 
    last(block_time) as block_time, 
    last(block_date) as block_date, 
    last(block_height) as block_height, 
    last(amount_raw) as amount_raw
from output_transfers group by tx_id, index