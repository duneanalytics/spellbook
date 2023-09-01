{{ config(
        materialized='incremental',
        alias = alias('satoshi'),
        tags = ['dunesql'],
        file_format = 'delta',
        unique_key = ['unique_transfer_id'],
        post_hook='{{ expose_spells(\'["bitcoin"]\',
                                    "sector",
                                    "transfers",
                                    \'["longnhbkhn", "hosuke"]\') }}') }}
with 
    input_transfers as (
        select
            CAST('input' AS VARCHAR(5)) || CAST('-' AS VARCHAR(1)) || CAST(tx_id AS VARCHAR(100)) || CAST('-' AS VARCHAR(1)) || CAST(index AS VARCHAR(100)) || CAST('-' AS VARCHAR(1)) || CAST(address AS VARCHAR(100)) as unique_transfer_id,
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
            CAST('output' AS VARCHAR(6)) || CAST('-' AS VARCHAR(1)) || CAST(tx_id AS VARCHAR(100)) || CAST('-' AS VARCHAR(1)) || CAST(index AS VARCHAR(100)) || CAST('-' AS VARCHAR(1)) || CAST(address AS VARCHAR(100)) as unique_transfer_id,
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

select unique_transfer_id, type, tx_id, index, 'bitcoin' as blockchain, wallet_address, block_time, block_date, block_height, amount_raw
from input_transfers
union
select unique_transfer_id, type, tx_id, index, 'bitcoin' as blockchain, wallet_address, block_time, block_date, block_height, amount_raw
from output_transfers