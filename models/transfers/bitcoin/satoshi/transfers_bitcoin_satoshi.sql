{{ config(materialized='view', alias = alias('satoshi'),
        tags = ['dunesql'],
        post_hook='{{ expose_spells(\'["bitcoin"]\',
                                    "sector",
                                    "transfers",
                                    \'["longnhbkhn"]\') }}') }}
with 
    input_transfers as (
        select
            CAST('input' AS VARCHAR(5)) || CAST('-' AS VARCHAR(1)) || CAST(tx_id AS VARCHAR(100)) || CAST('-' AS VARCHAR(1)) || CAST(index AS VARCHAR(100)) || CAST('-' AS VARCHAR(1)) || CAST(address AS VARCHAR(100)) as unique_transfer_id,
            address as wallet_address,
            block_time,
            block_date,
            block_height,
            -1 * value as amount_raw
        from
            {{ source('bitcoin', 'inputs') }} where address != null
    )
    , 
    output_transfers as (
        select
            CAST('output' AS VARCHAR(6)) || CAST('-' AS VARCHAR(1)) || CAST(tx_id AS VARCHAR(100)) || CAST('-' AS VARCHAR(1)) || CAST(index AS VARCHAR(100)) || CAST('-' AS VARCHAR(1)) || CAST(address AS VARCHAR(100)) as unique_transfer_id,
            address as wallet_address,
            block_time,
            block_date,
            block_height,
            value as amount_raw
        from
            {{ source('bitcoin', 'outputs') }} where address != null
    )

select unique_transfer_id, 'bitcoin' as blockchain, wallet_address, block_time, block_date, block_height, CAST(amount_raw AS VARCHAR(100)) as amount_raw
from input_transfers
union
select unique_transfer_id, 'bitcoin' as blockchain, wallet_address, block_time, block_date, block_height, CAST(amount_raw AS VARCHAR(100)) as amount_raw
from output_transfers