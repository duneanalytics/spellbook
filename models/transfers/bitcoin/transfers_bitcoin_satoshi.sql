{{ config(
        tags = ['dunesql'],
        materialized='view',
        schema = 'transfers_bitcoin',
        alias = alias('satoshi'),
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
    )
    , transfer_btc as (
        select any_value(type) as type, 
            tx_id, index, 'bitcoin' as blockchain, 
            any_value(wallet_address) as wallet_address, 
            any_value(block_time) as block_time, 
            any_value(block_date) as block_date, 
            any_value(block_height) as block_height, 
            any_value(amount_raw) as amount_raw
        from input_transfers group by tx_id, index
        union all
        select any_value(type) as type, 
            tx_id, index, 'bitcoin' as blockchain, 
            any_value(wallet_address) as wallet_address, 
            any_value(block_time) as block_time, 
            any_value(block_date) as block_date, 
            any_value(block_height) as block_height, 
            any_value(amount_raw) as amount_raw
        from output_transfers group by tx_id, index
    )

SELECT t.type, t.tx_id, t.index, t.blockchain,
    t.wallet_address, t.block_time, t.block_date,
    t.block_height, t.amount_raw,
    -1 * t.amount_raw * p.price as amount_transfer_usd
FROM transfer_btc t
LEFT JOIN {{ source('prices', 'usd') }} p
    ON date_trunc('minute', t.block_time) = p.minute
    AND p.symbol='BTC'
    AND p.blockchain is null