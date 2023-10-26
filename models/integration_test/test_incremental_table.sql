{{ config(
        tags=[ 'prod_exclude'],
        alias = 'test_incremental_table',
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key='unique_transfer_id',
        schema='integration_test'
        )
}}

select
    'ethereum' as blockchain,
    date_trunc('day', evt_block_time) as day,
    wallet_address,
    token_address,
    tokenId,
    sum(amount) as amount,
    unique_tx_id || '-' || cast(wallet_address as varchar) || '-' || cast(token_address as varchar) || cast(tokenId as varchar) || '-' || cast(sum(amount) as varchar) as unique_transfer_id
FROM {{ ref('test_view') }}
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
where evt_block_time > date_trunc('day', now() - interval '2' day) 
{% endif %}
group by
    date_trunc('day', evt_block_time), wallet_address, token_address, tokenId, unique_tx_id