{{ config(
        alias ='erc721_agg_hour',
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key='unique_transfer_id'
        )
}}

select
    'ethereum' as blockchain,
    date_trunc('hour', evt_block_time) as hour,
    wallet_address,
    token_address,
    tokenId,
    wallet_address || '-' || date_trunc('hour', evt_block_time) || '-' || token_address || '-'  || tokenId as unique_transfer_id,
    SUM(amount) as amount
from {{ ref('transfers_ethereum_erc721') }}
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
where evt_block_time >= date_trunc('day', now() - interval '1 week')
{% endif %}
group by 1,2,3,4,5,6
-- having sum(amount) = 1 -- commenting this out as it seems to affect the rolling models 
