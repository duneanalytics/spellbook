{{ config(
        alias ='erc721_agg'
        )
}}

select
    'ethereum' as blockchain,
    evt_block_time,
    evt_index,
    wallet_address,
    token_address,
    tokenId,
    lead(evt_block_time, 1, now()) OVER (PARTITION BY wallet_address, token_address, tokenId ORDER BY evt_block_time ASC, evt_index asc) AS next_evt,
    SUM(amount) over (PARTITION BY wallet_address, token_address, tokenId ORDER BY evt_block_time ASC, evt_index ASC) AS num_tokens,
    unique_tx_id || '-' || wallet_address || '-' || token_address || tokenId as unique_transfer_id
from {{ ref('transfers_ethereum_erc721') }}
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
where date_trunc('day', evt_block_time) > now() - interval 2 days
{% endif %}

-- summing over leaves us with a table that contains all current and prior holders, we need those in tact here to be able to carry forward the state
-- this can't be done in an incremental function