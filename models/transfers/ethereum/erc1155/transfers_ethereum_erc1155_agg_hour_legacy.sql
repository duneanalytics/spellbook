{{ config(
	    tags=['legacy'],
        alias = alias('erc1155_agg_hour', legacy_model=True),
        file_format ='delta',
        unique_key='unique_transfer_id'
        )
}}

select
    'ethereum' as blockchain,
    date_trunc('hour', evt_block_time) as hour,
    wallet_address,
    token_address,
    tokenId,
    sum(amount) as amount,
    unique_tx_id || '-' || wallet_address || '-' || token_address || tokenId || '-' || sum(amount)::string as unique_transfer_id
FROM {{ ref('transfers_ethereum_erc1155_legacy') }}
group by
    date_trunc('hour', evt_block_time), wallet_address, token_address, tokenId, unique_tx_id
;