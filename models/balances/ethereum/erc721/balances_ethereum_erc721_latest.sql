with
    minutes as (
        select
            explode(
                sequence(
                    to_date('2015-01-01'), date_trunc('minute', now()), interval 1 minute
                )
            ) as minute
    )

SELECT
    'ethereum' as blockchain,
    d.minute,
    b.wallet_address,
    b.token_address,
    b.tokenId,
    b.num_tokens,
    nft_tokens.name as collection
FROM minutes d
INNER JOIN {{ ref('transfers_ethereum_erc721_agg') }} b ON (b.evt_block_time <= d.minute AND d.minute < b.next_evt)
LEFT JOIN {{ ref('tokens_ethereum_nft') }} nft_tokens ON (nft_tokens.contract_address = b.token_address)
where num_tokens = 1
and d.minute = date_trunc('minute', now()) --as recent as the database pipeline allows as we are using the db time as base

--only here can we drop the prior owners with "num_token=1" to have a continuos chain of ownership for a specific NFT