{{ config(
        alias='erc721_hour'
        )
}}

with
    hours as (
        select
            explode(
                sequence(
                    to_date('2015-01-01'), date_trunc('hour', now()), interval 1 hour
                )
            ) as hour
    )

SELECT
    'ethereum' as blockchain,
    d.hour,
    b.wallet_address,
    b.token_address,
    b.tokenId,
    b.num_tokens,
    nft_tokens.name as collection,
    nft_tokens.category
FROM hours d
INNER JOIN {{ ref('transfers_ethereum_erc721_rolling') }} b ON (b.evt_block_time <= d.hour AND d.hour < b.next_evt)
LEFT JOIN {{ ref('tokens_ethereum_nft') }} nft_tokens ON (nft_tokens.contract_address = b.token_address)
where num_tokens = 1

--only here can we drop the prior owners with "num_token=1" to have a continuos chain of ownership for a specific NFT