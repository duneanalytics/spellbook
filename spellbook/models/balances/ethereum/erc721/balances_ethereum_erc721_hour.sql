{{ config(
        alias='erc721_hour',
        )
}}

with  hours AS
(
       SELECT explode( sequence( to_date('2015-01-01'), date_trunc('hour', now()), interval 1 hour ) ) AS hour
)

SELECT     /*+ RANGE_JOIN(h, 24) */
           th.blockchain,
           h.hour,
           th.wallet_address,
           th.token_address,
           th.tokenid,
           nft_tokens.name as collection
FROM      {{ ref('transfer_ethereum_erc721_hour') }} AS th
INNER JOIN hours h
ON         th.hour <= h.hour
AND        h.hour < th.next_hour
LEFT JOIN {{ ref('tokens_ethereum_nft') }} nft_tokens ON nft_tokens.contract_address = th.token_address
