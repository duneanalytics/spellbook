{{ config(
        alias='erc721_day',
        )
}}


with days AS
(
       SELECT explode( sequence( to_date('2015-01-01'), date_trunc('day', now()), interval 1 day ) ) AS day
)



SELECT     /*+ RANGE_JOIN(d, 24) */
           th.blockchain,
           d.day,
           th.wallet_address,
           th.token_address,
           th.tokenid,
           nft_tokens.name as collection
FROM {{ ref('transfer_ethereum_erc721_day') }} AS th
INNER JOIN days d
ON         th.day <= d.day
AND        d.day < th.next_day
LEFT JOIN {{ ref('tokens_ethereum_nft') }} nft_tokens ON nft_tokens.contract_address = th.token_address
