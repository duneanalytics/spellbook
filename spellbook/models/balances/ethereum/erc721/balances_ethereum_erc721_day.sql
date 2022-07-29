{{ config(
        alias='erc721_day'
        )
}}

with one_zero_balances AS
-- sum(amount) ... = 1 for the day the token was recieved
-- sum(amount) ... = 0 for the day the token was sent
--
(
       SELECT 'ethereum' as blockchain,
              date_trunc('day', evt_block_time) as day,
              wallet_address,
              token_address,
              tokenId,
              current_timestamp() as updated_at,
              sum(amount) over (partition by token_address, tokenId, wallet_address order by date_trunc('day', evt_block_time) asc) as balance,
              row_number() over (partition by token_address, tokenId, wallet_address order by date_trunc('day', evt_block_time) desc) as recency_index
       FROM    {{ ref('transfers_ethereum_erc721') }})

, days AS
(
       SELECT explode( sequence( to_date('2015-01-01'), date_trunc('day', now()), interval 1 day ) ) AS day
)


, token_holders as
     (SELECT   blockchain,
              day,
              lead(day, 1, now()) OVER (partition BY blockchain, token_address, tokenId ORDER BY day) AS next_day, --day sold
              wallet_address,
              token_address,
              tokenid
     FROM     one_zero_balances
     WHERE    balance = 1)

SELECT     /*+ RANGE_JOIN(d, 24) */
           th.blockchain,
           d.day,
           th.wallet_address,
           th.token_address,
           th.tokenid,
           nft_tokens.name as collection
FROM       token_holders AS th
INNER JOIN days d
ON         th.day <= d.day
AND        d.day < th.next_day
LEFT JOIN {{ ref('tokens_ethereum_nft') }} nft_tokens ON nft_tokens.contract_address = th.token_address
