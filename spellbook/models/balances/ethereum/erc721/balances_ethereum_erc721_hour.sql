{{ config(
        alias='erc721_hour'
        )
}}

with one_zero_balances AS
-- sum(amount) ... = 1 for the hour the token was recieved
-- sum(amount) ... = 0 for the hour the token was sent
--
(
       SELECT 'ethereum' as blockchain,
              date_trunc('hour', evt_block_time) as hour,
              wallet_address,
              token_address,
              tokenId,
              current_timestamp() as updated_at,
              sum(amount) over (partition by token_address, tokenId, wallet_address order by date_trunc('hour', evt_block_time) asc) as balance,
              row_number() over (partition by token_address, tokenId, wallet_address order by date_trunc('hour', evt_block_time) desc) as recency_index
       FROM    {{ ref('transfers_ethereum_erc721') }})

, hours AS
(
       SELECT explode( sequence( to_date('2015-01-01'), date_trunc('hour', now()), interval 1 hour ) ) AS hour
)


, token_holders as
     (SELECT   blockchain,
              hour,
              lead(hour, 1, now()) OVER (partition BY blockchain, token_address, tokenId ORDER BY hour) AS next_hour, --hour sold
              wallet_address,
              token_address,
              tokenid
     FROM     one_zero_balances
     WHERE    balance = 1)

SELECT     /*+ RANGE_JOIN(h, 24) */
           th.blockchain,
           h.hour,
           th.wallet_address,
           th.token_address,
           th.tokenid,
           nft_tokens.name as collection
FROM       token_holders AS th
INNER JOIN hours h
ON         th.hour <= h.hour
AND        h.hour < th.next_hour
LEFT JOIN {{ ref('tokens_ethereum_nft') }} nft_tokens ON nft_tokens.contract_address = th.token_address
