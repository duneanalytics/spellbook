{{ config(alias='erc721_transfer_day', materialized ='table') }}



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
       FROM {{ref('transfers_ethereum_erc721')}})


SELECT   blockchain,
          day,
          lead(day, 1, now()) OVER (partition BY blockchain, token_address, tokenId ORDER BY day) AS next_day, --day sold
          wallet_address,
          token_address,
          tokenid
 FROM     one_zero_balances
 WHERE    balance = 1