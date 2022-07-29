{{ config(materialized='table', alias='erc721_transfer_day') }}

with
    received_transfers as (
        select 'receive' || '-' ||  evt_tx_hash || '-' || evt_index || '-' || `to` as unique_tx_id,
            to as wallet_address,
            contract_address as token_address,
            evt_block_time,
            tokenId,
            1 as amount
        from
            {{ source('erc721_ethereum', 'evt_transfer') }}
    )

    ,
    sent_transfers as (
        select 'send' || '-' || evt_tx_hash || '-' || evt_index || '-' || `from` as unique_tx_id,
            from as wallet_address,
            contract_address as token_address,
            evt_block_time,
            tokenId,
            -1 as amount
        from
            {{ source('erc721_ethereum', 'evt_transfer') }}
    )

, transfers as
(select 'ethereum' as blockchain, wallet_address, token_address, evt_block_time, tokenId, amount, unique_tx_id
from sent_transfers
union
select 'ethereum' as blockchain, wallet_address, token_address, evt_block_time, tokenId, amount, unique_tx_id
from received_transfers)


, one_zero_balances AS
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
       FROM transfers)


SELECT   blockchain,
          hour,
          lead(hour, 1, now()) OVER (partition BY blockchain, token_address, tokenId ORDER BY hour) AS next_hour, --hour sold
          wallet_address,
          token_address,
          tokenid
 FROM     one_zero_balances
 WHERE    balance = 1