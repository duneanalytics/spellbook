CREATE OR REPLACE VIEW compound.view_borrow AS
SELECT CASE
           WHEN t.symbol = 'WETH' THEN 'ETH'
           ELSE t.symbol
       END AS token_symbol,
       "borrowAmount"/10^t.decimals AS borrow_amount,
       "borrowAmount"/10^t.decimals*p.price AS borrow_amount_usd,
       borrower,
       "accountBorrows"/10^t.decimals AS account_borrows,
       "totalBorrows"/10^t.decimals AS total_borrows,
       "totalBorrows"/10^t.decimals*p.price AS total_borrows_usd,
       events.contract_address AS ctoken,
       c."underlying_token_address" AS underlying_token,
       evt_tx_hash AS tx_hash,
       evt_block_time AS block_time
FROM
  (SELECT *
   FROM compound_v2."cErc20_evt_Borrow"
   UNION SELECT *
   FROM compound_v2."cEther_evt_Borrow"
   UNION SELECT *
   FROM compound_v2."CErc20Delegator_evt_Borrow") events
LEFT JOIN compound.view_ctokens c ON events.contract_address = c.contract_address
LEFT JOIN erc20.tokens t ON c.underlying_token_address = t.contract_address
LEFT JOIN prices.usd p ON p.minute = date_trunc('minute', evt_block_time)
           AND p.contract_address = c.underlying_token_address
;
