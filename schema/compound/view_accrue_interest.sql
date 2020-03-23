CREATE OR REPLACE VIEW compound.view_accrue_interest AS
SELECT CASE
           WHEN t.symbol = 'WETH' THEN 'ETH'
           ELSE t.symbol
       END AS token_symbol,
       "interestAccumulated"/10^t.decimals AS interest_accumulated,
       "interestAccumulated"/10^t.decimals*p.price AS interest_accumulated_usd,
       "totalBorrows"/10^t.decimals AS total_borrows,
       "totalBorrows"/10^t.decimals*p.price AS total_borrows_usd,
       "borrowIndex" AS borrow_index,
       c."underlying_token_address" AS token_address,
       evt_tx_hash AS tx_hash,
       evt_block_time AS block_time
FROM
  (SELECT "interestAccumulated", "borrowIndex", "totalBorrows", contract_address, evt_tx_hash, evt_block_time
   FROM compound_v2."cErc20_evt_AccrueInterest"
   UNION 
   SELECT "interestAccumulated", "borrowIndex", "totalBorrows", contract_address, evt_tx_hash, evt_block_time
   FROM compound_v2."cEther_evt_AccrueInterest"
   UNION 
   SELECT "interestAccumulated", "borrowIndex", "totalBorrows", contract_address, evt_tx_hash, evt_block_time
   FROM compound_v2."CErc20Delegator_evt_AccrueInterest") events
LEFT JOIN compound.view_ctokens c ON events.contract_address = c.contract_address
LEFT JOIN erc20.tokens t ON c.underlying_token_address = t.contract_address
LEFT JOIN
  (SELECT minute,
          contract_address,
          symbol,
          price
   FROM prices.usd
   WHERE symbol IN ('BAT', 'SAI', 'WETH', 'REP', 'USDC', 'WBTC', 'ZRX')
   UNION SELECT generate_series('2019-11-18', now(), '1 minute'),
                '\x6B175474E89094C44Da98b954EedeAC495271d0F'::bytea AS contract_address,
                'DAI' AS symbol,
                1 AS price) p ON p.minute = date_trunc('minute', evt_block_time)
AND p.contract_address = c.underlying_token_address
;
