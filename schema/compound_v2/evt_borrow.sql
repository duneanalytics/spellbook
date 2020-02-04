SELECT t.symbol AS "symbol",
"borrowAmount"/10^t.decimals AS "borrowAmount",
"borrowAmount"/10^t.decimals*p.price AS "borrowAmountUSD",
"accountBorrows"/10^t.decimals AS "accountBorrows",
"totalBorrows"/10^t.decimals AS "totalBorrows",
events.contract_address AS "cToken",
c."underlying_token_address" AS "underlyingToken", 
evt_tx_hash as tx_hash,
tx.block_time AS block_time
FROM
(SELECT * FROM compound_v2."cErc20_evt_Borrow"
UNION
SELECT * FROM compound_v2."cEther_evt_Borrow"
UNION 
SELECT * FROM compound_v2."CErc20Delegator_evt_Borrow"
) events
LEFT JOIN compound_v2.view_ctokens c ON events.contract_address = c.contract_address
LEFT JOIN ethereum.transactions tx ON events.evt_tx_hash = tx.hash
LEFT JOIN erc20.tokens t ON c.underlying_token_address = t.contract_address
LEFT JOIN prices.usd p ON p.minute = date_trunc('minute', tx.block_time) AND p.contract_address = c.underlying_token_address
;
