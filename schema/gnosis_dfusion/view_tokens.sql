CREATE OR REPLACE VIEW gnosis_dfusion.view_tokens AS
SELECT
    ROW_NUMBER() OVER (ORDER BY transactions.block_number, transactions.index) as token_id,
    tokens.token,
    erc20.symbol,
    erc20.decimals,
    transactions.block_time as add_date
FROM gnosis_dfusion."BatchExchange_call_addToken" tokens
JOIN ethereum."transactions" transactions
  ON transactions.hash=tokens.call_tx_hash
  AND transactions.success=true
LEFT OUTER JOIN erc20."tokens" as erc20
  ON erc20.contract_address = tokens.token;
