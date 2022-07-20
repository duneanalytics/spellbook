CREATE OR REPLACE VIEW compound.view_transfer_ctoken AS
SELECT c.symbol AS ctoken_symbol,
       "amount"/10^c.decimals AS amount,
       events."to",
       events."from",
       c."underlying_token_address" AS underlying_token,
       evt_tx_hash AS tx_hash,
       tx.block_time AS block_time
FROM
  (SELECT *
   FROM compound_v2."cErc20_evt_Transfer"
   UNION SELECT *
   FROM compound_v2."cEther_evt_Transfer"
   UNION SELECT *
   FROM compound_v2."CErc20Delegator_evt_Transfer") events
LEFT JOIN compound_v2.view_ctokens c ON events."contract_address" = c.contract_address
LEFT JOIN ethereum.transactions tx ON events.evt_tx_hash = tx.hash
;
