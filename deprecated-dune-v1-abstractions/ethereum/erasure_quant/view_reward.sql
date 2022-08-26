CREATE OR REPLACE VIEW erasure_quant.view_reward AS
SELECT "from" AS treasury,
       "to" AS staker,
       value / 10^t.decimals AS nmr_reward,
       value / 10^t.decimals * p.price AS usd_reward,
       s.contract_address AS token_address,
       "evt_tx_hash" AS tx_hash,
       "evt_block_time" AS block_time
FROM numerai."NumeraireBackend_evt_Transfer" s
LEFT JOIN erc20.tokens t ON s.contract_address = t.contract_address
LEFT JOIN prices.usd p ON p.minute = date_trunc('minute', s.evt_block_time)
AND p.contract_address = t.contract_address
WHERE "from" IN ('\x0000000000377D181A0ebd08590c6B399b272000',
                 '\xdc6997b078C709327649443D0765BCAa8e37aA6C',
                 '\x67b18F10C0Ff8C76e28a383B404E8e6FDEfe2050')
  AND "to" IN
    (SELECT DISTINCT staker
     FROM erasure_v100."OneWayGriefing_evt_StakeAdded")
;
