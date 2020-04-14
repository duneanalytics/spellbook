CREATE OR REPLACE VIEW numerai.view_countdown_griefing_initialize_agreement AS
SELECT operator,
       staker AS requester,
       counterparty AS fullfiller,
       t.symbol,
       "tokenID" AS token_id,
       "countdownLength" AS countdown_length,
       contract_address,
       "evt_tx_hash" AS tx_hash,
       "evt_block_time" AS block_time
FROM
    (SELECT operator,
            staker,
            counterparty,
            "countdownLength",
            "evt_tx_hash",
            "evt_block_time",
            "tokenID",
            "ratioType",
            "ratio"
    FROM erasure_v130."CountdownGriefing_evt_Initialized") r
LEFT JOIN
  (SELECT *
   FROM (
         VALUES (2::int,
                 'DAI'::text)) AS tmp (token_id, symbol)) s ON s.token_id = r."tokenID"
LEFT JOIN erc20.tokens t ON s.symbol = t.symbol
;
