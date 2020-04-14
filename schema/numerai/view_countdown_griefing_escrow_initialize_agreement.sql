CREATE OR REPLACE VIEW numerai.view_countdown_griefing_escrow_initialize_agreement AS
SELECT operator,
       buyer AS requester,
       seller AS fullfiller,
       "paymentAmount" / 10^t.decimals AS reward,
       "stakeAmount" / 10^t.decimals AS stake,
       t.symbol,
       "tokenID" AS token_id,
       "countdownLength" AS countdown_length,
       "agreementParams" AS agreement_params,
       contract_address,
       "evt_tx_hash" AS tx_hash,
       "evt_block_time" AS block_time,
       metadata ->> 'application' AS application,
       metadata ->> 'esp_version' AS esp_version,
       metadata ->> 'app_version' AS app_version,
       metadata ->> 'ipld_cid' AS ipld_cid,
       app_storage ->> 'attackPeriod' AS attack_period,
       app_storage ->> 'ratio' AS punishment_ratio,
       app_storage ->> 'description' AS description,
       app_storage ->> 'Deliverable' AS deliverable,
       app_storage ->> 'postType' AS post_type
FROM
    (SELECT operator,
            buyer,
            seller,
            "paymentAmount",
            "stakeAmount",
            "countdownLength",
            "agreementParams",
            "evt_tx_hash",
            "evt_block_time",
            "tokenID",
            json(json(convert_from("metadata", 'UTF8')) ->> 'app_storage') AS app_storage,
            json(convert_from("metadata", 'UTF8')) AS metadata
    FROM erasure_v130."CountdownGriefingEscrow_evt_Initialized") r
LEFT JOIN
  (SELECT *
   FROM (
         VALUES (2::int,
                 'DAI'::text)) AS tmp (token_id, symbol)) s ON s.token_id = r."tokenID"
LEFT JOIN erc20.tokens t ON s.symbol = t.symbol
;
