CREATE OR REPLACE VIEW erasure_bay.view_request_data AS
SELECT operator,
       buyer AS requester,
       seller AS fullfiller,
       "paymentAmount" / 10^t.decimals AS dai_reward,
       "paymentAmount" / 10^t.decimals * p.price AS usd_reward,
       "stakeAmount" / 10^t.decimals AS dai_stake,
       "stakeAmount" / 10^t.decimals * p.price AS usd_stake,
       t.symbol AS token_symbol,
       t.contract_address AS token_address,
       "tokenID" AS token_id,
       "countdownLength" AS countdown_length,
       "agreementParams" AS agreement_params,
       r.contract_address,
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
            contract_address,
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
LEFT JOIN prices.usd p ON p.minute = date_trunc('minute', r.evt_block_time)
      AND p.symbol = t.symbol
;
