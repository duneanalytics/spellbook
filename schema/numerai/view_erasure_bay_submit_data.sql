CREATE OR REPLACE VIEW numerai.view_erasure_bay_submit_data AS
SELECT "data" ->> 'nonce' AS nonce,
       "data" ->> 'esp_version' AS esp_version,
       "data" ->> 'filename' AS filename,
       "data" ->> 'encryptedSymKey' AS encrypted_sym_key,
       "data" ->> 'receiver' AS requester,
       "data" ->> 'message' AS message,
       "data" ->> 'proofhash' AS proof_hash,
       contract_address,
       tx_hash,
       block_time
FROM
  (SELECT json(convert_from("data", 'UTF8')) AS DATA,
          contract_address,
          "evt_tx_hash" AS tx_hash,
          "evt_block_time" AS block_time
   FROM erasure_v130."CountdownGriefingEscrow_evt_DataSubmitted") s
;
