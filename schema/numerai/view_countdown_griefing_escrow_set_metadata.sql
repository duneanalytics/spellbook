CREATE OR REPLACE VIEW numerai.view_countdown_griefing_escrow_set_metadata AS
SELECT metadata ->> 'application' AS application,
       metadata ->> 'esp_version' AS esp_version,
       metadata ->> 'app_version' AS app_version,
       metadata ->> 'ipld_cid' AS ipld_cid,
       app_storage ->> 'attackPeriod' AS attack_period,
       app_storage ->> 'ratio' AS punishment_ratio,
       app_storage ->> 'description' AS description,
       app_storage ->> 'Deliverable' AS deliverable,
       app_storage ->> 'postType' AS post_type,
       contract_address,
       "evt_tx_hash" AS tx_hash,
       "evt_block_time" AS block_time
FROM
    (SELECT "evt_tx_hash",
            "evt_block_time",
            contract_address,
            json(json(convert_from("metadata", 'UTF8')) ->> 'app_storage') AS app_storage,
            json(convert_from("metadata", 'UTF8')) AS metadata
    FROM erasure_v130."CountdownGriefingEscrow_evt_MetadataSet") r
;
