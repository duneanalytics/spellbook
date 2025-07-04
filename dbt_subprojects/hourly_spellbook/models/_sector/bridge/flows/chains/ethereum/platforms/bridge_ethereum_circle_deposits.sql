{% set blockchain = 'ethereum' %}

{{ config(
    schema = 'bridge_' + blockchain,
    alias = 'base_bridge_initiated',
    materialized = 'view',
    )
}}

WITH cctp_id_mapping AS (
    SELECT id, blockchain
    FROM (VALUES
    (0, 'ethereum')
    , (1, 'avalanche_c')
    , (2, 'optimism')
    , (3, 'arbitrum')
    , (4, 'noble')
    , (5, 'solana')
    , (6, 'base')
    , (7, 'polygon')
    , (8, 'sui')
    , (9, 'aptos')
    , (10, 'unichain')
    , (11, 'linea')
    , (12, 'codex')
    , (13, 'sonic')
    , (14, 'worldchain')
    ) AS x (id, blockchain)
    )

SELECT 'ethereum' AS deposit_chain
, i.blockchain AS withdraw_chain
, 'Circle' AS project
, '1' AS project_version
, true AS intent_based
, evt_block_date AS block_date
, evt_block_time AS block_time
, evt_block_number AS block_number
, amount AS deposit_amount_raw
, amount AS withdraw_amount_raw
, depositor AS sender
, varbinary_substring(mintRecipient,13) AS recipient
, 'erc20' AS deposit_token_standard
, 'erc20' AS withdraw_token_standard
, burnToken AS deposit_token_address
, NULL AS withdraw_token_address
, evt_tx_from AS tx_from
, evt_tx_hash AS tx_hash
, evt_index
, contract_address
, nonce AS transfer_id
FROM {{ source('circle_ethereum', 'tokenmessenger_evt_depositforburn') }} d
INNER JOIN cctp_id_mapping i ON d.destinationDomain=i.id