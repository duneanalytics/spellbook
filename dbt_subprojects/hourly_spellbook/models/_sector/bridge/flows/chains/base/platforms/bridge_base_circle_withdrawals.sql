{% set blockchain = 'base' %}

{{ config(
    schema = 'bridge_' + blockchain,
    alias = 'circle_withdrawals',
    materialized = 'view',
    )
}}

WITH cctp_id_mapping AS (
    SELECT id, blockchain, token_standard
    FROM (VALUES
    (0, 'ethereum', 'erc20')
    , (1, 'avalanche_c', 'erc20')
    , (2, 'optimism', 'erc20')
    , (3, 'arbitrum', 'erc20')
    , (4, 'noble', 'ics20')
    , (5, 'solana', 'spl')
    , (6, 'base', 'erc20')
    , (7, 'polygon', 'erc20')
    , (8, 'sui', 'move module')
    , (9, 'aptos', 'move module')
    , (10, 'unichain', 'erc20')
    , (11, 'linea', 'erc20')
    , (12, 'codex', 'erc20')
    , (13, 'sonic', 'erc20')
    , (14, 'worldchain', 'erc20')
    ) AS x (id, blockchain, token_standard)
    )

SELECT i.blockchain AS deposit_chain
, 'base' AS withdraw_chain
--, d.destinationDomain
, 'Circle' AS project
, '1' AS project_version
, true AS intent_based
, w.evt_block_date AS block_date
, w.evt_block_time AS block_time
, w.evt_block_number AS block_number
, w.amount AS deposit_amount_raw
, w.amount AS withdraw_amount_raw
, CASE WHEN varbinary_substring(m.sender,1, 12) = 0x000000000000000000000000 THEN varbinary_substring(m.sender,13) ELSE m.sender END AS sender
, w.mintRecipient AS recipient
, i.token_standard AS deposit_token_standard
, 'erc20' AS withdraw_token_standard
--, w.burnToken AS deposit_token_address
, w.mintToken AS withdraw_token_address
, w.evt_tx_from AS tx_from
, w.evt_tx_hash AS tx_hash
, w.evt_index
, w.contract_address
, m.nonce AS transfer_id
FROM {{ source('circle_base', 'tokenmessenger_evt_mintandwithdraw')}} w
INNER JOIN {{ source('circle_base', 'messagetransmitter_evt_messagereceived')}} m ON w.evt_block_number = m.evt_block_number
    and w.evt_index + 1 = m.evt_index
INNER JOIN cctp_id_mapping i ON i.id=m.sourceDomain