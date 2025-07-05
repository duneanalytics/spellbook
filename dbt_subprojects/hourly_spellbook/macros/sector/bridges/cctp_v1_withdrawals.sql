{% macro cctp_v1_withdrawals(blockchain) %}

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

SELECT i.blockchain AS deposit_chain
, '{{blockchain}}' AS withdrawal_chain
, 'CCTP' AS bridge_name
, '1' AS bridge_version
, w.evt_block_date AS block_date
, w.evt_block_time AS block_time
, w.evt_block_number AS block_number
, w.amount AS withdrawal_amount_raw
, CASE WHEN varbinary_substring(m.sender,1, 12) = 0x000000000000000000000000 THEN varbinary_substring(m.sender,13) ELSE m.sender END AS sender
, w.mintRecipient AS recipient
, i.token_standard AS deposit_token_standard
, 'erc20' AS withdrawal_token_standard
, w.mintToken AS withdrawal_token_address
, w.evt_tx_from AS tx_from
, w.evt_tx_hash AS tx_hash
, w.evt_index
, w.contract_address
, CAST(m.nonce AS varchar) AS transfer_id
FROM {{ source('circle_'  + blockchain, 'tokenmessenger_evt_mintandwithdraw')}} w
INNER JOIN  {{ source('circle_'  + blockchain, 'messagetransmitter_evt_messagereceived')}} m ON w.evt_block_number = m.evt_block_number
    and w.evt_index + 1 = m.evt_index
INNER JOIN cctp_id_mapping i ON i.id=m.sourceDomain

{% endmacro %}