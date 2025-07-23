{% macro cctp_v1_deposits(blockchain) %}

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

SELECT '{{blockchain}}' AS deposit_chain
, i.blockchain AS withdrawal_chain
, 'CCTP' AS bridge_name
, '1' AS bridge_version
, evt_block_date AS block_date
, evt_block_time AS block_time
, evt_block_number AS block_number
, amount AS deposit_amount_raw
, depositor AS sender
, CASE WHEN varbinary_substring(mintRecipient,1, 12) = 0x000000000000000000000000 THEN varbinary_substring(mintRecipient,13) ELSE mintRecipient END AS recipient
, 'erc20' AS deposit_token_standard
, 'erc20' AS withdrawal_token_standard
, burnToken AS deposit_token_address
, evt_tx_from AS tx_from
, evt_tx_hash AS tx_hash
, evt_index
, contract_address
, CAST(nonce AS varchar) AS bridge_transfer_id
FROM {{ source('circle_' + blockchain, 'tokenmessenger_evt_depositforburn') }} d
INNER JOIN cctp_id_mapping i ON d.destinationDomain=i.id

{% endmacro %}