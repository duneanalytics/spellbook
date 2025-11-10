{% set blockchain = 'ethereum' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'sui_native_v1_withdrawals',
    materialized = 'view',
    )
}}

WITH token_ids AS (
    SELECT 2 AS tokenID, 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 AS withdrawal_token_address
    UNION ALL
    SELECT CAST(tokenIDs[1] AS int) AS tokenID, tokenAddresses[1] AS withdrawal_token_address
    FROM {{ source('suibridge_ethereum', 'bridgeconfig_evt_tokensaddedv2') }}
    )

SELECT 'sui' AS deposit_chain
, sourceChainID AS deposit_chain_id
, '{{blockchain}}' AS withdrawal_chain
, 'Sui' AS bridge_name
, '1' AS bridge_version
, evt_block_date AS block_date
, evt_block_time AS block_time
, evt_block_number AS block_number
, erc20AdjustedAmount AS withdrawal_amount_raw
, senderAddress AS sender
, recipientAddress AS recipient
, 'erc20' AS withdrawal_token_standard
, tokenID AS withdrawal_token_address
, evt_tx_from AS tx_from
, evt_tx_hash AS tx_hash
, evt_index
, contract_address
, CAST(nonce AS varchar) as bridge_transfer_id
FROM {{ source('suibridge_ethereum', 'suibridge_evt_tokensclaimed') }} d
LEFT JOIN token_ids USING (tokenID)