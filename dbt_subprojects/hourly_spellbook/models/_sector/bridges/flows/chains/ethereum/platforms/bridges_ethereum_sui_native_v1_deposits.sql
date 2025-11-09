{% set blockchain = 'ethereum' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'sui_native_v1_deposits',
    materialized = 'view',
    )
}}

SELECT '{{blockchain}}' AS deposit_chain
, sourceChainID AS withdrawal_chain_id
, 'sui' AS withdrawal_chain
, 'Sui' AS bridge_name
, '1' AS bridge_version
, evt_block_date AS block_date
, evt_block_time AS block_time
, evt_block_number AS block_number
, suiAdjustedAmount AS deposit_amount_raw
, senderAddress AS sender
, recipientAddress AS recipient
, 'erc20' AS deposit_token_standard
, tokenID AS deposit_token_address
, evt_tx_from AS tx_from
, evt_tx_hash AS tx_hash
, evt_index
, contract_address
, CAST(nonce AS varchar) as bridge_transfer_id
FROM {{ source('suibridge_ethereum', 'suibridge_evt_tokensdeposited') }} d