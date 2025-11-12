{% set blockchain = 'solana' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'across_v3_deposits',
    materialized = 'view',
    )
}}

SELECT 'solana' AS deposit_chain
, destination_chain_id AS withdrawal_chain_id
, NULL AS withdrawal_chain
, 'Across' AS bridge_name
, '3' AS bridge_version
, evt_block_date AS block_date
, evt_block_time AS block_time
, evt_block_slot AS block_slot
, input_amount AS deposit_amount_raw
, depositor AS sender
, recipient
, 'erc20' AS deposit_token_standard
, input_token AS deposit_token_address
, evt_tx_signer AS tx_signer
, evt_executing_account AS executing_account
, evt_tx_id AS tx_id
, evt_inner_instruction_index AS evt_index
, evt_executing_account AS contract_address
, CAST(NULL AS varchar) AS bridge_transfer_id
FROM {{ source('across_v3_solana', 'svm_spoke_evt_fundsdeposited') }}