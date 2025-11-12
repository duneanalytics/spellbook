{% set blockchain = 'solana' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'across_v3_withdrawals',
    materialized = 'view',
    )
}}

SELECT NULL AS deposit_chain
, origin_chain_id AS deposit_chain_id
, 'solana' AS withdrawal_chain
, 'Across' AS bridge_name
, '3' AS bridge_version
, evt_block_date AS block_date
, evt_block_time AS block_time
, evt_block_slot AS block_slot
, output_amount AS withdrawal_amount_raw
, depositor AS sender
, recipient
, 'erc20' AS withdrawal_token_standard
, output_token AS withdrawal_token_address
, evt_tx_signer AS tx_signer
, evt_executing_account AS executing_account
, evt_tx_id AS tx_id
, evt_inner_instruction_index AS evt_index
, relayer AS contract_address
, CAST(NULL AS varchar) AS bridge_transfer_id
FROM {{ source('across_v3_solana', 'svm_spoke_evt_filledrelay') }}