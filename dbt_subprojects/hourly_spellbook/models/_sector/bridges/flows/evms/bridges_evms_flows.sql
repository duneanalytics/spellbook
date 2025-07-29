{{ config(
    schema = 'bridges_evms'
    , alias = 'flows'
    , materialized = 'view'
    )
}}

SELECT deposit_chain
, withdrawal_chain
, bridge_name
, bridge_version
, d.block_date AS deposit_block_date
, d.block_time AS deposit_block_time
, d.block_number AS deposit_block_number
, w.block_date AS withdraw_block_date
, w.block_time AS withdraw_block_time
, w.block_number AS withdraw_block_number
, d.deposit_amount_raw
, d.deposit_amount
, w.withdrawal_amount_raw
, w.withdrawal_amount
, COALESCE(d.deposit_amount_usd, w.withdrawal_amount_usd) AS amount_usd
, COALESCE(d.sender, w.sender) AS sender
, COALESCE(w.recipient, d.recipient) AS recipient
, d.deposit_token_standard
, w.withdrawal_token_standard
, d.deposit_token_address
, w.withdrawal_token_address
, d.tx_from AS deposit_tx_from
, d.tx_hash AS deposit_tx_hash
, w.tx_hash AS withdraw_tx_hash
, bridge_transfer_id
FROM {{ ref('bridges_evms_deposits') }} d
FULL OUTER JOIN {{ ref('bridges_evms_withdrawals') }} w USING (bridge_name, bridge_version, deposit_chain, withdrawal_chain, bridge_transfer_id)