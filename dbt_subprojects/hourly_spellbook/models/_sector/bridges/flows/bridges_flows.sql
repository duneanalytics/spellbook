{{ config(
    schema = 'bridges',
    alias = 'flows',
    materialized = 'view'
    )
}}

SELECT COALESCE(i.deposit_chain, f.deposit_chain) AS deposit_chain
, COALESCE(i.withdrawal_chain, f.withdrawal_chain) AS withdrawal_chain
, COALESCE(i.project, f.project) AS project
, COALESCE(i.project_version, f.project_version) AS project_version
, CASE WHEN i.event_side IS NOT NULL AND f.event_side IS NOT NULL THEN 'both'
    WHEN i.event_side IS NOT NULL THEN 'initiated'
    ELSE 'finalised'
    END AS event_side
, COALESCE(i.block_date, f.block_date) AS block_date
, COALESCE(i.block_time, f.block_time) AS block_time
, COALESCE(i.block_number, f.block_number) AS block_number
, date_diff('milisecond', i.block_time, f.block_time) AS bridge_miliseconds
, 1000 * (f.block_time - i.block_time) AS bridge_miliseconds_2
, COALESCE(i.sender, f.sender) AS sender
, COALESCE(i.recipient, f.recipient) AS recipient
, COALESCE(i.deposit_amount_raw, f.deposit_amount_raw) AS deposit_amount_raw
, COALESCE(i.deposit_amount, f.deposit_amount) AS deposit_amount
, COALESCE(i.deposit_amount_usd, f.deposit_amount_usd) AS deposit_amount_usd
, COALESCE(i.deposit_token_address, f.deposit_token_address) AS deposit_token_address
, COALESCE(i.deposit_token_standard, f.deposit_token_standard) AS deposit_token_standard
, COALESCE(i.withdrawal_amount_raw, f.withdrawal_amount_raw) AS withdrawal_amount_raw
, COALESCE(i.withdrawal_amount, f.withdrawal_amount) AS withdrawal_amount
, COALESCE(i.withdrawal_amount_usd, f.withdrawal_amount_usd) AS withdrawal_amount_usd
, COALESCE(i.withdrawal_token_address, f.withdrawal_token_address) AS withdrawal_token_address
, COALESCE(i.withdrawal_token_standard, f.withdrawal_token_standard) AS withdrawal_token_standard
, COALESCE(i.withdrawal_token_symbol, f.withdrawal_token_symbol) AS withdrawal_token_symbol
, i.tx_from -- tx_from on finalised chain is irrelevant
, i.tx_hash AS initiated_tx_hash
, f.tx_hash AS finalised_tx_hash
FROM {{ ref('bridges_deposits') }} i
FULL OUTER JOIN {{ ref('bridges_withdrawals') }} f USING (bridge_id)