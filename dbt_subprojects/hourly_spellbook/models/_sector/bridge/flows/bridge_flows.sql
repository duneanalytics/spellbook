{{ config(
    schema = 'bridge',
    alias = 'flows',
    materialized = 'view'
    )
}}

SELECT COALESCE(i.source_chain, f.source_chain) AS source_chain
, COALESCE(i.destination_chain, f.destination_chain) AS destination_chain
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
, COALESCE(i.source_amount_raw, f.source_amount_raw) AS source_amount_raw
, COALESCE(i.source_amount, f.source_amount) AS source_amount
, COALESCE(i.source_amount_usd, f.source_amount_usd) AS source_amount_usd
, COALESCE(i.source_token_address, f.source_token_address) AS source_token_address
, COALESCE(i.source_token_standard, f.source_token_standard) AS source_token_standard
, COALESCE(i.destination_amount_raw, f.destination_amount_raw) AS destination_amount_raw
, COALESCE(i.destination_amount, f.destination_amount) AS destination_amount
, COALESCE(i.destination_amount_usd, f.destination_amount_usd) AS destination_amount_usd
, COALESCE(i.destination_token_address, f.destination_token_address) AS destination_token_address
, COALESCE(i.destination_token_standard, f.destination_token_standard) AS destination_token_standard
, COALESCE(i.destination_token_symbol, f.destination_token_symbol) AS destination_token_symbol
, i.tx_from -- tx_from on finalised chain is irrelevant
, i.tx_hash AS initiated_tx_hash
, f.tx_hash AS finalised_tx_hash
FROM {{ ref('bridge_initiated') }} i
FULL OUTER JOIN {{ ref('bridge_finalised') }} f USING (bridge_id)