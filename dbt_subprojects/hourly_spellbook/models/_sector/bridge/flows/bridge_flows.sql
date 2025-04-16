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
, COALESCE(i.source_address, f.source_address) AS source_address
, COALESCE(i.destination_address, f.destination_address) AS destination_address
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
, i.tx_from AS initiated_tx_from
, f.tx_from AS finalised_tx_from
, i.tx_hash AS initiated_tx_hash
, f.tx_hash AS finalised_tx_hash
, i.evt_index AS initiated_evt_index
, f.evt_index AS finalised_evt_index
, i.contract_address AS initiated_contract_address
, f.contract_address AS finalised_contract_address
FROM {{ ref('bridge_initiated') }} i
FULL OUTER JOIN {{ ref('bridge_finalised') }} f USING (bridge_id)