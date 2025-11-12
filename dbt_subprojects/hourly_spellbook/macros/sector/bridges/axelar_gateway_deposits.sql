{% macro axelar_gateway_deposits(blockchain, events) %}

SELECT '{{blockchain}}' AS deposit_chain
, CAST(NULL AS uint256) AS withdrawal_chain_id
, CASE lower(d.destinationChain)
    WHEN 'secret-snip' THEN 'secret'
    WHEN 'axelarnet' THEN 'axelar'
    WHEN 'terra-2' THEN 'terra'
    WHEN 'cosmoshub' THEN 'cosmos'
    WHEN 'avalanche' THEN 'avalanche_c'
    WHEN 'assetmantle' THEN 'mantle'
    WHEN 'osmosis-3' THEN 'osmosis'
    ELSE lower(d.destinationChain) END AS withdrawal_chain
, 'Axelar' AS bridge_name
, 'Gateway' AS bridge_version
, d.evt_block_date AS block_date
, d.evt_block_time AS block_time
, d.evt_block_number AS block_number
, d.amount AS deposit_amount_raw
, d.sender
, d.destinationAddress AS recipient
, ti.token_address AS deposit_token_address
, 'erc20' AS deposit_token_standard
, 'erc20' AS withdrawal_token_standard
, d.evt_tx_from AS tx_from
, d.evt_tx_hash AS tx_hash
, d.evt_index
, d.contract_address
, {{ dbt_utils.generate_surrogate_key(['d.evt_tx_hash', 'd.evt_index']) }} as bridge_transfer_id
FROM ({{ events }}) d
LEFT JOIN {{ ref('bridges_axelar_gateway_token_indexes') }} ti ON d.symbol=ti.token_symbol

{% endmacro %}