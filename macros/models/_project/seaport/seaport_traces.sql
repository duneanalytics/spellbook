{% macro nft_transfers(blockchain, seaport_events) %}
{%- set token_standard_start = 'bep' if blockchain == 'bnb' else 'erc' -%}
{%- set spark_mode = True -%} {# TODO: Potential bug. Consider disabling #}

WITH base_data AS (
     SELECT evt_block_time AS block_time
    , evt_block_number AS block_number
    , orderHash AS order_hash
    , offerer
    , recipient
    , evt_tx_hash AS tx_hash
    , contract_address AS seaport_contract_address
    , consideration
    , offer
    , zone
    FROM '{{seaport_events}}'
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
    )


SELECT '{{blockchain}}' AS blockchain
, date_trunc('day', block_time) AS block_date
, block_time
, block_number
, 'consideration' AS trace_side
, order_hash
, tx_hash
, CASE json_extract_scalar(consideration_item, '$.itemType')
    WHEN '0' THEN 'ETH'
    WHEN '1' THEN '{{token_standard_start}}' || '20'
    WHEN '2' THEN '{{token_standard_start}}' || '721'
    WHEN '3' THEN '{{token_standard_start}}' || '1155'
    END AS token_standard
, consideration_index AS trace_index
, seaport_contract_address
, from_hex(json_extract_scalar(consideration_item, '$.token')) AS token_address
, CAST(json_extract_scalar(consideration_item, '$.amount') AS UINT256) AS amount
, json_extract_scalar(consideration_item, '$.identifier') AS identifier
, from_hex(json_extract_scalar(consideration_item, '$.recipient')) AS recipient
, offerer
, zone
FROM (
    SELECT block_time
    , block_number
    , order_hash
    , tx_hash
    , offerer
    , seaport_contract_address
    , consideration_index
    , consideration_item
    , zone
    FROM base_data
    CROSS JOIN UNNEST(consideration) WITH ordinality AS t (consideration_item, consideration_index)
    )

UNION ALL

SELECT '{{blockchain}}' AS blockchain
, date_trunc('day', block_time) AS block_date
, block_time
, block_number
, 'offer' AS trace_side
, order_hash
, tx_hash
, CASE json_extract_scalar(offer_item, '$.itemType')
    WHEN '0' THEN 'ETH'
    WHEN '1' THEN '{{token_standard_start}}' || '20'
    WHEN '2' THEN '{{token_standard_start}}' || '721'
    WHEN '3' THEN '{{token_standard_start}}' || '1155'
    END AS token_standard
, offer_index AS trace_index
, seaport_contract_address
, from_hex(json_extract_scalar(offer_item, '$.token')) AS token_address
, CAST(json_extract_scalar(offer_item, '$.amount') AS UINT256) AS amount
, json_extract_scalar(offer_item, '$.identifier') AS identifier
, recipient
, offerer
, zone
FROM (
    SELECT block_time
    , block_number
    , order_hash
    , tx_hash
    , recipient
    , offerer
    , seaport_contract_address
    , offer_index
    , offer_item
    , zone
    FROM base_data
    CROSS JOIN UNNEST(offer) WITH ordinality AS t (offer_item, offer_index)
    )

{% endmacro %}