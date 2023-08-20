{% macro seaport_traces(blockchain, seaport_events) %}
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
    FROM {{ seaport_events }}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
    )

, exploded_traces AS (
    SELECT block_time
    , block_number
    , order_hash
    , tx_hash
    , recipient
    , offerer
    , seaport_contract_address
    , 'offer' AS trace_side
    , trace_index
    , trace_item
    , zone
    FROM base_data
    CROSS JOIN UNNEST(offer) WITH ordinality AS t (trace_item, trace_index)
    
    UNION ALL

    SELECT block_time
    , block_number
    , order_hash
    , tx_hash
    , from_hex(json_extract_scalar(consideration_item, '$.recipient')) AS recipient
    , offerer
    , seaport_contract_address
    , 'consideration' AS trace_side
    , trace_index
    , trace_item
    , zone
    FROM base_data
    CROSS JOIN UNNEST(consideration) WITH ordinality AS t (trace_item, trace_index)
    )

SELECT '{{blockchain}}' AS blockchain
, date_trunc('day', block_time) AS block_date
, block_time
, block_number
, order_hash
, tx_hash
, CASE json_extract_scalar(trace_item, '$.itemType')
    WHEN '0' THEN 'native'
    WHEN '1' THEN '{{token_standard_start}}' || '20'
    WHEN '2' THEN '{{token_standard_start}}' || '721'
    WHEN '3' THEN '{{token_standard_start}}' || '1155'
    END AS token_standard
, from_hex(json_extract_scalar(trace_item, '$.token')) AS token_address
, CAST(json_extract_scalar(trace_item, '$.amount') AS UINT256) AS amount
, json_extract_scalar(trace_item, '$.identifier') AS identifier
, recipient
, offerer
, seaport_contract_address
, trace_side
, trace_index
, trace_item
, zone
FROM exploded_traces

{% endmacro %}