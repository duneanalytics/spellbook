{% macro seaport_traces(blockchain, seaport_events) %}
{%- set token_standard_start = 'bep' if blockchain == 'bnb' else 'erc' -%}
-- unique over: [tx_hash, evt_index, (order_hash), trace_side, trace_index]
-- note that this will contain duplicates for cases where duplicate events are emitted for the same order(hash) being fulfilled.
-- This is the case when multiple orders are matched in 1 tx
-- any child models should still account for these duplicates to prevent double counting those trades.

WITH base_data AS (
     SELECT evt_block_time AS block_time
    , evt_block_number AS block_number
    , orderHash AS order_hash
    , offerer
    , recipient
    , evt_tx_hash AS tx_hash
    , evt_index
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
    , evt_index
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
    , evt_index
    , from_hex(json_extract_scalar(trace_item, '$.recipient')) AS recipient
    , offerer
    , seaport_contract_address
    , 'consideration' AS trace_side
    , trace_index
    , trace_item
    , zone
    FROM base_data
    CROSS JOIN UNNEST(consideration) WITH ordinality AS t (trace_item, trace_index)
    )

, all_traces AS (
    SELECT block_time
    , block_number
    , order_hash
    , tx_hash
    , evt_index
    , json_extract_scalar(trace_item, '$.itemType') AS item_type
    , from_hex(json_extract_scalar(trace_item, '$.token')) AS token_address
    , CAST(json_extract_scalar(trace_item, '$.amount') AS UINT256) AS amount
    , CAST(json_extract_scalar(trace_item, '$.identifier') AS UINT256) AS identifier
    , recipient
    , offerer
    , seaport_contract_address
    , trace_index
    , trace_side
    , zone
    FROM exploded_traces
    )

SELECT '{{blockchain}}' AS blockchain
, date_trunc('day', block_time) AS block_date
, block_time
, block_number
, order_hash
, tx_hash
, evt_index
, CASE item_type
    WHEN '0' THEN 'native'
    WHEN '1' THEN '{{token_standard_start}}' || '20'
    WHEN '2' THEN '{{token_standard_start}}' || '721'
    WHEN '3' THEN '{{token_standard_start}}' || '1155'
    END AS token_standard
, token_address
, amount
, identifier
, recipient
, offerer
, seaport_contract_address
-- Seaport versions documented here: https://github.com/ProjectOpenSea/seaport
, CASE WHEN seaport_contract_address = 0x00000000006c3852cbef3e08e8df289169ede581 THEN '1.1'
    WHEN seaport_contract_address = 0x00000000000006c7676171937c444f6bde3d6282 THEN '1.2'
    WHEN seaport_contract_address = 0x0000000000000ad24e80fd803c6ac37206a45f15 THEN '1.3'
    WHEN seaport_contract_address = 0x00000000000001ad428e4906ae43d8f9852d0dd6 THEN '1.4'
    WHEN seaport_contract_address = 0x00000000000000adc04c56bf30ac9d3c0aaf14dc THEN '1.5'
    ELSE 'unknown'
    END AS seaport_version
, trace_side
, trace_index
, zone
FROM all_traces

{% endmacro %}
