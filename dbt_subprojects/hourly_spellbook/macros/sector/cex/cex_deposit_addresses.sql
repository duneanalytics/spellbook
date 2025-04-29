{% macro cex_deposit_addresses(blockchain, cex_local_flows, crosschain_first_funded_by) %}

WITH deposits AS (
    SELECT f."from" AS address
    , f.cex_name
    , f.token_standard
    , CASE WHEN f."from"=f.tx_from THEN true ELSE false END AS self_executed
    , MIN(f.block_time) AS block_time
    , MIN(f.block_number) AS block_number
    , MIN_BY(f.tx_hash, f.block_number) AS hash
    FROM {{cex_local_flows}} f
    WHERE f.flow_type = 'Inflow'
        {% if is_incremental() %}
        AND {{ incremental_predicate('block_time') }}
        {% endif %}
    GROUP BY 1, 2, 3, 4
    HAVING COUNT(DISTINCT f.cex_name) = 1
    )

, isolate_unique AS (
    SELECT address
    FROM deposits
    GROUP BY address
    HAVING COUNT(DISTINCT cex_name) = 1
    )

SELECT '{{blockchain}}' AS blockchain
, d.address
, d.cex_name
, d.token_standard
, d.block_time AS deposit_block_time
, d.block_number AS deposit_block_number
, ffb.block_time AS funded_block_time
, ffb.block_number AS funded_block_number
, ffb.first_funded_by
, ffb.first_funding_executed_by
, d.self_executed
, d.hash AS tx_hash
FROM deposits d
INNER JOIN isolate_unique iu ON iu.address=d.address
    --AND iu.token_standard=d.token_standard 
-- check that the address was first funded on same chain and recently
INNER JOIN {{crosschain_first_funded_by}} ffb ON ffb.blockchain='{{blockchain}}'
    AND ffb.address=d.address
    AND ffb.block_time BETWEEN d.block_time - interval '6' hour AND d.block_time
    {% if is_incremental() %}
    AND {{ incremental_predicate("ffb.block_time - interval '6' hour") }}
    {% endif %}

{% endmacro %}
