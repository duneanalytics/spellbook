{% macro cex_evms(cex_addresses, blockchain, token_transfers, contract_creations) %}

WITH first_appearance AS (
    -- contract creations
    SELECT a.address
    , cc.block_time AS first_used
    , true AS is_contract
    FROM {{contract_creations}} cc
    INNER JOIN {{cex_addresses}} a ON a.address = cc.address
    {% if is_incremental() %}
    LEFT JOIN {{this}} b ON a.address = b.address
    WHERE b.cex_name IS NULL
    AND {{incremental_predicate('cc.block_time')}}
    {% endif %}
    
    UNION ALL

    -- first token received
    SELECT a.address
    , MIN(t.block_time) AS first_used
    , false AS is_contract
    FROM {{token_transfers}} t
    INNER JOIN {{cex_addresses}} a ON a.address = t.to
    {% if is_incremental() %}
    LEFT JOIN {{this}} b ON a.address = b.address
    WHERE b.cex_name IS NULL
    AND {{incremental_predicate('t.block_time')}}
    {% endif %}
    GROUP BY a.address
    )

, unique_entries AS (
    SELECT address
    , MIN(first_used) AS first_used
    , MAX(is_contract) AS is_contract
    FROM first_appearance
    GROUP BY 1
    )

SELECT '{{blockchain}}' AS blockchain
, address
, ca.cex_name
, ca.distinct_name
, ca.added_by
, ca.added_date
, n.first_used
, n.is_contract
FROM unique_entries n
INNER JOIN {{cex_addresses}} ca USING (address)

{% endmacro %}