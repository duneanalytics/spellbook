{% macro cex_deposit_addresses(blockchain, token_transfers, cex_addresses, cex_flows, first_funded_by) %}

WITH potential_addresses AS (
    SELECT DISTINCT f."from" AS potential_deposit
    , f.to AS cex_address
    , f.cex_name
    , CASE WHEN f.cex_name=affb.cex_name THEN true ELSE false END AS funded_by_same_cex
    , ffb.block_time AS creation_block_time
    , ffb.block_number AS creation_block_number
    FROM {{cex_flows}} f
    -- check that it's not another cex address
    LEFT JOIN {{cex_addresses}} b ON b.address=f."from"
        AND b.address IS NULL
    INNER JOIN {{first_funded_by}} ffb ON ffb.address=f."from"
        {% if is_incremental() %}
        AND {{incremental_predicate('ffb.block_time')}}
        {% endif %}
    -- check if funder is the same cex
    LEFT JOIN {{cex_addresses}} affb ON ffb.first_funded_by=affb.address
        AND f.cex_name=affb.cex_name
    {% if is_incremental() %}
    WHERE {{incremental_predicate('f.block_time')}}
    {% endif %}
    )

, potential_addresses_fund_movements AS (
    -- Recieved
    SELECT pa.potential_deposit
    , pa.cex_name
    , pa.funded_by_same_cex
    , t.contract_address
    , t.token_standard
    , pa.creation_block_time
    , pa.creation_block_number
    , amount AS deposited
    , 0 AS sent
    FROM {{token_transfers}} t
    INNER JOIN potential_addresses pa ON pa.potential_deposit=t."to"
    -- Exclude received from cex addresses
    LEFT JOIN {{cex_addresses}} cex ON cex.address=t."from"
        AND cex.address IS NULL
    WHERE t.block_time BETWEEN pa.creation_block_time - interval '18' hour AND pa.creation_block_time + interval '6' day
    
    UNION ALL
    
    -- Sent
    SELECT pa.potential_deposit
    , pa.cex_name
    , pa.funded_by_same_cex
    , t.contract_address
    , t.token_standard
    , pa.creation_block_time
    , pa.creation_block_number
    , 0 AS deposited
    , amount AS sent
    FROM {{token_transfers}} t
    INNER JOIN potential_addresses pa ON pa.potential_deposit=t."from"
    AND t.to=pa.cex_address
    WHERE t.block_time BETWEEN pa.creation_block_time AND pa.creation_block_time + interval '1' day
    )

, potential_addresses_fund_movements_aggregated AS (
    SELECT potential_deposit
    , cex_name
    , funded_by_same_cex
    , contract_address
    , token_standard
    , creation_block_time
    , creation_block_number
    , SUM(deposited) AS deposited
    , SUM(sent) AS sent
    FROM potential_addresses_fund_movements
    GROUP BY 1, 2, 3, 4, 5, 6, 7
    )

SELECT '{{blockchain}}' AS blockchain
, potential_deposit AS address
, cex_name
, creation_block_time
, creation_block_number
, funded_by_same_cex
FROM potential_addresses_fund_movements_aggregated
WHERE deposited > 0
AND sent > 0
AND (deposited=sent OR
    (token_standard='native' AND sent BETWEEN GREATEST(deposited - 0.02, 0) AND deposited)) -- Will lose some to gas if native token

{% endmacro %}