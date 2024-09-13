{% macro cex_deposit_addresses(blockchain, token_transfers, cex_addresses, cex_flows, first_funded_by) %}

WITH potential_addresses AS (
    SELECT ROW_NUMBER() OVER (PARTITION BY f."from") AS row_number
    , f."from" AS potential_deposit
    , f.to AS cex_address
    , f.cex_name
    , MAX(f.cex_name = affb.cex_name) AS funded_by_same_cex
    , ffb.block_time AS creation_block_time
    , ffb.block_number AS creation_block_number
    FROM {{cex_flows}} f
    INNER JOIN {{first_funded_by}} ffb ON ffb.address=f."from"
        AND ffb.block_time BETWEEN f.block_time - interval '1' day AND f.block_time
        AND ffb.block_time > NOW() - interval '6' month
        {% if is_incremental() %}
        AND {{incremental_predicate('ffb.block_time')}}
        {% endif %}
    -- check if funder is the same cex
    LEFT JOIN {{cex_addresses}} affb ON ffb.first_funded_by=affb.address
        AND f.cex_name=affb.cex_name
    WHERE f.flow_type = 'Inflow'
    AND f.block_time > NOW() - interval '6' month
    {% if is_incremental() %}
    AND {{incremental_predicate('f.block_time')}}
    {% endif %}
    GROUP BY f."from", f.to, f.cex_name, ffb.block_time, ffb.block_number
    )

, unique_addresses AS (
    SELECT potential_deposit
    FROM potential_addresses
    GROUP BY potential_deposit
    HAVING COUNT(*) = 1
    )

, sent_tokens AS (
    SELECT pa.potential_deposit
    , pa.cex_name
    , pa.funded_by_same_cex
    , tt.contract_address
    , tt.token_standard
    , pa.creation_block_time
    , pa.creation_block_number
    , SUM(tt.amount) AS sent
    FROM {{cex_flows}} tt
    INNER JOIN potential_addresses pa ON pa.potential_deposit=tt."from"
        AND tt.to=pa.cex_address
    INNER JOIN unique_addresses ua ON ua.potential_deposit=pa.potential_deposit
    WHERE tt.block_time BETWEEN pa.creation_block_time AND pa.creation_block_time + INTERVAL '1' DAY
    GROUP BY pa.potential_deposit, pa.cex_name, pa.funded_by_same_cex, tt.contract_address, tt.token_standard, pa.creation_block_time, pa.creation_block_number
    )

, sent_and_received AS (
    SELECT st.potential_deposit
    , st.cex_name
    , st.funded_by_same_cex
    , st.contract_address
    , st.token_standard
    , st.creation_block_time
    , st.creation_block_number
    , SUM(tt.amount) AS deposited
    , st.sent
    FROM {{token_transfers}} tt
    INNER JOIN sent_tokens st ON st.potential_deposit=tt."to"
    WHERE tt.block_time BETWEEN st.creation_block_time - INTERVAL '18' HOUR AND st.creation_block_time + INTERVAL '6' DAY
    GROUP BY st.potential_deposit, st.cex_name, st.funded_by_same_cex, st.contract_address, st.token_standard, st.creation_block_time, st.creation_block_number, st.sent
    )

SELECT '{{blockchain}}' AS blockchain
, potential_deposit AS address
, sar.cex_name
, sar.creation_block_time
, sar.creation_block_number
, sar.funded_by_same_cex
FROM sent_and_received sar
{% if is_incremental() %}
LEFT JOIN {{this}} eda ON potential_deposit = eda.address 
    AND COALESCE(eda.address, 'NULL') = 'NULL'
{% endif %}
WHERE sar.deposited > 0
AND sar.sent > 0
AND (sar.deposited=sent OR
    (sar.token_standard='native' AND sar.sent BETWEEN GREATEST(sar.deposited - 0.02, 0) AND sar.deposited)) -- Will lose some to gas if native token

{% endmacro %}
