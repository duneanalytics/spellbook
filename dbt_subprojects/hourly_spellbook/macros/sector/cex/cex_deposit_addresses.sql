{% macro cex_deposit_addresses(blockchain, transactions, token_transfers, cex_addresses, cex_flows, first_funded_by, creation_traces) %}

-- start by looking at addresses that sent tokens to an identified cex address
WITH potential_addresses AS (
    SELECT f."from" AS potential_deposit
    , ffb.first_funded_by
    , f.to AS cex_address
    , f.cex_name
    , COALESCE(MAX(f.cex_name = affb.cex_name), false) AS funded_by_same_cex
    , ffb.block_time AS creation_block_time
    , ffb.block_number AS creation_block_number
    , f.token_address
    , f.token_standard
    , SUM(f.amount) AS outflow_amount
    , array_agg(DISTINCT f.tx_hash) AS distinct_transfer_hashes
    FROM {{cex_flows}} f
    -- ignore addresses already found in previous incremental runs
    {% if is_incremental() %}
    LEFT JOIN {{this}} a ON a.address = f."from"
        AND a.address IS NULL
    {% endif %}
    -- check that it's not another cex address
    LEFT JOIN {{cex_addresses}} b ON b.address = f."from"
        AND b.address IS NULL
    -- make sure it was first funded recently
    INNER JOIN {{first_funded_by}} ffb ON ffb.address = f."from"
        AND ffb.block_time > ffb.block_time - interval '2' day
        {% if is_incremental() %}
        AND {{incremental_predicate('ffb.block_time')}}
        {% endif %}
    -- check if funder is the same cex
    LEFT JOIN {{cex_addresses}} affb ON ffb.first_funded_by = affb.address
        AND f.cex_name = affb.cex_name
    WHERE f.flow_type = 'Inflow'
    {% if is_incremental() %}
    AND {{incremental_predicate('f.block_time')}}
    {% endif %}
    GROUP BY f."from", ffb.first_funded_by, f.to, f.cex_name, f.token_address, f.token_standard, ffb.block_time, ffb.block_number
    )

-- fetch inflows for outflown tokens
, inflows_and_outflows AS (
    SELECT pa.potential_deposit
    , pa.first_funded_by
    , pa.cex_address
    , pa.cex_name
    , pa.funded_by_same_cex
    , pa.creation_block_time
    , pa.creation_block_number
    , pa.token_address
    , pa.token_standard
    , pa.outflow_amount
    , pa.distinct_transfer_hashes
    , SUM(tt.amount) AS inflow_amount
    FROM {{token_transfers}} tt
    INNER JOIN potential_addresses pa ON pa.potential_deposit = tt.to
        AND tt.block_time BETWEEN pa.creation_block_time - INTERVAL '2' day AND pa.creation_block_time
        AND pa.token_address = tt.contract_address
        AND tt.amount <= pa.outflow_amount
    GROUP BY pa.potential_deposit,  pa.first_funded_by, pa.cex_address, pa.cex_name, pa.funded_by_same_cex, pa.creation_block_time, pa.creation_block_number, pa.token_address, pa.token_standard, pa.outflow_amount,  pa.distinct_transfer_hashes
    )

-- ensure the address only deposited to one cex address
, unique_cex_recipient AS (
    SELECT potential_deposit
    , COUNT(DISTINCT tx_hash) AS distinct_transfer_hashes_count
    FROM inflows_and_outflows
    CROSS JOIN UNNEST(distinct_transfer_hashes) AS t(tx_hash)
    GROUP BY potential_deposit
    HAVING COUNT(DISTINCT cex_address) = 1
    )

SELECT DISTINCT '{{blockchain}}' AS blockchain
, potential_deposit AS address
, i.first_funded_by
, i.cex_address
, i.cex_name
, i.funded_by_same_cex
, i.creation_block_time
, i.creation_block_number
, CASE WHEN MAX(ct.tx_hash) IS NULL THEN true ELSE false END AS is_smart_contract
FROM inflows_and_outflows i
INNER JOIN unique_cex_recipient ua USING (potential_deposit)
-- check that it never sent to other addresses
LEFT JOIN {{token_transfers}} tt ON tt."from" = potential_deposit
    AND tt.to != i.cex_address
    AND tt.amount IS NULL
-- check that it executed fewer or equal tx count to the number of outflow txs
LEFT JOIN {{transactions}} txs ON txs."from" = potential_deposit
    AND txs.nonce > ua.distinct_transfer_hashes_count
    AND txs.block_number IS NULL
-- check if it's a smart contract
LEFT JOIN {{creation_traces}} ct ON ct.address = potential_deposit
-- ensure non-zero amounts were flown in + out and only keep if outflow matches inflow (allows slightly less if it's the native gas token)
WHERE i.inflow_amount > 0
AND i.outflow_amount > 0
AND (i.outflow_amount = i.inflow_amount OR
    (i.token_standard = 'native' AND i.outflow_amount BETWEEN GREATEST(i.inflow_amount - 0.05, 0) AND i.inflow_amount)) -- Can lose some to gas if native token
GROUP BY potential_deposit, i.first_funded_by, i.cex_address, i.cex_name, i.funded_by_same_cex, i.creation_block_time, i.creation_block_number
{% endmacro %}
