{% macro cex_deposit_addresses(blockchain, cex_local_flows, crosschain_first_funded_by) %}

WITH unique_inflows AS (
    SELECT "from" AS suspected_deposit_address
    , MIN(block_number) AS block_number
    , MIN_BY(unique_key, block_number) AS unique_key
    FROM {{cex_local_flows}}
    {% if is_incremental() %}
    -- LEFT JOIN ON ITSELF TO REMOVE EXISTING ADDRESSES
    -- OR INNER JOIN TO ONLY KEEP RECENTLY CREATED ADDRESS ACROSS CHAINS
    WHERE {{ incremental_predicate('block_time') }}
    AND flow_type IN ('Inflow') --, 'Executed', 'Executed Contract')
    {% else %}
    WHERE flow_type IN ('Inflow') --, 'Executed', 'Executed Contract')
    {% endif %}
    AND block_time > NOW() - interval '4' month 
    AND varbinary_substring("from", 1, 18) <> 0x000000000000000000000000000000000000 -- removing last 3 bytes, often used to identify null or system addresses
    GROUP BY 1
    HAVING COUNT(DISTINCT cex_name) = 1
    )

, unique_inflows_expanded AS (
    SELECT block_number
    , cf.block_time
    , suspected_deposit_address 
    , cf.token_standard
    , cf.token_address
    , CASE WHEN token_standard = 'native' THEN amount+(f.tx_fee) ELSE amount END AS amount
    FROM {{cex_local_flows}} cf
    INNER JOIN unique_inflows ui USING (block_number, unique_key)
    INNER JOIN gas_ethereum.fees f USING (block_number, tx_hash)
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('cf.block_time') }}
    AND {{ incremental_predicate('cf.block_time') }}
    {% endif %}
    )

, in_and_out AS (
    SELECT i.suspected_deposit_address
    , i.amount AS amount_consolidated
    , i.block_time AS consolidation_block_time
    , SUM(t.amount) AS amount_deposited
    , MIN(t.block_time) AS deposit_first_block_time
    , MAX(t.block_time) AS deposit_last_block_time
    FROM {{ source('tokens_'~blockchain,'transfers') }} t
    INNER JOIN unique_inflows_expanded i ON t.block_number<i.block_number
        AND t.to=i.suspected_deposit_address
        AND t.token_standard=i.token_standard
        AND t.contract_address=i.token_address
        AND t.block_time BETWEEN i.block_time - interval '1' day AND i.block_time
        --AND i.amount_raw BETWEEN t.amount_raw*0.9 AND t.amount_raw*1.1
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('t.block_time') }}
    {% endif %}
    GROUP BY 1, 2
    )

SELECT suspected_deposit_address
, amount_consolidated
, consolidation_block_time
, amount_deposited
, deposit_first_block_time
, deposit_last_block_time
FROM in_and_out
WHERE amount_consolidated < amount_deposited*1.1

{% endmacro %}
