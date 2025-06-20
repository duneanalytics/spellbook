{% macro cex_deposit_addresses(blockchain, cex_local_flows, local_gas_fees) %}

WITH unique_inflows AS (
    SELECT "from" AS suspected_deposit_address
    , MIN(block_number) AS block_number
    , MIN_BY(unique_key, block_number) AS unique_key
    FROM {{cex_local_flows}} cf
    {% if is_incremental() %}
    LEFT JOIN {{this}} t ON cf."from"=t.address
    WHERE {{ incremental_predicate('block_time') }}
    AND flow_type IN ('Inflow') --, 'Executed', 'Executed Contract')
    AND t.address IS NULL
    {% else %}
    WHERE flow_type IN ('Inflow') --, 'Executed', 'Executed Contract')
    {% endif %}
    AND varbinary_substring("from", 1, 16) <> 0x00000000000000000000000000000000 -- removing last 5 bytes, often used to identify null or system addresses
    GROUP BY 1
    HAVING COUNT(DISTINCT cex_name) = 1
    )

, unique_inflows_expanded AS (
    SELECT block_number
    , cf.block_time
    , suspected_deposit_address 
    , cf.token_standard
    , cf.token_address
    , CASE WHEN cf.token_standard = 'native' THEN cf.amount+(f.tx_fee) ELSE cf.amount END AS amount
    , cex_name
    FROM {{cex_local_flows}} cf
    INNER JOIN unique_inflows ui USING (block_number, unique_key)
    INNER JOIN {{local_gas_fees}} f USING (block_number, tx_hash)
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('cf.block_time') }}
    AND {{ incremental_predicate('f.block_time') }}
    {% endif %}
    
    )

, in_and_out AS (
    {% if blockchain == 'ethereum' %}

    SELECT suspected_deposit_address
    , cex_name
    , amount_consolidated
    , consolidation_block_time
    , SUM(amount_deposited) AS amount_deposited
    , MIN(deposit_first_block_time) AS deposit_first_block_time
    , MAX(deposit_last_block_time) AS deposit_last_block_time
        FROM (
        SELECT i.suspected_deposit_address
        , i.cex_name
        , i.amount AS amount_consolidated
        , i.block_time AS consolidation_block_time
        , SUM(t.amount) AS amount_deposited
        , MIN(t.block_time) AS deposit_first_block_time
        , MAX(t.block_time) AS deposit_last_block_time
        FROM {{ source('tokens_'~blockchain,'transfers') }} t
        INNER JOIN unique_inflows_expanded i ON t.to=i.suspected_deposit_address
            AND t.token_standard=i.token_standard
            AND t.contract_address=i.token_address
            AND t.block_time BETWEEN i.block_time - interval '1' day AND i.block_time
            --AND t.block_number<i.block_number
            --AND i.amount_raw BETWEEN t.amount_raw*0.9 AND t.amount_raw*1.1
        {% if is_incremental() %}
        WHERE {{ incremental_predicate('t.block_time') }}
        {% endif %}
        GROUP BY 1, 2, 3, 4

        UNION ALL

        SELECT i.suspected_deposit_address
        , i.cex_name
        , i.amount AS amount_consolidated
        , i.block_time AS consolidation_block_time
        , SUM(w.amount/1e9) AS amount_deposited
        , MIN(w.block_time) AS deposit_first_block_time
        , MAX(w.block_time) AS deposit_last_block_time
        FROM {{source('ethereum', 'withdrawals')}} w
        INNER JOIN unique_inflows_expanded i ON w.block_number<i.block_number
            AND w.address=i.suspected_deposit_address
            AND i.token_standard = 'native'
            AND w.block_time BETWEEN i.block_time - interval '1' day AND i.block_time
        {% if is_incremental() %}
        WHERE {{ incremental_predicate('w.block_time') }}
        {% endif %}
        GROUP BY 1, 2, 3, 4
        )
    GROUP BY 1, 2, 3, 4

    {% else %}

    SELECT i.suspected_deposit_address
    , i.cex_name
    , i.amount AS amount_consolidated
    , i.block_time AS consolidation_block_time
    , SUM(t.amount) AS amount_deposited
    , MIN(t.block_time) AS deposit_first_block_time
    , MAX(t.block_time) AS deposit_last_block_time
    FROM {{ source('tokens_'~blockchain,'transfers') }} t
    INNER JOIN unique_inflows_expanded i ON t.to=i.suspected_deposit_address
        AND t.token_standard=i.token_standard
        AND t.contract_address=i.token_address
        AND t.block_time BETWEEN i.block_time - interval '1' day AND i.block_time
        --AND t.block_number<i.block_number
        --AND i.amount_raw BETWEEN t.amount_raw*0.9 AND t.amount_raw*1.1
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('t.block_time') }}
    {% endif %}
    GROUP BY 1, 2, 3, 4

    {% endif %}
    )

SELECT suspected_deposit_address AS address
, '{{blockchain}}' AS blockchain
, cex_name
, amount_consolidated
, consolidation_block_time
, amount_deposited
, deposit_first_block_time
, deposit_last_block_time
FROM in_and_out
WHERE amount_deposited > amount_consolidated

{% endmacro %}
