{% macro cex_deposit_addresses(blockchain, cex_local_flows) %}

WITH unique_inflows_raw AS (
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
    HAVING COUNT(DISTINCT cf.cex_name) = 1
    )

, unique_inflows AS (

    SELECT cf.block_number
    , cf.block_time
    , ui.suspected_deposit_address 
    , cf.token_standard
    , cf.token_address
    , CASE WHEN cf.token_standard = 'native' THEN cf.amount+(f.tx_fee) ELSE cf.amount END AS amount
    , cf.cex_name
    , cf.unique_key
    FROM {{cex_local_flows}} cf
    INNER JOIN unique_inflows_raw ui ON cf.block_number = ui.block_number
        AND cf.unique_key = ui.unique_key
    INNER JOIN {{ source('gas_' + blockchain, 'fees') }} f
        ON cf.block_number = f.block_number
        AND cf.tx_hash = f.tx_hash
        AND f.blockchain = '{{blockchain}}'
    {% if is_incremental() %}
    WHERE cf.flow_type = 'Inflow'
    AND {{ incremental_predicate('cf.block_time') }}
    AND {{ incremental_predicate('f.block_time') }}
    {% endif %}
    )

, unique_inflows_expanded AS (
    SELECT uie.block_number
    , uie.block_time AS consolidation_first_block_time
    , MAX(cf.block_time) AS consolidation_last_block_time
    , uie.suspected_deposit_address 
    , uie.token_standard
    , uie.token_address
    , uie.cex_name
    , uie.unique_key
    , SUM(cf.amount) AS amount
    , COUNT(*) AS consolidation_count
    FROM {{cex_local_flows}} cf
    INNER JOIN unique_inflows uie ON uie.suspected_deposit_address=cf."from"
        AND uie.token_standard=cf.token_standard
        AND uie.token_address=cf.token_address
        AND uie.cex_name=cf.cex_name
        AND uie.block_time BETWEEN cf.block_time AND cf.block_time + interval '1' day
    WHERE cf.flow_type = 'Inflow'
    GROUP BY 1, 2, 4, 5, 6, 7, 8
    )

, in_and_out AS (
    {% if blockchain == 'ethereum' %}

    SELECT suspected_deposit_address
    , cex_name
    , amount_consolidated
    , consolidation_first_block_time
    , consolidation_last_block_time
    , consolidation_count
    , token_standard
    , token_address
    , consolidation_unique_key
    , MIN_BY(deposit_unique_key, deposit_first_block_time) AS deposit_unique_key
    , SUM(amount_deposited) AS amount_deposited
    , MIN(deposit_first_block_time) AS deposit_first_block_time
    , MAX(deposit_last_block_time) AS deposit_last_block_time
    , SUM(deposit_count) AS deposit_count
        FROM (
        SELECT i.suspected_deposit_address
        , i.cex_name
        , i.amount AS amount_consolidated
        , i.consolidation_first_block_time
        , i.consolidation_last_block_time
        , i.consolidation_count
        , i.token_standard
        , i.token_address
        , i.unique_key AS consolidation_unique_key
        , MIN_BY(t.unique_key, (t.block_number, t.tx_index)) AS deposit_unique_key
        , SUM(t.amount) AS amount_deposited
        , MIN(t.block_time) AS deposit_first_block_time
        , MAX(t.block_time) AS deposit_last_block_time
        , COUNT(*) AS deposit_count
        FROM {{ source('tokens_'~blockchain,'transfers') }} t
        INNER JOIN unique_inflows_expanded i ON t.to=i.suspected_deposit_address
            AND t.token_standard=i.token_standard
            AND t.contract_address=i.token_address
            AND t.block_time BETWEEN i.consolidation_first_block_time - interval '1' day AND i.consolidation_last_block_time
        {% if is_incremental() %}
        WHERE {{ incremental_predicate('t.block_time') }}
        {% endif %}
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9

        UNION ALL

        SELECT i.suspected_deposit_address
        , i.cex_name
        , i.amount AS amount_consolidated
        , i.consolidation_first_block_time
        , i.consolidation_last_block_time
        , i.consolidation_count
        , i.token_standard
        , i.token_address
        , i.unique_key AS consolidation_unique_key
        , CAST(NULL AS varchar) AS deposit_unique_key
        , SUM(w.amount/1e9) AS amount_deposited
        , MIN(w.block_time) AS deposit_first_block_time
        , MAX(w.block_time) AS deposit_last_block_time
        , COUNT(*) AS deposit_count
        FROM {{source('ethereum', 'withdrawals')}} w
        INNER JOIN unique_inflows_expanded i ON w.block_number<i.block_number
            AND w.address=i.suspected_deposit_address
            AND i.token_standard = 'native'
            AND w.block_time BETWEEN i.consolidation_first_block_time - interval '1' day AND i.consolidation_last_block_time
        {% if is_incremental() %}
        WHERE {{ incremental_predicate('w.block_time') }}
        {% endif %}
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
        )
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9

    {% else %}

    SELECT i.suspected_deposit_address
    , i.cex_name
    , i.amount AS amount_consolidated
    , i.consolidation_first_block_time
    , i.consolidation_last_block_time
    , i.consolidation_count
    , i.token_standard
    , i.token_address
    , i.unique_key AS consolidation_unique_key
    , MIN_BY(t.unique_key, (t.block_number, t.tx_index)) AS deposit_unique_key
    , SUM(t.amount) AS amount_deposited
    , MIN(t.block_time) AS deposit_first_block_time
    , MAX(t.block_time) AS deposit_last_block_time
    , COUNT(*) AS deposit_count
    FROM {{ source('tokens_'~blockchain,'transfers') }} t
    INNER JOIN unique_inflows_expanded i ON t.to=i.suspected_deposit_address
        AND t.token_standard=i.token_standard
        AND t.contract_address=i.token_address
        AND t.block_time BETWEEN i.consolidation_first_block_time - interval '1' day AND i.consolidation_last_block_time
        --AND t.block_number<i.block_number
        --AND i.amount_raw BETWEEN t.amount_raw*0.9 AND t.amount_raw*1.1
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('t.block_time') }}
    {% endif %}
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9

    {% endif %}
    )

, filter_multioutflow_addresses AS (
    SELECT suspected_deposit_address
    FROM {{ source('tokens_'~blockchain,'transfers') }} tt
    INNER JOIN in_and_out iao ON iao.suspected_deposit_address = tt."from"
        AND tt.block_time BETWEEN iao.deposit_first_block_time AND iao.consolidation_last_block_time + interval '1' hour
    LEFT JOIN {{ ref('cex_evms_addresses') }} ca ON tt.to=ca.address
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('tt.block_time') }}
    {% endif %}
    GROUP BY 1
    HAVING MIN(CASE WHEN iao.cex_name=ca.cex_name THEN true ELSE false END) 
    )

SELECT suspected_deposit_address AS address
, '{{blockchain}}' AS blockchain
, cex_name
, token_standard AS first_deposit_token_standard
, token_address AS first_deposit_token_address
, deposit_first_block_time
, consolidation_first_block_time
, deposit_count
, consolidation_count
, amount_deposited
, consolidation_unique_key
, deposit_unique_key
FROM in_and_out
INNER JOIN filter_multioutflow_addresses USING (suspected_deposit_address)
WHERE deposit_first_block_time <= consolidation_first_block_time
AND deposit_last_block_time <= consolidation_last_block_time
AND amount_consolidated BETWEEN amount_deposited*0.9 AND amount_deposited*1.1 -- account for older dust test transfers
{% endmacro %}
