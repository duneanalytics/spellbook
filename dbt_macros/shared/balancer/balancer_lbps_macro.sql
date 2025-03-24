{% macro 
    balancer_v3_compatible_lbps_macro(
        blockchain, project_decoded_as
    ) 
%}

WITH lbps_list AS (
        SELECT
            e.pool AS pool_address,
            c.symbol AS pool_symbol,
            e.projectToken AS project_token,
            e.reserveToken AS reserve_token
        FROM {{ source(project_decoded_as + '_' + blockchain, 'LBPoolFactory_evt_LBPoolCreated') }} e
        JOIN {{ source(project_decoded_as + '_' + blockchain, 'LBPoolFactory_call_create') }} c ON e.pool = c.output_pool
    ),

    lbps_weight_update AS (
        SELECT
            contract_address AS pool_address,
            evt_block_time,
            startTime,
            endTime,
            element_at(startWeights, 1) / POWER(10,18) AS project_token_start_weight,
            element_at(startWeights, 2) / POWER(10,18) AS reserve_token_start_weight,
            element_at(endWeights, 1) / POWER(10,18) AS project_token_end_weight,
            element_at(endWeights, 2) / POWER(10,18) AS reserve_token_end_weight
        FROM {{ source(project_decoded_as + '_' + blockchain, 'LBPool_evt_GradualWeightUpdateScheduled') }}
    ),

    last_weight_update AS (
        SELECT *
        FROM (
            SELECT 
                pool_address,
                from_unixtime(startTime) AS start_time,
                from_unixtime(endTime) AS end_time,
                project_token_start_weight,
                reserve_token_start_weight,
                project_token_end_weight,
                reserve_token_end_weight,
                ROW_NUMBER() OVER (PARTITION BY pool_address ORDER BY evt_block_time DESC) AS ranking
            FROM lbps_weight_update c
        ) w
        WHERE ranking = 1
    )
    
        SELECT 
            '{{blockchain}}' AS blockchain,
            l.pool_symbol,
            l.pool_address,
            w.start_time,
            w.end_time,
            l.project_token,
            t1.symbol AS project_token_symbol,
            l.reserve_token,
            t2.symbol AS reserve_token_symbol,
            project_token_start_weight,
            reserve_token_start_weight,
            project_token_end_weight,
            reserve_token_end_weight
        FROM lbps_list l
        JOIN last_weight_update w
        ON w.pool_address = l.pool_address
        LEFT JOIN {{ source('tokens', 'erc20') }} t1 ON l.project_token = t1.contract_address AND t1.blockchain = '{{blockchain}}'
        LEFT JOIN {{ source('tokens', 'erc20') }} t2 ON l.reserve_token = t2.contract_address AND t2.blockchain = '{{blockchain}}'

{% endmacro %}